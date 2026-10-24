/**
 * 配对流程（电脑端）—— 激活码换隧道，配对码连手机。
 *
 * ## 一次配对的完整时序
 *
 * ```
 *   客户电脑                      example.com 服务器              手机
 *      │                                  │                          │
 *      │ ① 填激活码 POST /pair/register    │                          │
 *      ├─────────────────────────────────>│                          │
 *      │   服务器建隧道 + 建 DNS + 发令牌   │                          │
 *      │<─────────────────────────────────┤                          │
 *      │ {hostname, tunnelToken, accessToken}                         │
 *      │                                  │                          │
 *      │ ② 起 cloudflared（用 tunnelToken） │                          │
 *      │ ③ POST /pair/handshake ──────────>│                          │
 *      │<── {pairCode: "A7K2M9", 600s} ────┤                          │
 *      │ ④ 屏幕上显示二维码 + 配对码        │                          │
 *      │                                  │  ⑤ 输码 POST /pair/claim │
 *      │                                  │<─────────────────────────┤
 *      │                                  │──> {hostname, accessToken}
 *      │                                  │                          │
 *      │<════════ 隧道建立，两端同令牌，开始搬事件 ═══════════════════>│
 * ```
 *
 * ## 为什么要有「配对码」这一跳
 *
 * 服务器下发给电脑的是 `{hostname, tunnelToken, accessToken}`，其中
 * `accessToken` 是 64 位十六进制 —— 塞进二维码会是一大坨，手输更不可能。
 * 而配对码只有 6 位，**二维码和手输两条路都成立**。
 *
 * 关键在于：**凭据本身不经过二维码**。二维码里只有那个 6 位码，
 * 真正的令牌由手机直接向服务器换取。所以哪怕二维码被人拍到，
 * 泄露的也只是一次性的、10 分钟就过期的短码。
 *
 * ## 为什么二维码里只有 6 位码，没有别的
 *
 * 试过编码成 `dsh-pair:A7K2M9` 这种带 scheme 的串，好处是将来能扩展，
 * 坏处是**用户拿系统相机扫出来的就是一串看不懂的东西**，还得自己从中
 * 抠出那 6 位。而现在扫出来就是 `A7K2M9`，照着输即可 —— 面向大众分发时
 * 「扫不出也能照抄」比「协议优雅」重要得多。
 *
 * @module dsh-remote-desktop/pair
 */

import { chmodSync, closeSync, createWriteStream, existsSync, mkdirSync, openSync, readFileSync, readSync, renameSync, statSync, unlinkSync, writeFileSync } from 'node:fs';
import { createHash } from 'node:crypto';
import { hostname as osHostname } from 'node:os';
import { join } from 'node:path';
import { Readable } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import { qrDataUri } from './qr.js';

/**
 * 配对服务的公网地址（Cloudflare 隧道，走 HTTPS）。
 *
 * ⚠️ **优先读环境变量 `DSH_REMOTE_PAIR_ENDPOINT`**，没设才用这个占位默认值。
 *
 * 为什么要有这个开关：配对服务端不在本仓库里（那是部署方自己的基础设施），
 * 所以开源版只能放一个 `example.com` 占位地址。自建的人要么改这一行、
 * 要么设环境变量 —— 后者不用改源码，升级版本时也不会被覆盖掉。
 *
 * 与手机端 `dsh-jiuling-remote-mobile/lib/index.js` 的 `PAIR_ENDPOINT`
 * **必须指向同一个服务**，否则两端各自去不同的地方 claim 配对码。
 */
export const PAIR_ENDPOINT =
	typeof process.env.DSH_REMOTE_PAIR_ENDPOINT === 'string' && process.env.DSH_REMOTE_PAIR_ENDPOINT !== ''
		? process.env.DSH_REMOTE_PAIR_ENDPOINT.replace(/\/+$/, '')
		: 'https://pair.example.com';

/**
 * 平台 → cloudflared 文件名。
 *
 * 与服务器上 `/pair/cloudflared/` 的白名单**必须一致**，改一边就得改另一边。
 * 只列我们真的分发过的四个：Windows x64、Linux x64、macOS x64 / arm64。
 */
const PLATFORM_FILES = {
	'win32-x64': 'cloudflared-windows-amd64.exe',
	'linux-x64': 'cloudflared-linux-amd64',
	'darwin-x64': 'cloudflared-darwin-amd64',
	'darwin-arm64': 'cloudflared-darwin-arm64',
};

/**
 * 分发的 cloudflared **必须**匹配这些 sha256（外部审计 3.3 / 修复清单 P1-6）。
 *
 * ## 为什么必须有这一条
 *
 * 原来只校验 `size >= 1MB`，**没有哈希、没有签名**，就 `chmod 755` 并运行。
 * 而那个文件来自**我们自己的服务器**（`/pair/cloudflared/<name>`，无鉴权、
 * 无完整性保护）。于是链条是：
 *
 * ```
 * 谁控制了服务器 / CF_API_TOKEN（能改 zone 内 DNS）
 *   ⇒ 换掉那个 55 MB 的可执行文件
 *   ⇒ 在每一台客户机上执行任意代码（供应链 RCE）
 * ```
 *
 * 审计的原话是「服务器一次失陷 = 全客户失陷，两个方向都成立」—— 这一条就是
 * 那个「反方向」。校验和把「改服务器上的文件」这一步从「静默 RCE」变成
 * 「客户机拒绝运行并报错」。
 *
 * ## ⚠️ 这是**逐字节**的完整性校验，不是身份认证
 *
 * 哈希值硬编码在**包内**（跟着 npm 分发），所以攻击者若同时能改服务器上的
 * 文件和本包的源码，这条就失效。它的定位是**提高门槛 + 让篡改可见**，
 * 真正要解决根因得靠「用 CF 官方源分发」或「签名 + 独立公钥」
 * （审计 P1-6 的另外两个选项）。当前这一步是**成本最低、收益最大**的那个。
 *
 * ## ⚠️ 哈希对不上时**必须拒绝运行**，不能只告警
 *
 * 告警是「已经执行了可疑二进制，顺便说一句」—— 那不是防护。
 *
 * ## 更新方式
 *
 * 服务器上换了分发包 ⇒ 这里四个值必须同步换，否则所有客户机都下不动。
 * 取值命令：`sha256sum /workspace/dsh-pair/cloudflared/*`
 */
const CLOUDFLARED_SHA256 = {
	'cloudflared-windows-amd64.exe': '4d2490e74082c5d48dbfedaf92576fa5bd1702379ab88eb7faa0cfbe8dc891b6',
	'cloudflared-linux-amd64': '3bfc539f45d2620c71debb7a86be0efa737ed5ccbad9eb85c787116900e5c586',
	'cloudflared-darwin-amd64': '3b0dbaf378337e9d7fb6cd235a76d1d6a7dee434c2d8edf9281164fb6a3837ab',
	'cloudflared-darwin-arm64': '647615df99d49cd179ea5e13ef760b9bbfb33630f6ae62f87e33dfadda60adcc',
};

/**
 * 算一个文件的 sha256（流式，不把 55 MB 读进内存）。
 *
 * @param path - 文件路径。
 * @returns 十六进制摘要。
 */
function sha256OfFile(path) {
	const hash = createHash('sha256');
	const handle = openSync(path, 'r');
	try {
		const buffer = Buffer.allocUnsafe(1024 * 1024);
		for (;;) {
			const read = readSync(handle, buffer, 0, buffer.length, null);
			if (read <= 0) break;
			hash.update(buffer.subarray(0, read));
		}
	} finally {
		closeSync(handle);
	}
	return hash.digest('hex');
}

/**
 * 带超时的配对服务请求。
 *
 * ⚠️ 不用 `AbortSignal.timeout()`：那个 API 要 Node 17.3+，而这里宁可自己拿
 * `AbortController` + `setTimeout` 拼一次，省得为一个便利函数抬高版本要求。
 *
 * @param path - 相对 `/pair/` 的路径，如 `/pair/register`。
 * @param body - 请求体；`undefined` 表示发 GET。
 * @param timeoutMs - 超时毫秒数。
 * @returns `{ ok, status, payload }`。
 */
async function pairRequest(path, body, timeoutMs = 30000) {
	const controller = new AbortController();
	const timer = setTimeout(() => controller.abort(), timeoutMs);
	try {
		const response = await fetch(PAIR_ENDPOINT + path, {
			method: body === undefined ? 'GET' : 'POST',
			headers: body === undefined ? {} : { 'content-type': 'application/json' },
			body: body === undefined ? undefined : JSON.stringify(body),
			signal: controller.signal,
		});
		const text = await response.text();
		let payload;
		try {
			payload = JSON.parse(text);
		} catch {
			payload = { error: 'bad-json', message: text.slice(0, 200) };
		}
		return { ok: response.ok, status: response.status, payload };
	} catch (error) {
		const aborted = error?.name === 'AbortError';
		return {
			ok: false,
			status: 0,
			payload: {
				error: aborted ? 'timeout' : 'network',
				message: aborted
					? `连接配对服务器超时（${Math.round(timeoutMs / 1000)} 秒）`
					: `连不上配对服务器：${String(error?.message ?? error)}`,
			},
		};
	} finally {
		clearTimeout(timer);
	}
}

/** 规范化激活码：去空白、去横杠、转大写（用户从微信里复制粘贴常带这些）。 */
function normalizeCode(raw) {
	return String(raw ?? '')
		.trim()
		.toUpperCase()
		.replace(/[\s-]/g, '');
}

/**
 * 激活码的短指纹，**只用来打日志**。
 *
 * ⚠️ 和 `index.js` 里的 `tokenFingerprint` 同构：日志里不能出现激活码本身
 * （外部审计 4.4），但排查时又需要能对账「用户在界面上填的是不是这一串」。
 * 用 `sha256` 而不是取前几位 —— 激活码是随机十六进制，前几位能直接当爆破起点。
 *
 * @param code - 已规范化的激活码。
 * @returns 8 位十六进制指纹。
 */
function codeFingerprint(code) {
	return createHash('sha256').update(String(code ?? ''), 'utf8').digest('hex').slice(0, 8);
}

/**
 * 造一个配对流程实例。
 *
 * @param options - `{ stateDir, logger }`。
 * @returns 配对状态与动作的集合。
 */
export function createPairing({ stateDir, logger }) {
	const STATE_PATH = join(stateDir, 'pair.json');
	const BIN_DIR = join(stateDir, 'bin');

	/** cloudflared 自动下载的进度（给 UI 显示；没在下载时 `active` 为 false）。 */
	const download = { active: false, received: 0, total: 0, error: '' };

	/** 读配对状态；没配对过或文件坏了都返回 `undefined`。 */
	function read() {
		try {
			const parsed = JSON.parse(readFileSync(STATE_PATH, 'utf8'));
			if (typeof parsed?.hostname !== 'string' || typeof parsed?.tunnelToken !== 'string') {
				return undefined;
			}
			return parsed;
		} catch {
			return undefined;
		}
	}

	/**
	 * 落盘配对状态。
	 *
	 * ⚠️ 走「临时文件 + rename」而不是直接写：这个文件里是**唯一一份**隧道凭据，
	 * 写一半断电就永久丢了（得重新生成隧道才能恢复）。rename 在同一文件系统上是
	 * 原子的，要么旧内容要么新内容，不会出现半截 JSON。
	 *
	 * @param record - 要保存的记录。
	 */
	function save(record) {
		mkdirSync(stateDir, { recursive: true });
		const tmp = `${STATE_PATH}.${process.pid}.tmp`;
		writeFileSync(tmp, JSON.stringify(record, null, 2), { mode: 0o600 });
		renameSync(tmp, STATE_PATH);
	}

	/** 清掉配对状态（换激活码 / 解绑时用）。 */
	function clear() {
		try {
			unlinkSync(STATE_PATH);
		} catch {
			/* 本来就没有 */
		}
	}

	/**
	 * 解绑：**先通知服务端清记录**，再清本机。
	 *
	 * ## 为什么必须通知服务端（外部审计 4.3）
	 *
	 * 原来 `clear()` 只删本机 `pair.json`，服务端**毫不知情**。而服务端对已有
	 * `clientSecret` 的记录是 fail-closed 的（`legacy_claim` 已删），于是：
	 *
	 * ```
	 * 客户点「解绑」→ 本机凭据没了 → 想再配对 → 服务端说「设备不符」
	 *   → 客户机从此**再也注册不上**（唯一出路是管理员 unregister）
	 * ```
	 *
	 * UI 上那句「如需重连请重新配对」实际做不到 —— 这就是审计说的
	 * 「`allow_rebind` 是死代码」。现在解绑会调 `/pair/release`，
	 * 服务端删掉记录（连隧道一起），下次配对走全新流程。
	 *
	 * ## ⚠️ 顺序：**先通知、再清本机**
	 *
	 * 反过来的话（先清本机），`clientSecret` 已经没了，服务端凭据校验必然失败
	 * ⇒ 记录永远留在服务端 ⇒ 又回到上面那个死局。所以必须先把凭据读出来、
	 * 打完接口，才清本机。
	 *
	 * ## ⚠️ 服务端失败**不阻塞**本机解绑
	 *
	 * 用户点「解绑」的意图是「这台机器不再对外」—— 那个语义**本机就能实现**
	 * （清凭据 + 换令牌 + 停隧道）。网络不通时如果卡住不办，用户看到的是
	 * 「解绑失败」但机器其实还开着，反而更危险。
	 *
	 * 所以：服务端失败 ⇒ 本机照办，并在返回值里如实说明，让 UI 提示
	 * 「本机已解绑，但服务端记录未清除，如需重新配对请联系客服」。
	 *
	 * @returns `{ ok, message, released }` —— `released` 表示服务端记录已清。
	 */
	async function release() {
		const record = read();
		const code = typeof record?.activationCode === 'string' ? record.activationCode : '';
		const secret = typeof record?.clientSecret === 'string' ? record.clientSecret : '';
		let released = false;
		let serverMessage = '';
		if (code !== '' && secret !== '') {
			const { ok, payload } = await pairRequest('/pair/release', { code, clientSecret: secret });
			released = ok === true;
			if (!ok) {
				serverMessage = String(payload?.message ?? payload?.error ?? '服务端未确认');
				logger?.warn?.(`[dsh-remote][pair] 解绑时服务端未清除记录（${serverMessage}）`);
			}
		}
		// 服务端那步无论成败都清本机：解绑的语义是「本机不再对外」。
		clear();
		return { ok: true, released, message: serverMessage };
	}

	/**
	 * 用激活码向服务器注册，拿到这台电脑专属的隧道与令牌。
	 *
	 * 服务器对同一个激活码是**幂等**的：重复注册会原样返回上次的隧道，
	 * 不再消耗 Cloudflare 配额。所以重装插件是安全的。
	 *
	 * ⚠️ **复用时不返回 `tunnelToken`**（服务器侧改动，见 `pair_server.register`）。
	 * 那个 token 只在首次建隧道时需要，用来起 `cloudflared`；之后本机
	 * `pair.json` 里已经有了。每次都下发等于把它也变成一个「凭激活码即可领取」
	 * 的长期凭据。所以复用时从**本机已有记录**里读旧的 tunnelToken。
	 *
	 * ⚠️ **凭据判据只有「设备凭据」一条**（服务端 `register`）：
	 * 本机带着 `clientSecret` 来 ⇒ 认识这台机器，**令牌不轮换**（轮换会把你已经
	 * 配对好的手机踢下线）。
	 *
	 * ⚠️ **老记录（没有 `clientSecret`）现在一律被拒**，返回
	 * `legacy-needs-migration` —— 由管理员在服务端 `pair_admin.py migrate <码>`
	 * 显式迁移。这里曾经有一条「用激活码 + 主机名认领一次」的自动升级路径，
	 * 已**彻底删除**：那条判据的两个输入（激活码、主机名）都是攻击者可控的，
	 * 而老记录的 `deviceFingerprint` 全是空的，实际退化成了「激活码即通行」，
	 * 且**不轮换令牌** ⇒ 受害者毫无感知。迁移会轮换 `accessToken`，所以客户机
	 * 需要重新配对一次。
	 *
	 * @param activationCode - 用户填的激活码。
	 * @returns `{ ok, record?, message }`。
	 */
	async function register(activationCode) {
		const code = normalizeCode(activationCode);
		if (code === '') return { ok: false, message: '请填写激活码' };

		// ⚠️ 带上本机已存的设备凭据（若有）。服务端拿它判「就是这台机器」——
		// **只看激活码是不够的**（外部审计 S1/S2：激活码泄露即等于电脑控制权）。
		// 首次注册时它是空，服务端会下发一把新的。
		const previous = read();
		// ⚠️ **不打激活码本身**（外部审计 4.4）。`index.js` 启动处那条注释说得对：
		// 「日志里的激活码 = 静默接管」—— 凡能读引擎日志 / GUI 日志面板 / `/export`
		// 导出包 / 诊断包的人都能拿到它。只打一个单向短指纹，够对账、不够还原。
		logger?.info?.(`[dsh-remote][pair] 正在注册（激活码指纹 ${codeFingerprint(code)}）…`);
		const { ok, payload } = await pairRequest('/pair/register', {
			code,
			device: osHostname(),
			clientSecret: previous?.clientSecret,
		});
		if (!ok || typeof payload?.hostname !== 'string') {
			logger?.warn?.(`[dsh-remote][pair] 注册失败: ${JSON.stringify(payload)}`);
			return { ok: false, message: payload?.message ?? `注册失败（HTTP ${payload?.error ?? '?'}）` };
		}

		// ⚠️ tunnelToken 优先用本次响应；复用时响应里没有，退回本机旧记录。
		// 两者都没有才真算失败 —— 没有它 cloudflared 起不来，隧道就是死的。
		const tunnelToken =
			typeof payload.tunnelToken === 'string' && payload.tunnelToken !== ''
				? payload.tunnelToken
				: previous?.tunnelToken;
		if (typeof tunnelToken !== 'string' || tunnelToken === '') {
			return {
				ok: false,
				message: '服务器没下发隧道凭据，本机也没有可复用的记录 —— 请让管理员解绑后重试',
			};
		}

		const record = {
			activationCode: code,
			subdomain: payload.subdomain,
			hostname: payload.hostname,
			tunnelToken,
			accessToken: payload.accessToken,
			// 设备凭据：之后所有涉及凭据的调用（handshake / rotate / 再注册）都要它。
			clientSecret: payload.clientSecret,
			pairedAt: Date.now(),
			reused: payload.reused === true,
		};
		save(record);
		logger?.info?.(`[dsh-remote][pair] 注册成功: ${record.hostname}${record.reused ? '（复用已有隧道）' : ''}`);
		return { ok: true, record, message: `已绑定 ${record.hostname}` };
	}

	/**
	 * 生成一个 6 位配对码（给手机用），并渲染成二维码。
	 *
	 * 每次调用都会换一个新码（旧码作废），所以「重新生成」按钮天然可用。
	 *
	 * @returns `{ ok, pairCode?, expiresIn?, svg?, message }`。
	 */
	async function handshake() {
		const record = read();
		if (record === undefined) return { ok: false, message: '还没有配对，请先填激活码' };

		// ⚠️ 必须带设备凭据：服务端靠它挡「拿泄露的激活码生成配对码 → claim 取得令牌」
		// 这条链（外部审计 S1）。
		const { ok, payload } = await pairRequest('/pair/handshake', {
			code: record.activationCode,
			clientSecret: record.clientSecret,
		});
		if (!ok || typeof payload?.pairCode !== 'string') {
			return { ok: false, message: payload?.message ?? '生成配对码失败，请重试' };
		}
		return {
			ok: true,
			pairCode: payload.pairCode,
			expiresIn: payload.expiresIn,
			expiresAt: Date.now() + Number(payload.expiresIn ?? 600) * 1000,
			// 二维码里**只有那 6 位码**，见文件头「为什么二维码里只有 6 位码」。
			// ⚠️ 交出去的是 **data: URI**（给 `<img src>` 用）而不是 SVG 源码：
			// 前端原来拿它喂 `dangerouslySetInnerHTML`，是全仓唯一的原始 HTML sink
			// （外部审计 M6）。变成图片后 SVG 不进 DOM 解析，那条通道从结构上关掉。
			qr: qrDataUri(payload.pairCode, { margin: 2, scale: 1 }),
			message: '把这个码输到手机上即可',
		};
	}

	/**
	 * 找到 cloudflared；找不到就**自动从配对服务器下载**。
	 *
	 * ## 为什么不直接去 GitHub 下
	 *
	 * 实测：**国内服务器直连 `github.com` 是不通的**（`curl` 返回 000），
	 * 客户电脑大概率同样。让每个客户各自去 GitHub 撞一次，结果就是
	 * 大量客户卡在「隧道起不来」，而用户完全看不出这是网络问题。
	 *
	 * 所以由配对服务器中转一次（它已经能出网，且文件已经放在上面）。
	 *
	 * @returns `{ ok, path?, message, cached? }`。
	 */
	async function ensureCloudflared() {
		const cached = findCloudflared();
		if (cached !== undefined) return { ok: true, path: cached, cached: true, message: '已找到 cloudflared' };

		const fileName = PLATFORM_FILES[`${process.platform}-${process.arch}`];
		if (fileName === undefined) {
			return {
				ok: false,
				message: `暂不支持这个平台（${process.platform}-${process.arch}），请手工安装 cloudflared 后重试`,
			};
		}
		// ⚠️ 没有已知哈希就**不下发、不运行**（fail-closed）。新平台先补哈希。
		const expectedSha = CLOUDFLARED_SHA256[fileName];
		if (typeof expectedSha !== 'string') {
			return {
				ok: false,
				message: `没有 ${fileName} 的校验值，出于安全考虑拒绝下载。请手工安装 cloudflared 后重试`,
			};
		}

		const target = join(BIN_DIR, process.platform === 'win32' ? 'cloudflared.exe' : 'cloudflared');
		const tmp = `${target}.part`;
		mkdirSync(BIN_DIR, { recursive: true });
		logger?.info?.(`[dsh-remote][pair] 正在下载 cloudflared（${fileName}）…`);

		download.active = true;
		download.received = 0;
		download.total = 0;
		download.error = '';

		const controller = new AbortController();
		const timer = setTimeout(() => controller.abort(), 300000);
		try {
			const response = await fetch(`${PAIR_ENDPOINT}/pair/cloudflared/${fileName}`, {
				signal: controller.signal,
			});
			if (!response.ok || response.body === null) {
				throw new Error(`HTTP ${response.status}`);
			}
			download.total = Number(response.headers.get('content-length') ?? 0);

			// 逐块写盘并记账：55 MB 全读进内存没必要，且 UI 要显示进度。
			const source = Readable.fromWeb(response.body);
			source.on('data', (chunk) => {
				download.received += chunk.length;
			});
			await pipeline(source, createWriteStream(tmp));
			clearTimeout(timer);

			const size = statSync(tmp).size;
			if (size < 1024 * 1024) throw new Error(`文件太小（${size} 字节），可能不是可执行文件`);
			// ⚠️ 校验必须在 `renameSync` **之前**：先校验再落位，绝不留下未校验的
			// 可执行文件（哪怕是短暂的窗口 —— 别的进程可能正好扫到这个路径）。
			const actualSha = sha256OfFile(tmp);
			if (actualSha !== expectedSha) {
				throw new Error(
					`校验值不符（期望 ${expectedSha.slice(0, 16)}…，实际 ${actualSha.slice(0, 16)}…），已拒绝使用该文件`,
				);
			}
			renameSync(tmp, target);
			if (process.platform !== 'win32') chmodSync(target, 0o755);

			logger?.info?.(`[dsh-remote][pair] cloudflared 已就绪: ${target}（${size} 字节，校验通过）`);
			return { ok: true, path: target, cached: false, message: 'cloudflared 已下载完成' };
		} catch (error) {
			const aborted = error?.name === 'AbortError';
			download.error = aborted ? '下载超时' : String(error?.message ?? error);
			logger?.warn?.(`[dsh-remote][pair] 下载 cloudflared 失败: ${download.error}`);
			try {
				unlinkSync(tmp);
			} catch {
				/* 没落盘 */
			}
			return { ok: false, message: `下载 cloudflared 失败：${download.error}` };
		} finally {
			clearTimeout(timer);
			download.active = false;
		}
	}

	/** 找本机已有的 cloudflared（下载过的排在最前）。 */
	function findCloudflared() {
		const candidates = [
			process.env.DSH_REMOTE_CLOUDFLARED,
			join(BIN_DIR, process.platform === 'win32' ? 'cloudflared.exe' : 'cloudflared'),
			process.env.LOCALAPPDATA ? join(process.env.LOCALAPPDATA, 'cloudflared', 'cloudflared.exe') : undefined,
			'/usr/local/bin/cloudflared',
			'/usr/bin/cloudflared',
			'/opt/homebrew/bin/cloudflared',
		].filter(Boolean);
		for (const candidate of candidates) if (existsSync(candidate)) return candidate;
		return undefined;
	}

	/**
	 * 给 UI 的一份快照。
	 *
	 * ⚠️⚠️ **不回 `activationCode`**（外部审计 M2）：这个端点是未认证可达的
	 * （见 `handlePairRoute` 的 fence 注释），回显激活码等于把「开通 + 接管」
	 * 两把钥匙挂在一个 HTTP 端点后面。前端本来也不用它 —— 要改激活码就直接
	 * 重新填一次。
	 *
	 * ⚠️ 同样**不回 `home`**：那是本机用户名与目录结构，属于不需要外泄的信息
	 * （前端用的 `platform` / `cloudflared` / `hostname` / `paired` / `download`
	 * 都保留）。
	 */
	function status() {
		const record = read();
		return {
			paired: record !== undefined,
			hostname: record?.hostname ?? '',
			pairedAt: record?.pairedAt ?? 0,
			cloudflared: findCloudflared() ?? '',
			download: { ...download },
			platform: `${process.platform}-${process.arch}`,
		};
	}

	return { read, save, clear, release, register, handshake, ensureCloudflared, findCloudflared, status, STATE_PATH };
}
