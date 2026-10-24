/**
 * dsh-remote-mobile —— 手机端（host half，跑在手机的 DSH 引擎里）。
 *
 * ## 职责
 *
 * 手机是这个方案的「瘦客户端 + 搬运工」：
 * - 手机本地跑**完整官方前端**（零远程 UI 加载，这是本方案相对「映射桌面」的核心优势）；
 * - 手机不跑 LLM。用户在「电脑文件夹」里发的消息，转成 RPC 打到电脑；
 * - 电脑回的 durable 事件写进**手机本地**同名 sessionId 的会话日志。
 *
 * 于是会话列表、历史、工具卡片、流式渲染全部是官方现成组件在工作，
 * 手机端不需要写任何聊天 UI（只写那个「电脑文件夹」入口对话框）。
 *
 * ## ⚠️ cwd 绝不跨端传（实测硬约束）
 *
 * Android 是 posix 语义，`path.posix.isAbsolute('C:/Users/...') === false`，
 * 而 `validateSessionHeader` 要求 cwd 是绝对路径；更危险的是
 * `SessionCommandController.createOrAdopt()` 里有 `await mkdir(cwd, {recursive:true})`。
 * 所以**手机端会话用自己的本地 cwd**，电脑路径只作展示文字，两端靠 sessionId 映射。
 *
 * ## 为什么手机端不装 LLM 也能「发消息」
 *
 * 关键：手机端会话**不要有 live agent**。一旦 `ctx.agents.get(sessionId)` 有值，
 * 官方 `sessionController.prompt` 就会把消息交给那个 agent（真跑手机本地模型）。
 * 本插件**拦截 `sessionController.prompt`**：远程会话的消息转发电脑，
 * 同时用 `Session.append()` 在手机本地落一条 user/message，前端立刻显示用户气泡。
 *
 * 注意 `follow` / `page` 这类历史 RPC 的第三个参数是 `false`（不激活 Agent），
 * 所以手机端打开一个远程会话**不会**触发本地模型，这是本方案能成立的前提。
 *
 * ## 事件搬运（durable 通道）
 *
 * 电脑的 durable 事件按顺序写进手机本地会话：
 * - 直接 `session.append(type, data, opts)`，与电脑端同构，前端零改动；
 * - **跳过 `session/end-seed`** —— 手机端 `Session` 构造时已自动插过一条，
 *   重放时再写会让 seq 错位一位（实测踩过这个坑）。
 *
 * ## 流式通道（best-effort，全局一条）
 *
 * 电脑的流式帧经**一条全局 SSE** 转给浏览器，浏览器侧注入 `assistant/live-chunk`
 * transient（不落盘）。用全局而不是按会话开多条，是因为手机 WebView 对
 * 并发连接有上限，且重开 App 后一条连接就能恢复所有会话的动画。
 *
 * 丢帧是安全的：电脑端帧来自进程内事件永不丢，重连后 durable settlement
 * 会把完整内容补上（实测结论），最坏只是少了逐字动画。
 *
 * @module dsh-remote-mobile
 */

import { createHash, randomUUID } from 'node:crypto';
import { appendFileSync, existsSync, mkdirSync, readFileSync, readdirSync, renameSync, rmSync, writeFileSync } from 'node:fs';
import { request as httpRequest } from 'node:http';
import { request as httpsRequest } from 'node:https';
import { homedir } from 'node:os';
import { join } from 'node:path';

export const name = 'dsh-jiuling-remote-mobile';

/**
 * `sessions` 建会话/追加事件，`webServer` 给浏览器挂桥，`sessionController` 拦截 prompt。
 *
 * ⚠️ **`userQuestions` 是可选的，必须带 `required: false`。**
 *
 * 电脑端发起的 ask 要显示在手机上，最省事、观感最一致的办法不是自己画卡片，
 * 而是**借手机本地这套官方卡片**：调 `ctx.userQuestions.ask()`，官方
 * `dsh-client-ui-user-questions` 就会照常渲染出带 A/B/C 选项、带「跳过/提交」、
 * 带多题翻页的那张卡片 —— 和手机自己发起提问时**一模一样**。
 *
 * 但它是引擎里的可选服务：万一某个版本没有（或被用户在插件页关掉），
 * 插件不能因此整个装不上 —— 那样连远程功能都没了。所以声明成非必需，
 * 用的时候判空降级（见 `showAskViaOfficial`）。
 */
export const inject = {
	sessions: { required: true },
	sessionController: { required: true },
	webServer: { required: true },
	workspaceRegistry: { required: true },
	userQuestions: { required: false },
};

/** 浏览器访问桥的路由。 */
const ROUTE_RPC = '/api/dsh-remote/rpc';
const ROUTE_STREAM = '/api/dsh-remote/stream';

/** 状态目录（手机端）。 */
const STATE_DIR = join(process.env.DSH_HOME ?? join(homedir(), '.dsh'), 'dsh-remote');

/**
 * 落盘诊断（真机唯一可见的日志通道）。
 *
 * ## ⚠️⚠️ 这个函数必须是**模块级**的（2026-10-22 修）
 *
 * `mirrorEvent()`（模块级函数）里早就调用了 `debugLog`，但**从来没有定义过**
 * 它 —— 之前只把它写成 `apply()` 里的局部闭包，模块级函数看不见。
 *
 * 后果是致命的：`mirrorEvent` 在**首次校准 seq** 那一句就抛
 * `ReferenceError: debugLog is not defined`，被上层 `catch` 吞掉后只打一条
 * 真机不可见的 `ctx.logger.warn`。表现是：
 *
 *   · 手机本地会话日志**永远停在建会话时的 454 字节**，一条事件都没写进去；
 *   · 手机上永远看不到电脑的回复（用户 2026-10-22 报的「没有任何回复」）；
 *   · 因为每次都抛在写盘之前，所以连 `seq 校准` 都没成功过一次。
 *
 * 所以它必须定义在所有调用者之前、且不依赖任何 `ctx`。
 *
 * @param line - 要追加的一行（自动加 ISO 时间戳）。
 */
function debugLog(line) {
	try {
		appendFileSync(join(STATE_DIR, 'mobile-debug.log'), `${new Date().toISOString()} ${line}\n`, 'utf8');
	} catch {
		// 诊断失败绝不影响业务
	}
}

/** 连接配置：电脑隧道地址 + 访问令牌。 */
const CONFIG_PATH = join(STATE_DIR, 'mobile.json');

/** 远程会话清单（持久化，重开 App 后能接着搬）。 */
const SESSIONS_PATH = join(STATE_DIR, 'sessions.json');

/**
 * 电脑配置的本地快照 —— 模型目录、预设名单、根目录列表。
 *
 * ## 为什么要落盘（用户明确要求，2026-10-17）
 *
 * 用户原话：「我不想要你每次都往电脑上面拉取数据……用户匹配成功之后，他就加载
 * 一次文件目录，还有预设以及模型列表就可以了，后续你就不用再拉取了。……电脑的
 * 配置很少会有改变的，有改变的话，用户他自己在那个工作区那里点一下刷新来重新
 * 拉取就行了。平时呢你就把上一次拉到的结果，直接给我存死在本地，后续就从本地
 * 读取就行了，这样速度快体验优。」
 *
 * 对应两个真机症状：
 *
 * 1. **切模型转圈**：`modelCatalog` / `presetList` 这两个 RPC 原来是**每次都联网**
 *    （见 `case 'modelCatalog'`），前端每展开一次模型菜单或切一个模型，就要等一趟
 *    公网往返。本地已有数据时这完全是白等。
 * 2. **电脑下线后预设还是旧的**：`desktopCache` 只在**成功**时写入、从不因失败清空，
 *    所以电脑隧道断了以后，内存里那份旧预设仍被 `mergePresets` 并进去，
 *    用户切回手机工作区也看到电脑的预设（`pojia`），选中就报
 *    `Unknown agent preset: pojia`。
 *
 * 落盘之后，读路径**完全不碰网络**：有本地快照就直接给，没有才拉一次。
 * 重新拉取只由两条路触发 —— 配对成功、以及用户自己点「刷新」。
 */
const DESKTOP_CACHE_PATH = join(STATE_DIR, 'desktop-cache.json');

/** 落盘时最多保留多少层子目录缓存（见 `saveDesktopCache` 的注释）。 */
const DESKTOP_DIRS_PERSIST_MAX = 60;

/**
 * 取出子目录缓存里**最近**的那些层，供落盘使用。
 *
 * ⚠️ 依赖 `Object.keys` 的**插入顺序**：`desktopCache.dirs` 是按用户浏览顺序
 * 写进去的，所以尾部就是最新的（与 `rememberDir` 的淘汰顺序同源）。
 *
 * @param dirs - 无原型缓存对象。
 * @returns 只含最近若干层的普通对象。
 */
function tailDirs(dirs) {
	if (dirs === null || typeof dirs !== 'object') return {};
	const keys = Object.keys(dirs);
	const start = keys.length > DESKTOP_DIRS_PERSIST_MAX ? keys.length - DESKTOP_DIRS_PERSIST_MAX : 0;
	const kept = {};
	for (let at = start; at < keys.length; at += 1) {
		const key = keys[at];
		if (!isSafeCacheKey(key)) continue;
		kept[key] = dirs[key];
	}
	return kept;
}

/**
 * 把落盘的 `dirs` 还原成可用的子目录缓存。
 *
 * ⚠️ **不能直接拿来用**：磁盘上那份是 `JSON.parse` 出来的**普通对象**，
 * `dirs['__proto__']` 会返回 `Object.prototype` 而不是 `undefined`
 * （`listDesktop` 的缓存命中判据是 `cached != null`，那样会命中一个不是列表的东西）。
 * 所以逐键过 `isSafeCacheKey`，并重建到 `Object.create(null)` 上。
 *
 * 任何形状不对的条目直接丢弃 —— 这份文件是加速用的，坏了最多退回联网拉一次。
 *
 * @param raw - 磁盘上读出的 `dirs`（可能是任意值）。
 * @returns 无原型对象。
 */
function restoreDirs(raw) {
	const dirs = emptyDirCache();
	if (raw === null || typeof raw !== 'object' || Array.isArray(raw)) return dirs;
	const keys = Object.keys(raw);
	for (let at = 0; at < keys.length; at += 1) {
		const key = keys[at];
		if (!isSafeCacheKey(key)) continue;
		const value = raw[key];
		if (value === null || typeof value !== 'object') continue;
		dirs[key] = value;
	}
	return dirs;
}

/**
 * 读本地快照。
 *
 * ⚠️ 任何异常都只返回 `null`，绝不让它挡住插件启动 —— 这个文件是**加速用的**，
 * 坏了最多退回「联网拉一次」，不该让整个远程功能起不来。
 *
 * @returns `{catalog, presets, directory, at}` 或 `null`。
 */
function loadDesktopCache() {
	try {
		if (!existsSync(DESKTOP_CACHE_PATH)) return null;
		const raw = readFileSync(DESKTOP_CACHE_PATH, 'utf8');
		if (typeof raw !== 'string' || raw === '') return null;
		const parsed = JSON.parse(raw);
		if (parsed === null || typeof parsed !== 'object') return null;
		return {
			catalog: parsed.catalog ?? null,
			presets: parsed.presets ?? null,
			directory: parsed.directory ?? null,
			// ⚠️ **子目录缓存也要恢复**。从前这里只恢复前三项，`dirs` 一直是空的，
			// 于是每次重启 App 之后，用户点进任何一层子目录又要过一趟隧道
			// （实测 700～1200ms）—— 用户报的「打开某个文件夹还是要加载」在重启后
			// 必然复发，无论内存里预取得多勤快。
			dirs: restoreDirs(parsed.dirs),
			at: typeof parsed.at === 'number' ? parsed.at : 0,
		};
	} catch (error) {
		return null;
	}
}

/**
 * 写本地快照（临时文件 + rename，与 `saveConfig` 同一个理由）。
 *
 * @param cache - `{catalog, presets, directory, at}`。
 */
function saveDesktopCache(cache) {
	try {
		mkdirSync(STATE_DIR, { recursive: true });
		const record = {
			catalog: cache.catalog ?? null,
			presets: cache.presets ?? null,
			directory: cache.directory ?? null,
			// ⚠️ **只存最近 `DESKTOP_DIRS_PERSIST_MAX` 层**：内存里能留 200 层方便
			// 来回翻页，但全写进磁盘会让这个文件涨到好几 MB，每次 `rememberDir`
			// 都要重新 `stringify` 一遍整棵，越用越慢。用户真正会回到的是最近看过
			// 的那几层，按插入顺序取尾部即是最新的。
			dirs: tailDirs(cache.dirs),
			at: cache.at ?? Date.now(),
		};
		const tmp = `${DESKTOP_CACHE_PATH}.${process.pid}.tmp`;
		writeFileSync(tmp, JSON.stringify(record), { mode: 0o600 });
		renameSync(tmp, DESKTOP_CACHE_PATH);
	} catch (error) {
		/* 写盘失败不影响本次会话的使用，下次成功时会再写 */
	}
}

/** 默认隧道地址（占位）。自建部署改这里或设 `mobile.json` 的 `endpoint`。 */
const DEFAULT_ENDPOINT = 'https://dsh-remote.example.com';

/**
 * 凭据只允许发往这个 DNS 区（见 `assertTrustedHost`）。
 *
 * ⚠️ 这只是**默认值**。实际生效的值来自 `mobile.json` 的 `zone` 字段
 *（没写才用这里），见 `loadConfig` / `currentZone`。
 *
 * 开源版留 `example.com` 占位；自建部署把 `zone` 设成自己的域名即可，
 * **不需要改源码**。
 */
const CF_ZONE_NAME = 'example.com';

/**
 * 配对服务地址（默认值）。
 *
 * ⚠️ 实际生效的值优先取 `mobile.json` 的 `pairEndpoint`，其次环境变量
 * `DSH_REMOTE_PAIR_ENDPOINT`，都没有才用这个占位值。见 `loadConfig`。
 *
 * 与电脑端 `dsh-jiuling-remote-desktop/lib/pair.js` 的 `PAIR_ENDPOINT`
 * **必须指向同一个服务** —— 两端各写一份是因为这两个包要能独立安装，
 * 不能互相 import（一个是 host 插件、一个是另一台机器上的 host 插件）。
 */
const PAIR_ENDPOINT = 'https://pair.example.com';

/**
 * 当前生效的配对服务地址。
 *
 * 与 `activeZone` 同构：`apply()` 里随配置刷新，供同步调用点使用。
 */
let activePairEndpoint = PAIR_ENDPOINT;

/** @returns 当前生效的配对服务地址。 */
function currentPairEndpoint() {
	return activePairEndpoint;
}

/**
 * 保存连接配置（配对成功后写）。
 *
 * ⚠️ 走「临时文件 + rename」：这个文件是手机端**唯一一份**凭据，
 * 写一半断电就得重新配对一次。
 *
 * @param endpoint - 电脑的隧道地址。
 * @param token - 访问令牌。
 * @returns 落盘后的 `{ endpoint, token }`。
 */
function saveConfig(endpoint, token) {
	const record = {
		endpoint: String(endpoint).replace(/\/+$/, ''),
		token: String(token),
	};
	mkdirSync(STATE_DIR, { recursive: true });
	const tmp = `${CONFIG_PATH}.${process.pid}.tmp`;
	writeFileSync(tmp, JSON.stringify(record, null, 2), { mode: 0o600 });
	renameSync(tmp, CONFIG_PATH);
	return record;
}

/**
 * 令牌为空 —— 表示**尚未配对**。
 *
 * ⚠️ 早期这里硬编码了一个「内置访问令牌」，让两端装完即连。那是真实漏洞：
 * 常量随 npm 包公开，任何未配对的电脑都跑在**同一个公开令牌**上，谁都能连上去
 * 读他的电脑（实测用包内公开令牌打公网隧道 → HTTP 200，读到 177 个目录条目）。
 * 现在**包里不含任何凭据**：令牌只能由配对流程从服务器 claim 得到（见
 * `claimPairing`），未配对时就是空字符串，`callRemote` / `follow` 会直接报
 * 「尚未配对」，不会静默用一个假令牌去连。
 */
const EMPTY_TOKEN = '';

/**
 * **已作废**的旧公开令牌 —— 只用于识别，**不是凭据**。
 *
 * 1.0.9 及更早的版本把它当默认令牌写进 `mobile.json`。老用户升级后文件里还留着
 * 这个值，若不识别就会被 `pairStatus` 判成「已配对」，而实际上电脑端早已改用随机
 * 令牌 —— 症状是「显示已配对，一发消息就 401」。所以见到它就当成未配对。
 *
 * ## ⚠️ 为什么存哈希而不是原值
 *
 * 识别它只需要比对，不需要原值。把已泄露的令牌**再明文写一遍**没有任何好处，
 * 却会让它继续出现在每个新版本里。所以存 `sha256(原值)`。
 */
const LEGACY_PUBLIC_TOKEN_FINGERPRINT =
	'6c04899c20fc7ffab9b2fe7f9cc6e56a8b7ca0c66c96b393f8b93ab1c9784356';

/**
 * 判断一个令牌是不是那个已泄露的旧公开令牌。
 *
 * @param value - `mobile.json` 里的 `token`。
 * @returns 是否应视为「未配对」。
 */
function isLegacyPublicToken(value) {
	return createHash('sha256').update(String(value ?? ''), 'utf8').digest('hex') === LEGACY_PUBLIC_TOKEN_FINGERPRINT;
}

/**
 * 读配置。首次运行写一份模板，用户直接改文件即可。
 *
 * ⚠️ 这里**没有**任何内置令牌兜底（早期有，是漏洞）。`token` 为空即「未配对」，
 * 调用方据此直接拒绝，而不是拿一个包里公开的常量去连。
 *
 * ## `zone`（可选）
 *
 * 凭据白名单用的 DNS 区。**不写就用 `CF_ZONE_NAME` 那个默认值**。
 * 自建部署时把它设成自己的域名，否则 `assertTrustedHost` 会拒绝向
 * 自己的端点发送凭据（症状是「配对了但一点就报拒绝发送凭据」）。
 *
 * 这样就不必为了换域名去改源码 —— 开源版保持 `example.com` 占位，
 * 自用版在 `mobile.json` 里写一行即可。
 *
 * @returns `{ endpoint, token, zone }`。
 */
function loadConfig() {
	let fallbackZone = CF_ZONE_NAME;
	let fallbackPair = PAIR_ENDPOINT;
	try {
		const parsed = JSON.parse(readFileSync(CONFIG_PATH, 'utf8'));
		// ⚠️ 旧版写进文件的公开令牌要当成**未配对**，否则界面显示已配对、
		// 实际一调用就 401（电脑端已换随机令牌）。
		const raw = typeof parsed.token === 'string' ? parsed.token : EMPTY_TOKEN;
		const token = isLegacyPublicToken(raw) ? EMPTY_TOKEN : raw;
		if (typeof parsed.zone === 'string' && parsed.zone.trim() !== '') fallbackZone = parsed.zone.trim();
		if (typeof parsed.pairEndpoint === 'string' && parsed.pairEndpoint.trim() !== '') {
			fallbackPair = parsed.pairEndpoint.trim().replace(/\/+$/, '');
		}
		return {
			endpoint:
				typeof parsed.endpoint === 'string' && parsed.endpoint !== ''
					? parsed.endpoint.replace(/\/+$/, '')
					: DEFAULT_ENDPOINT,
			token,
			zone: fallbackZone,
			pairEndpoint: fallbackPair,
		};
	} catch {
		try {
			mkdirSync(STATE_DIR, { recursive: true });
			writeFileSync(
				CONFIG_PATH,
				JSON.stringify(
					{ endpoint: DEFAULT_ENDPOINT, token: EMPTY_TOKEN, zone: CF_ZONE_NAME, pairEndpoint: PAIR_ENDPOINT },
					null,
					2,
				),
				{ mode: 0o600 },
			);
		} catch {
			/* 只读环境：用默认值继续 */
		}
		return { endpoint: DEFAULT_ENDPOINT, token: EMPTY_TOKEN, zone: fallbackZone, pairEndpoint: fallbackPair };
	}
}

/**
 * 当前生效的白名单区名。
 *
 * `assertTrustedHost` 是同步的、且会被多处直接调用，所以这里读一个
 * 模块级可变值（在 `apply()` 里随配置刷新），而不是每次都去读文件。
 *
 * ⚠️ 默认是编译期的 `CF_ZONE_NAME`，只有 `mobile.json` 里显式写了 `zone`
 * 才会被覆盖 —— 这样「不配置」的行为与改动前**逐字一致**。
 */
let activeZone = CF_ZONE_NAME;

/** @returns 当前生效的白名单区名。 */
function currentZone() {
	return activeZone;
}

/**
 * 镜像工作区登记表：`<slug> → <电脑路径>`。
 *
 * ## ⚠️ 为什么必须有这张表（否则「备选区选电脑目录」会静默退化成本地会话）
 *
 * 电脑目录在手机上的落点是**镜像目录** `STATE_DIR/workspaces/<slug>`，而 slug 是
 * 电脑路径的**单向哈希**（`desktopSlug`，末 8 位是 sha256 摘要）——**推不回原路径**。
 *
 * 而用户第二次开对话时走的是**另一条路**：他不再进「电脑文件夹」对话框，而是直接在
 * 新建对话首页的**备选区**里点上次那个工作区。那条路是官方前端自己
 * `sessionController.create({workspaceId})`，**根本不经过本插件的 `createSession` 桥**
 * ⇒ `remoteSessions` 里没有这个 sessionId ⇒ prompt 拦截不命中 ⇒ 消息走手机本地模型。
 *
 * 真机症状（用户原话）：「我问他你的工作目录是什么，他回答的就是我安卓的目录，
 * 模型也没加载到电脑的」。第一次手动建时一切正常，**第二次起就变本地**。
 *
 * 所以要能从「镜像目录」反查出「电脑路径」，才能在那个路口把会话重新绑成远程。
 */
const WORKSPACES_PATH = join(STATE_DIR, 'workspaces.json');

/** 读镜像工作区登记表。 */
function loadWorkspaceMap() {
	try {
		const parsed = JSON.parse(readFileSync(WORKSPACES_PATH, 'utf8'));
		const map = parsed?.workspaces;
		if (map === null || typeof map !== 'object') return {};
		/** @type {Record<string, string>} */
		const out = {};
		for (const key of Object.keys(map)) {
			if (typeof map[key] === 'string' && map[key] !== '') out[key] = map[key];
		}
		return out;
	} catch {
		return {};
	}
}

/** 写镜像工作区登记表（失败不致命，只影响重开后的自动绑定）。 */
function saveWorkspaceMap(map, logger) {
	try {
		mkdirSync(STATE_DIR, { recursive: true });
		writeFileSync(WORKSPACES_PATH, JSON.stringify({ workspaces: map }, null, 2), { mode: 0o600 });
	} catch (error) {
		logger?.warn?.(`[dsh-remote-mobile] 保存镜像工作区登记表失败: ${String(error?.message ?? error)}`);
	}
}

/** 读远程会话清单。 */
function loadRemoteSessions() {
	try {
		const parsed = JSON.parse(readFileSync(SESSIONS_PATH, 'utf8'));
		return new Set(Array.isArray(parsed.sessions) ? parsed.sessions.filter((x) => typeof x === 'string') : []);
	} catch {
		return new Set();
	}
}

/**
 * 读「同步来的历史对话」清单。
 *
 * 与 `remoteSessions` 分开存，因为两者语义不同：`remoteSessions` 是
 * 「消息要转发电脑」的会话（含手机自己新建的远程会话）；而这一份专指
 * **从电脑同步来的历史对话** —— 它们的**历史内容不在本地**，
 * 读历史时必须转发电脑（见 `syncConversations` 的注释）。
 */
function loadSyncedSessions() {
	try {
		const parsed = JSON.parse(readFileSync(SESSIONS_PATH, 'utf8'));
		return new Set(Array.isArray(parsed.synced) ? parsed.synced.filter((x) => typeof x === 'string') : []);
	} catch {
		return new Set();
	}
}

/**
 * 读「远程会话 → 电脑路径」的落盘映射。
 *
 * ## ⚠️⚠️ 为什么必须落盘（这是「退出 App 回来就发不了消息」的根因）
 *
 * 用户报障原话：「我退出 APP 之后，后面再回来就发不了消息了」，真机报错：
 *
 *     远程会话 "session-c0d12a80-…" 不在手机本地 (gateway/internal)
 *
 * 原来的逻辑是：`prompt` 拦截器里用
 * `ctx.sessions.get(sessionId)?.header?.cwd` 反解电脑路径
 *（经 `desktopPathOfMirror(workspaceMap, cwd)`）。
 *
 * 但 **`ctx.sessions.get()` 只返回「当前活着」的会话** —— App 重启后，
 * 本地会话日志虽然在磁盘上，却**没有被载入内存**，于是 `get()` 返回
 * `undefined` ⇒ `header.cwd` 拿不到 ⇒ 反解不出电脑路径。紧接着那句
 * `if (ctx.sessions.get(...) === undefined) throw new Error('远程会话 … 不在手机本地')`
 * 直接把这次发送否掉，用户看到红条、消息发不出去。
 *
 * 而且 `pendingPlans` 是**内存** Map（故意的，见其注释），重启后也空了 ——
 * 两条路同时断，所以「重启后必然发不出去」。
 *
 * 修法：在**同步 / 建会话那一刻**把 `<sessionId, 电脑路径>` 落盘。
 * 此后无论会话是死是活，都能解出 `desktopPath` 发给电脑
 *（电脑端对已存在的会话有幂等保护，重复带不会推翻历史）。
 *
 * @returns `Record<sessionId, desktopPath>`。
 */
function loadRemotePaths() {
	try {
		const parsed = JSON.parse(readFileSync(SESSIONS_PATH, 'utf8'));
		const map = parsed?.paths;
		if (map === null || typeof map !== 'object') return {};
		/** @type {Record<string, string>} */
		const out = {};
		for (const [key, value] of Object.entries(map)) {
			if (typeof key === 'string' && key !== '' && typeof value === 'string' && value !== '') out[key] = value;
		}
		return out;
	} catch {
		return {};
	}
}

/**
 * 读「远程会话 → 用户在手机上选定的模型」的落盘映射。
 *
 * ## 为什么必须落盘（用户报障的根因）
 *
 * 用户原话：「我在手机选的是 wk 供应商的 DeepSeek V4.1 flash，然而电脑发起请求
 * 的却是 GLM 5.3 flash，我的 API 供应商没有这个模型，导致报错。」
 *
 * 原来模型的去向只有一条路：`pendingPlans`（**内存** Map）里的 `plan.model`，
 * 由 `prompt` 拦截器读出来塞进 `body.model` 一起发给电脑。
 *
 * 而 `pendingPlans` 活不过 App 重启 / 被系统杀后台 / 我部署插件时的
 * `force-stop`。一旦它空了：
 *   · `plan === undefined` ⇒ `prompt` 拦截器走「老路径」⇒ **body 不带 model**
 *   · 电脑端 `prompt` 分支里 `params.model` 是 undefined ⇒ 跳过 `selectModel`
 *   · 于是电脑用它**自己的默认模型**去请求
 * 用户看到的就是「手机选 A、电脑用 B」，而 B 若不在他的供应商里就 502。
 *
 * 所以模型选择必须像 `paths` / `titles` 一样落盘，重启后仍能带上。
 *
 * @returns `Record<sessionId, {provider, model, reasoningEffort?}>`。
 */
function loadRemoteModels() {
	try {
		const parsed = JSON.parse(readFileSync(SESSIONS_PATH, 'utf8'));
		const map = parsed?.models;
		if (map === null || typeof map !== 'object') return {};
		/** @type {Record<string, {provider: string, model: string, reasoningEffort?: string}>} */
		const out = {};
		for (const [key, value] of Object.entries(map)) {
			if (typeof key !== 'string' || key === '') continue;
			if (value === null || typeof value !== 'object') continue;
			if (typeof value.provider !== 'string' || value.provider === '') continue;
			if (typeof value.model !== 'string' || value.model === '') continue;
			out[key] = {
				provider: value.provider,
				model: value.model,
				...(typeof value.reasoningEffort === 'string' && value.reasoningEffort !== ''
					? { reasoningEffort: value.reasoningEffort }
					: {}),
			};
		}
		return out;
	} catch {
		return {};
	}
}

/**
 * 读「远程会话 → 电脑上的标题」的落盘映射。
 *
 * ## 为什么要落盘（否则「未命名」会在每次重启后复活）
 *
 * 同步会话在手机本地不落盘，本地没有 `session/title` 事件可读；而
 * `controller.follow` 的首帧必须带 `projections.values.title`，否则前端把
 * 会话摘要的标题覆盖成 `undefined`，侧边栏显示「未命名」。
 *
 * 标题只存在内存里的话，重启后 `follow` 又拿不到 ⇒ 又变「未命名」，
 * 而用户不会为此重新同步一遍。所以跟 `paths` 一起落盘。
 *
 * @returns `Record<sessionId, title>`。
 */
function loadDesktopTitles() {
	try {
		const parsed = JSON.parse(readFileSync(SESSIONS_PATH, 'utf8'));
		const map = parsed?.titles;
		if (map === null || typeof map !== 'object') return {};
		/** @type {Record<string, string>} */
		const out = {};
		for (const [key, value] of Object.entries(map)) {
			if (typeof key === 'string' && key !== '' && typeof value === 'string' && value !== '') out[key] = value;
		}
		return out;
	} catch {
		return {};
	}
}

/**
 * 读「同步会话 → 本地占用的轮次偏移」的落盘映射。
 *
 * ## 为什么需要它（用户 2026-10-22 报障，**最严重的一处**）
 *
 * 同步来的空壳会话要在侧边栏可见，必须让 `sessionListMetadata.blank` 从
 * `true` 翻成 `false` —— 而只有 `turn/start` 能翻它。所以
 * `markSessionNonBlank()` 会往本地日志里补一个**占位轮次**。
 *
 * ## 原来的写法为什么必然炸
 *
 * 原实现写 `turn/start {turn: 999999}` 且**不写 `turn/end`**，理由是想跟
 * 电脑端的真实轮次编号（1、2、3…）彻底分开。但官方校验器
 *（`dsh-session-persistence-jsonl/lib/worker.cjs`）是这样的：
 *
 *     let nextTurn = 1;                                    // L8543
 *     case "turn/start":
 *       if (openTurn !== null || data["turn"] !== nextTurn)
 *         throw new SessionFormatError(
 *           `turn/start ${data["turn"]} does not open expected turn ${nextTurn}`);  // L8574
 *
 * 也就是说 **turn 编号必须从 1 开始、严格连续递增**。`999999` 一读就炸：
 *
 *     历史加载失败: stored session "session-…" is corrupt:
 *     SessionFormatError: turn/start 999999 does not open expected turn 1
 *
 * 而 `session.append()` **写入时不校验、读取时才校验** —— 同步那一刻看着
 * 一切正常，用户下次打开这个会话就永久打不开了。
 *
 * ## 现在的方案
 *
 * 写一个**完全合法**的成对占位：`turn/start {turn:1}` + `turn/end {turn:1}`。
 * 这在格式上无懈可击（1 是第一个轮次，闭合也合法）。
 *
 * 但它会**占用本地的 turn 1**，而电脑端给这个会话派的第一个真实轮次**也是 1**
 * ⇒ 后续 `step/start {turn:1, step:1}` 会撞
 * `step/start does not match the open turn and next step`。
 *
 * 所以配合这张表：**这个会话的电脑端轮次整体 +1**（见 `mirrorEvent`）。
 * 本地 turn 1 = 占位，电脑 turn 1 → 本地 turn 2，永远不撞。
 *
 * ⚠️ 必须落盘：`mirrorEvent` 在冷启动后（App 重启）也要知道该不该 +1。
 * 只在内存里的话，重启后同步会话的事件会被写成 turn 1，立刻撞车。
 *
 * @returns `Record<sessionId, number>`（值是偏移量，目前只有 1）。
 */
function loadTurnOffsets() {
	try {
		const parsed = JSON.parse(readFileSync(SESSIONS_PATH, 'utf8'));
		const map = parsed?.turnOffsets;
		if (map === null || typeof map !== 'object') return {};
		/** @type {Record<string, number>} */
		const out = {};
		for (const [key, value] of Object.entries(map)) {
			if (typeof key !== 'string' || key === '') continue;
			if (typeof value !== 'number' || !Number.isInteger(value) || value <= 0) continue;
			out[key] = value;
		}
		return out;
	} catch {
		return {};
	}
}

/** 写远程会话清单（失败不致命，只影响重开后的自动恢复）。 */
function saveRemoteSessions(set, logger, synced, paths, titles, turnOffsets, models) {
	try {
		mkdirSync(STATE_DIR, { recursive: true });
		// ⚠️ 六份清单**存在同一个文件**里（`sessions.json`），所以写的时候必须
		// 一起写。只写 `sessions` 会把 `synced` / `paths` / `titles` /
		// `turnOffsets` / `models` 抹掉 —— 那样重开 App 之后同步来的对话会退化成
		// 「读本地（空）历史」，界面一片空白；`paths` 丢了意味着「重启后发不了
		// 消息」（见 `loadRemotePaths`）；`titles` 丢了意味着侧边栏变回「未命名」
		//（见 `loadDesktopTitles`）；`turnOffsets` 丢了意味着同步会话的事件
		// 会被写成 turn 1、和占位轮次撞车 ⇒ 会话再次 corrupt（见 `loadTurnOffsets`）；
		// `models` 丢了意味着**电脑用错模型**（见 `loadRemoteModels`，用户报障
		// 「手机选 DeepSeek、电脑用 GLM 导致 502」就是这个）。
		const body = { sessions: [...set] };
		if (synced !== undefined) body.synced = [...synced];
		if (paths !== undefined) body.paths = { ...paths };
		if (titles !== undefined) body.titles = { ...titles };
		if (turnOffsets !== undefined) body.turnOffsets = { ...turnOffsets };
		if (models !== undefined) body.models = { ...models };
		writeFileSync(SESSIONS_PATH, JSON.stringify(body, null, 2), { mode: 0o600 });
	} catch (error) {
		logger?.warn?.(`[dsh-remote-mobile] 保存远程会话清单失败: ${String(error?.message ?? error)}`);
	}
}

/**
 * 跨端请求专用的 undici 连接池。
 *
 * ## ⚠️ 为什么不能用全局 `fetch`（真机实测的硬坑）
 *
 * 在 Android 上，引擎进程里**全局 undici dispatcher 连不通公网**：同一个进程、
 * 同一个 URL、同一时刻的对照实验（`__diag` 探针）——
 *
 * | 通道 | 结果 |
 * |---|---|
 * | 全局 `fetch()`（默认 dispatcher） | **12018 ms 超时** ❌ |
 * | `new Agent({ autoSelectFamily: false })` | **2658 ms 200** ✅ |
 * | `new Agent({ connect: { family: 4 } })` | 5283 ms 200 ✅ |
 * | `node:https.request()` | 2125 ms 200 ✅ |
 *
 * DNS 正常（10 ms 解析出 2 个 IPv6 + 2 个 IPv4），所以不是解析问题；是全局
 * dispatcher 的 **Happy Eyeballs / autoSelectFamily 在 Android 上卡住**
 * （IPv6 地址拿到却连不通，又不及时回退到 IPv4）。
 *
 * 症状具有极强的误导性：**纯本地的桥方法（如 `isRemote`）几十毫秒就返回**，
 * 而任何真正出网的桥方法（`ping` / `listDesktop`）永久挂起 —— 看起来像
 * 「插件没装上」或「隧道不通」，实际是本机引擎的连接策略问题。
 *
 * 所以本插件**不复用全局 dispatcher**，自己建一个关掉 `autoSelectFamily` 的池。
 * 只影响本插件发出的请求，不碰进程全局状态。
 *
 * @param url - 完整 URL。
 * @param init - `fetch` 的第二参数。
 * @returns 响应。
 */
async function remoteFetch(url, init) {
	// ⚠️⚠️ **禁止跟随重定向**（外部审计 M7）。实测：302 跨源会**带着
	// `x-dsh-remote-token`** 打到攻击者的主机上，307 则会把整个请求体
	//（含 `{"pairCode":…}`）原样重发过去。端点被攻陷、或上游插一个重定向，
	// 令牌/配对码就送出去了。
	//
	// `redirect: 'manual'` 让 fetch 把 3xx 当普通响应返回（`res.ok === false`），
	// 于是上层按 HTTP 错误处理 —— 这正是我们要的：**宁可不配，也不把凭据送去
	// 一个不像我们服务器的地方。**
	init = { redirect: 'manual', ...init };
	assertTrustedHost(url);
	const pool = await remotePool();
	// ① 首选：undici 专用池（关掉 autoSelectFamily，Android 上最稳）。
	// ② 备用：`node:https`（强制 IPv4，不依赖 undici 解析）。
	//
	// ⚠️ **两条通道必须互为备份，不能只做「解析失败才兜底」**。线上实测过：
	// 同一个版本、同一个账号，配对服务器日志里既有 `POST /pair/claim 400`
	// （请求到了）又有界面上的 `fetch failed`（请求没到）—— 说明是**间歇性**
	// 建连失败。只修「undici 没解析到」那一种情形，另一种照样挂。
	//
	// ⚠️ 重试的**安全性由调用方的幂等键保证**，不能靠「fetch 抛错就一定没送到」
	// 这个假设（外部审计 L6）。
	//
	// 原来这里的注释写的是「`fetch()` 抛错意味着建连或响应头阶段就失败了，
	// 重发不会产生重复副作用」—— 这句话**不成立**：连接可能在服务器**已经处理完
	// 请求**、响应头还没回来时断掉（隧道掐连接、手机切网、CF 边缘超时都会这样）。
	// 那时重发就是**同一件事做两遍**。
	//
	// 对只读调用（`listDirectory` / `modelCatalog` / `presetList`）无所谓；
	// 对写操作要靠幂等键：
	//
	// | 方法 | 幂等键 |
	// |---|---|
	// | `prompt` | **`requestId`** —— 手机端**总是**带上（见 `controller.prompt`），
	// |          | 电脑端用它做 `hasPromptRequest` 去重 |
	// | `createSession` | `sessionId`（同 id 重复创建是复用） |
	// | `cancel` | 天然幂等 |
	//
	// ⚠️ 所以**新增写操作时必须一起想清楚它的幂等键**；没有幂等键的写操作
	// **不能**走这条重试路径（否则一次建连抖动 = 一次重复副作用）。
	const attempts = [];
	if (pool !== null) {
		try {
			return await pool.undici.fetch(url, { ...init, dispatcher: pool.agent });
		} catch (error) {
			if (isAbort(error)) throw error;
			attempts.push(describeFetchError(error, '走的是专用 undici 池'));
		}
	} else if (!remoteFallbackWarned) {
		remoteFallbackWarned = true;
		console.warn('[dsh-remote-mobile] 拿不到 undici，改用 node:https 通道（强制 IPv4）。');
	}
	try {
		return await nodeHttpsFetch(url, init);
	} catch (error) {
		if (isAbort(error)) throw error;
		attempts.push(describeFetchError(error, '走的是 node:https 通道（强制 IPv4）'));
		throw new Error(`两条通道都失败：\n  ① ${attempts[0]}\n  ② ${attempts[1]}`);
	}
}

/**
 * 端点域名白名单：只往 `*.example.com` 送凭据。
 *
 * ## 为什么需要（外部审计 M7）
 *
 * `payload.hostname` 是**网络对端**给的字段（配对服务器的响应、或 `mobile.json`
 * 里被改过的值），而调用方会拿它拼 URL 去发请求 —— 请求里带着 `accessToken`。
 * 审计实测的两种绕过：
 * ① `https://a@evil.com` 这类 **userinfo 写法**，人眼看是 `a@`，实际 host 是
 *    `evil.com`；
 * ② 上游插一个 302/307 重定向（已由 `redirect:'manual'` 挡掉，但域名白名单是
 *    第二道，且能挡住「直接写一个外域」这种更朴素的写法）。
 *
 * ⚠️ **用 `new URL(...).hostname` 判，不要用字符串 `includes`**：`includes` 对
 * `evil.com/?x=example.com` 和 `example.com.evil.com` 都会误判为通过。
 * ⚠️ 判的是**后缀且必须是子域边界**：`evil-example.com` 不该通过，
 * 所以比的是 `.example.com` 结尾（或正好等于 `example.com`）。
 *
 * @param url - 完整 URL。
 * @throws 当目标域名不在白名单里时。
 */
function assertTrustedHost(url) {
	let host;
	try {
		host = new URL(url).hostname.toLowerCase();
	} catch {
		throw new Error('目标地址不是合法 URL，已拒绝发送凭据');
	}
	// ⚠️ 用**当前生效**的区名（可能被 `mobile.json` 的 `zone` 覆盖），
	// 不是编译期的常量 —— 自建部署需要把白名单指向自己的域名。
	const zone = currentZone().toLowerCase();
	if (host === zone || host.endsWith(`.${zone}`)) return;
	// ⚠️ **放行 loopback**：本机调试 / 自建反代 / 测试用的就是 `127.0.0.1`，
	// 而且往 loopback 送凭据**不构成外泄**（出不了这台机器）。少了这一条，
	// 所有走本机端点的测试与自建部署都会直接报错。
	if (host === '127.0.0.1' || host === 'localhost' || host === '[::1]' || host === '::1') return;
	throw new Error(`拒绝对 ${host} 发送凭据（只允许 ${zone} 下的地址或本机）`);
}

/**
 * 是否是**主动中止**（不是网络故障）。
 *
 * ⚠️ SSE 断开重连靠 `AbortController`，把它当成网络故障包装掉，重连逻辑就失效了。
 *
 * @param error - 抛出的错误。
 * @returns 是否应当原样上抛。
 */
function isAbort(error) {
	return error?.name === 'AbortError' || error?.name === 'TimeoutError';
}

/**
 * 不依赖 undici 的 HTTPS 兜底通道（只用 Node 内置的 `node:https`）。
 *
 * ## 为什么需要它
 *
 * `undici` 是**引擎**的依赖（`<engine>/node_modules/undici`），不在插件的解析链上。
 * 一旦解析不到，退回全局 `fetch` 在 Android 上会因 **Happy Eyeballs** 卡死
 * （真机实测 12018 ms 超时，而关掉 `autoSelectFamily` 的池 2658 ms 就通）。
 * 症状是配对时一句 `fetch failed`，配对服务器日志里**根本没有这次请求**。
 *
 * `node:https` 是 Node 内置模块，**解析零风险**，且它天然按 DNS 返回顺序逐个尝试
 * 地址（不做 Happy Eyeballs 竞速），实测在真机上 2125 ms 就通。
 *
 * 这里刻意**只强制 IPv4**：Android 上运营商给 AAAA 记录、但 IPv6 实际不通是常态，
 * 而 `node:https` 遇到连不通的地址要等超时才换下一个 —— 直接锁 IPv4 最稳。
 *
 * ## 能力取舍（调用方按需使用）
 *
 * 只实现本插件真正用到的部分：POST/GET + 自定义头 + 文本/流式响应体。
 * 返回对象形状对齐 `fetch`：`{ ok, status, headers: {get}, text(), json(), body }`，
 * 其中 `body` 是**异步可迭代**的（SSE 消费方 `for await (const chunk of res.body)` 照常工作）。
 *
 * @param url - 完整 URL（https 走 TLS，http 走明文，便于本机调试）。
 * @param init - `{ method, headers, body, signal }`。
 * @returns 类 `fetch` 响应对象。
 */
function nodeHttpsFetch(url, init = {}) {
	const target = new URL(url);
	const isTls = target.protocol === 'https:';
	const send = isTls ? httpsRequest : httpRequest;
	const method = init.method ?? 'GET';
	const headers = { ...(init.headers ?? {}) };
	// 强制 IPv4：见上面的注释（IPv6 不通时能省掉一整个超时窗口）。
	const options = {
		protocol: target.protocol,
		hostname: target.hostname,
		port: target.port === '' ? (isTls ? 443 : 80) : target.port,
		path: `${target.pathname}${target.search}`,
		method,
		headers,
		family: 4,
	};

	return new Promise((resolve, reject) => {
		const req = send(options, (res) => {
			const body = res[Symbol.asyncIterator]
				? res
				: (async function* empty() {})();
			resolve({
				ok: res.statusCode >= 200 && res.statusCode < 300,
				status: res.statusCode,
				headers: {
					get: (name) => res.headers[String(name).toLowerCase()] ?? null,
				},
				body,
				text: () =>
					new Promise((done, fail) => {
						const chunks = [];
						res.on('data', (chunk) => chunks.push(chunk));
						res.on('end', () => done(Buffer.concat(chunks).toString('utf8')));
						res.on('error', fail);
					}),
				json() {
					return this.text().then((text) => JSON.parse(text));
				},
			});
		});
		req.on('error', reject);
		if (init.signal) {
			if (init.signal.aborted) {
				req.destroy();
				const abort = new Error('The operation was aborted');
				abort.name = 'AbortError';
				reject(abort);
				return;
			}
			init.signal.addEventListener('abort', () => {
				req.destroy();
				const abort = new Error('The operation was aborted');
				abort.name = 'AbortError';
				reject(abort);
			});
		}
		if (init.body !== undefined && init.body !== null) req.write(init.body);
		req.end();
	});
}

/**
 * 把 fetch 的失败原因展开成可读文字。
 *
 * ⚠️ **`fetch failed` 本身不含任何可诊断信息** —— 真正的 errno 藏在 `error.cause`
 * （有时还要再深一层 `cause.cause`）里。不展开的话，界面上只能看到「fetch failed」，
 * 而下面这些完全不同的故障会给出**一模一样**的文案，导致无法判断该改代码还是换通道：
 *
 * | 真因（在 cause 里） | 含义 | 对策 |
 * |---|---|---|
 * | `ENOTFOUND` | DNS 解析不了（运营商 DNS 污染/无网络） | 换 DNS 或换通道 |
 * | `ETIMEDOUT` / `UND_ERR_CONNECT_TIMEOUT` | 连得上 DNS、连不上 IP（被阻断） | 换通道 |
 * | `ECONNRESET` / `UND_ERR_SOCKET` | 建连后被重置（中间设备干扰 TLS） | 换通道 |
 * | `CERT_*` | 证书校验失败（手机时间不对也会这样） | 校时 |
 *
 * @param error - `fetch` 抛出的错误。
 * @param channel - 这条错误来自哪条通道（`'undici'` / `'https'`）。
 * @returns 形如 `fetch failed ← ENOTFOUND getaddrinfo ENOTFOUND pair… ← 走的是专用 undici 池`。
 */
function describeFetchError(error, channel) {
	const parts = [];
	if (error?.message) parts.push(String(error.message));
	let cause = error?.cause;
	for (let depth = 0; cause && depth < 3; depth += 1) {
		const code = cause.code ? String(cause.code) : '';
		const message = cause.message ? String(cause.message) : '';
		const text = code !== '' && message !== '' ? `${code} ${message}` : code || message;
		if (text !== '') parts.push(text);
		cause = cause.cause;
	}
	// 同时连多个地址时抛的是 AggregateError，每个子错误的 errno 都要看到。
	if (Array.isArray(error?.errors) && error.errors.length > 0) {
		parts.push(
			error.errors
				.map((item) => `${item?.code ?? ''} ${item?.message ?? ''}`.trim())
				.join(' | '),
		);
	}
	parts.push(channel);
	return parts.join(' ← ');
}

/** 解析 undici 失败时的告警只打一次（否则每条 RPC 刷一行）。 */
let remoteFallbackWarned = false;

/** 专用连接池的初始化 Promise（并发首调只初始化一次）。 */
let remotePoolPromise;

/**
 * 建（或取）专用连接池。
 *
 * @returns `{ undici, agent }`；拿不到 undici 时 `null`。
 */
function remotePool() {
	if (remotePoolPromise === undefined) {
		remotePoolPromise = (async () => {
			const undici = await loadUndici();
			if (undici === null) return null;
			return { undici, agent: new undici.Agent({ connect: { autoSelectFamily: false } }) };
		})();
	}
	return remotePoolPromise;
}

/**
 * 解析 undici 模块。
 *
 * ## ⚠️ 为什么不能只写 `await import('undici')`（配对报 `fetch failed` 的根因）
 *
 * 本插件装在 `<DSH_HOME>/profiles/<profile>/node_modules/`，而 undici 是
 * **引擎**的依赖，装在 `<engine>/node_modules/undici` —— 它在插件的解析链
 * **之外**，于是 `import('undici')` 抛 `ERR_MODULE_NOT_FOUND`，`remoteFetch`
 * 静默退回全局 `fetch`（即上面那张表里 12018 ms 超时的那个）。
 *
 * 真机实测（CDY_TN90 / Android 10 / 插件 1.0.12，2026-10-16）：
 *
 * | 写法 | 结果 |
 * |---|---|
 * | `await import('undici')`（插件目录内） | **ERR_MODULE_NOT_FOUND** ❌ |
 * | `createRequire(process.argv[1]).resolve('undici')` | ✅ → `<engine>/node_modules/undici/index.js` |
 *
 * 这个故障是**间歇性**的：网络恰好在 Happy Eyeballs 回退窗口内时全局 fetch
 * 也能通，所以会看到「同一个版本、一台手机能配对、另一台报 fetch failed」——
 * 而**配对服务器日志里根本没有那次请求**（请求没出网），这是最快的判据。
 *
 * @returns undici 模块；都失败时 `null`。
 */
async function loadUndici() {
	// ① 首选：以**引擎入口**为锚点解析。插件由引擎加载，`process.argv[1]` 就是
	//    引擎的 bin.js（实测 …/engine/node_modules/@deepseek-ai/dsh/lib/bin.js），
	//    它的 node_modules 里就有 undici。
	const entry = process.argv[1];
	if (typeof entry === 'string' && entry !== '') {
		try {
			const { createRequire } = await import('node:module');
			const { pathToFileURL } = await import('node:url');
			const resolved = createRequire(pathToFileURL(entry).href).resolve('undici');
			return await import(pathToFileURL(resolved).href);
		} catch {
			/* 换下一个锚点 */
		}
	}
	// ② 兜底：本插件自己的解析链（将来若把 undici 声明成依赖，这里会命中）。
	try {
		return await import('undici');
	} catch {
		return null;
	}
}

/**
 * 调电脑端的一条 RPC。
 *
 * @param config - `{ endpoint, token }`。
 * @param method - 方法名。
 * @param params - 参数。
 * @returns 电脑端的 `result`。
 */
async function callRemote(config, method, params = {}) {
	if (config.token === '') {
		// 未配对 = 没有令牌。**不要**提示用户去手改文件：面向大众的主路径是配对。
		throw new Error('还没配对：请在电脑的「设置 → 手机远程」里生成配对码，再到「添加工作区 → 电脑」填进去');
	}
	const res = await remoteFetch(`${config.endpoint}/api/dsh-remote/rpc`, {
		method: 'POST',
		headers: {
			'content-type': 'application/json',
			'x-dsh-remote-token': config.token,
		},
		body: JSON.stringify({ id: randomUUID(), method, params }),
	}).catch((error) => {
		// ⚠️ **网络层失败也要说人话**（同下面按状态码分档的理由）。
		//
		// `fetch` 抛的是 `TypeError: fetch failed` / `UND_ERR_CONNECT_TIMEOUT`
		// 这类东西，用户完全看不懂。而这几种都是**手机自己没网**（地铁里、
		// 飞行模式、Wi-Fi 刚断），是最常见的场景之一，必须给一句能行动的提示。
		const text = String((error && (error.code || error.message)) || error);
		if (/ENOTFOUND|EAI_AGAIN|ECONNREFUSED|ETIMEDOUT|UND_ERR|fetch failed|network/i.test(text)) {
			throw new Error(`手机这边连不上外网，请检查网络后重发（${text.slice(0, 60)}）`);
		}
		throw error;
	});
	if (!res.ok) {
		// ⚠️ **错误文案是给用户看的，不是给开发者看的。**
		//
		// 原来这里是 `远程请求失败: HTTP ${res.status}`。而最常见的情形是
		// **电脑那边没开**（睡眠/关机/没跑 DSH），隧道那头返回 502/503/504，
		// 用户看到一行「HTTP 502」完全不知道要做什么 —— 那正是「发不出消息」
		// 时最需要一句明确指引的时刻。
		//
		// 所以按状态码分档，把「接下来该干什么」写进文案：
		//   - 502/503/504：隧道通了但电脑没应答 ⇒ 让用户去看电脑；
		//   - 530：**Cloudflare 自己的错误码**，配 error 1033 时含义是
		//     「边缘认识这条隧道，但此刻没有 cloudflared 连在上面」——
		//     即「电脑开着，但隧道进程断了」。这跟「电脑没开」是**不同**的
		//     处理方式（电脑不用管，等隧道自己重连，或者去电脑上重启 DSH）。
		//     真机上这曾经是一行「远程请求失败: HTTP 530」，用户完全无从下手。
		//   - 401/403：令牌不对（多发生在电脑端重装/重新配对之后）；
		//   - 其余：保留状态码，便于用户反馈时提供线索。
		if (res.status === 530) {
			throw new Error(
				'连不上电脑的隧道（HTTP 530）：电脑多半开着，但隧道断了。请稍等几秒重试；' +
					'一直不行就去电脑上重启一下 DSH',
			);
		}
		if (res.status === 502 || res.status === 503 || res.status === 504) {
			throw new Error(
				`连不上电脑（HTTP ${res.status}）：请确认电脑上的 DSH 还开着、没休眠，然后重发一次`,
			);
		}
		if (res.status === 401 || res.status === 403) {
			throw new Error(
				`电脑不认这台手机了（HTTP ${res.status}）：多半是电脑端重新配对过，请到「添加工作区 → 电脑」里重新配对`,
			);
		}
		throw new Error(`远程请求失败: HTTP ${res.status}`);
	}
	const payload = await res.json();
	if (payload.ok !== true) throw new Error(payload.error ?? '远程请求失败');
	return payload.result;
}

/**
 * 调配对服务器的一条接口。
 *
 * ⚠️ 同样走 `remoteFetch`（专用 undici 池）：配对服务器也在公网上，
 * 用全局 `fetch` 会撞上 Android 上那个 12 秒超时的连接策略问题（见 `remoteFetch`）。
 *
 * @param path - 例如 `/pair/claim`。
 * @param body - 请求体；不传就发 GET。
 * @returns 解析后的 JSON（失败抛错，错误文案直接可读）。
 */
async function pairFetch(path, body) {
	const init = body === undefined
		? { method: 'GET' }
		: {
				method: 'POST',
				headers: { 'content-type': 'application/json' },
				body: JSON.stringify(body),
			};
	const res = await remoteFetch(`${currentPairEndpoint()}${path}`, init);
	let payload;
	try {
		payload = await res.json();
	} catch {
		throw new Error(`配对服务器返回了非 JSON（HTTP ${res.status}）`);
	}
	if (!res.ok) {
		throw new Error(payload.message ?? payload.error ?? `配对失败: HTTP ${res.status}`);
	}
	// ⚠️ **不能要求 `payload.ok === true`**：配对服务器 `claim_handshake()` 的
	// 成功响应体只有 `{hostname, accessToken, subdomain}`，**没有 `ok` 字段**
	//（失败时才是 400 + `{error, message}`）。按 `ok` 判会让每一次正常配对
	// 都报「配对失败: HTTP 200」，而 HTTP 状态码明明是对的。
	if (payload.ok === false) {
		throw new Error(payload.message ?? payload.error ?? '配对失败');
	}
	return payload;
}

/**
 * 用 6 位配对码领取这台电脑的连接信息。
 *
 * ⚠️ **配对码是一次性的**：服务器在 `claim` 成功后立刻删掉它。所以这里的顺序
 * 必须是「先领、后写盘」—— 反过来（先写盘再领）一旦写盘失败，码已经作废，
 * 用户得回电脑上重新生成一个。
 *
 * @param code - 用户输入的 6 位码（大小写、空格、连字符都由服务器容错）。
 * @returns 落盘后的 `{ endpoint, token }`。
 */
async function claimPairing(code) {
	const payload = await pairFetch('/pair/claim', { pairCode: code });
	const endpoint = `https://${payload.hostname}`;
	const token = payload.accessToken;
	if (typeof endpoint !== 'string' || typeof token !== 'string' || token === '') {
		throw new Error('配对服务器没有返回完整的连接信息');
	}
	return saveConfig(endpoint, token);
}

/**
 * 目录缓存专用的**空对象**：没有原型，因此不可能被 `__proto__` 之类的键污染。
 *
 * ## 为什么（外部审计 5.2e）
 *
 * `desktopCache.dirs` 的键来自 `params.path`（前端传上来的电脑路径）。
 * 用普通对象 `{}` 时，`dirs['__proto__'] = x` 走的是**原型设置**语义
 * （而不是添一个属性）⇒ 改写这个对象的原型链。审计标为「自伤为主」——
 * 确实不是远程接管，但那是因为这个对象的原型恰好没被别处依赖；
 * 「恰好没被依赖」不是安全边界，是运气。
 *
 * `Object.create(null)` 让 `__proto__` 变回一个**普通字符串键**，
 * 污染这条路从语义上就不存在了。同时把 `__proto__` 显式拒掉：
 * 它本来也不是一个合法的电脑路径。
 *
 * @returns 无原型空对象。
 */
function emptyDirCache() {
	return Object.create(null);
}

/**
 * 这个键能不能当目录缓存的键。
 *
 * @param path - 前端传来的路径。
 * @returns 是否可接受。
 */
function isSafeCacheKey(path) {
	return typeof path === 'string' && path !== '' && path !== '__proto__' && path !== 'constructor';
}

/**
 * 单个镜像事件的字节上限（4 MiB，外部审计 5.2b）。
 *
 * `mirrorEvent` 把**公网那台电脑**发来的事件原样 `append` 进手机本地会话。
 * 没有上限时，对端只要发一条 `data` 巨大（或条数无限）的事件，就能把手机存储
 * 写满 —— 而会话日志是**只增不减**的（`session.v4.jsonl`），写满之后 App
 * 连自己的配置都写不进去。
 *
 * 4 MiB 的依据：正常事件里最大的是 `assistant/message` 的正文与工具输出，
 * 实测都在几十 KB 量级；附件字节**不走这条路**（它们以 content part 的形式
 * 在 RPC 里传，已经在 `RPC_BODY_LIMIT` 里限过）。所以 4 MiB 对合法事件极其宽松。
 */
const MIRROR_EVENT_LIMIT = 4 * 1024 * 1024;

/**
 * 给「同步来的空壳会话」补的那个占位轮次用的编号。
 *
 * ## ⚠️⚠️ 它必须是 `1`，而且**必须成对闭合**（用户 2026-10-22 报障）
 *
 * 那个 `turn/start` 的唯一目的，是把 `sessionListMetadata.blank` 从 true
 * 翻成 false（只有 `turn/start` 能翻，见
 * `dsh-api-session-controller/lib/index.js:1808`），从而让侧边栏肯渲染
 * 这个会话。
 *
 * ## 三代方案的踩坑史（**别再来第四次**）
 *
 * ### ❌ 第一代：`turn = seq + 1`
 *
 * 空壳有 4 条环境事件，于是 turn = 5。官方 turn 从 1 连续递增 ⇒ 直接非法：
 *   `SessionFormatError: turn/start does not open a valid turn`
 * 那条会话连历史都打不开了。
 *
 * ### ❌ 第二代：`turn = 999999` + 不写 `turn/end`
 *
 * 想法是「用电脑端永远到不了的编号，两边轮次空间彻底分开」。
 * 但官方校验器（`dsh-session-persistence-jsonl/lib/worker.cjs`）是：
 *
 *     let nextTurn = 1;                                        // L8543
 *     if (openTurn !== null || data["turn"] !== nextTurn)
 *       throw new SessionFormatError(
 *         `turn/start ${data["turn"]} does not open expected turn ${nextTurn}`);  // L8574
 *
 * **编号必须从 1 开始严格连续**，`999999` 一读就炸。而
 * `session.append()` 写入时不校验、**读取时才校验** —— 同步那一刻一切正常，
 * 用户下次打开就永久报：
 *
 *     历史加载失败: stored session "session-…" is corrupt:
 *     SessionFormatError: turn/start 999999 does not open expected turn 1
 *
 * ### ✅ 第三代（现在）：`turn: 1` + **成对写 `turn/end`**
 *
 * 完全合法：1 是第一个轮次，闭合也合法，校验器一句话都挑不出来。
 *
 * 代价：**占用了本地的 turn 1**，而电脑端给这个会话派的第一个真实轮次也是 1
 * ⇒ 后续 `step/start {turn:1, step:1}` 会撞
 * `step/start does not match the open turn and next step`。
 *
 * 所以配套一张落盘表 `turnOffsets`（见 `loadTurnOffsets`）：这个会话的
 * **电脑端轮次整体 +1**（`mirrorEvent` 里做），本地 turn 1 = 占位，
 * 电脑 turn 1 → 本地 turn 2，永不撞车。
 */
const PLACEHOLDER_TURN = 1;

/**
 * 老版本（1.0.9x）写进日志的那个非法占位编号。
 *
 * 只为**识别并修复老日志**而保留：见到它就说明这个会话是旧代码标过的，
 * 里面有一条 `turn/start {turn: 999999}`（没有配对的 `turn/end`），
 * 官方校验器读到这里必炸 ⇒ 整个会话打不开。
 *
 * 见 `repairLegacyPlaceholder()`。
 */
const LEGACY_MARKER_TURN = 999999;

/**
 * 判断一条事件是否符合官方 `session wire event` 的信封要求。
 *
 * ## 为什么必须提前判（而不是交给前端）
 *
 * 前端 `assertSessionWireEvent`
 *（`dsh-api-session-controller/lib/client.js:290`）是**抛错**而不是跳过：
 * 一条坏事件会让 `SessionEventStream.follow` 整个终止，界面变成错误态。
 * 而这条流的对端是公网上的电脑，中间还可能过 Cloudflare 隧道 ——
 * 不能假设每条都合规。
 *
 * 这里只校验**信封**（与官方同一组判据），不校验 `data` 的语义：
 * `validateSessionEventData` 依赖事件类型注册表，本插件不掌握；
 * 而电脑端本来就是官方引擎，`data` 天然合规。
 *
 * @param event - 待判的事件对象。
 * @returns 是否可以直接交给前端。
 */
function isWireEvent(event) {
	if (typeof event !== 'object' || event === null || Array.isArray(event)) return false;
	// ⚠️ 官方**拒绝未知字段**（`default: throw`），所以这里也得拦。
	const allowed = ['type', 'seq', 'time', 'data', 'ignorable', 'surfaceOp', 'sourceEventSeqs'];
	for (const key of Object.keys(event)) {
		if (allowed.indexOf(key) === -1) return false;
	}
	// ⚠️ **逐条对齐 `assertSessionWireEvent`，别自己加严**。
	//
	// 这里多加一条判据（比如拒绝空 `type`）就会把一条本来合法的事件丢掉，
	// 而前端的 `SessionEventStream` 是按**连续 seq** 消费的 —— 丢一条就是缺口，
	// `assistantStream` 的 `revision` 也会对不上，症状是**整个会话打不开**，
	// 比一条事件渲染不全严重得多。所以判据只能与官方**逐字等价**：
	// 官方用 `typeof event["type"] !== "string"`（空串能过），这里也一样。
	if (typeof event.type !== 'string') return false;
	if (typeof event.seq !== 'number' || !Number.isSafeInteger(event.seq) || event.seq < 0) return false;
	if (Object.is(event.seq, -0)) return false;
	if (typeof event.time !== 'number' || !Number.isSafeInteger(event.time)) return false;
	if (!Object.prototype.hasOwnProperty.call(event, 'data') || event.data === undefined) return false;
	if (Object.prototype.hasOwnProperty.call(event, 'ignorable') && event.ignorable !== true) return false;
	return true;
}

/**
 * 把一个电脑事件写进手机本地会话。
 *
 * ## seq 对齐规则（这是搬运正确性的核心）
 *
 * ⚠️ 两端的 seq **起点不一样**，这是实测确认的：
 *
 * - 电脑端走 `sessionController.create` → agent factory → `agents.create({seed: ...})`，
 *   `seed !== undefined` ⇒ `Session` 构造时**自动插一条 `session/end-seed`**（seq=0），
 *   真实事件从 seq=1 开始（`dsh-session` L1351-1352）。
 * - 手机端走 `sessions.create(id, {meta})`，**不带 seed** ⇒ 那条自动插入不触发
 *   （L1351-1352 的条件是 `seed !== void 0`），真实事件从 seq=0 开始。
 *
 * 所以映射关系是 `手机 seq = 电脑 seq - 已跳过的 end-seed 条数`。
 * `end-seed` 只会出现在日志最前面，跳过它之后两端就逐条对齐了。
 *
 * 据此判断：
 * - 折算后的 seq < 本地 seq ⇒ 已写过（SSE 重连时电脑会补发历史），**跳过**；
 * - 折算后 === 本地 seq ⇒ 正是下一条，写入；
 * - 折算后 > 本地 seq ⇒ 中间缺事件。此时**不能硬写** —— 官方 `Session` 的 seq
 *   必须连续，硬写会造成空洞。抛错交给调用方记日志，等重连补齐。
 *
 * @param session - 手机本地会话。
 * @param event - 电脑端事件（`{type, seq, time, data, surfaceOp?, sourceEventSeqs?}`）。
 * @param state - 该会话的搬运状态 `{ skippedSeeds }`。
 * @returns 是否写入了。
 */
function mirrorEvent(session, event, state) {
	// 同理：`type` 必须是字符串，否则 `session.append` 会拿到非字符串的键。
	if (typeof event.type !== 'string' || event.type === '') {
		throw new Error('事件流里的事件缺少 type，已拒收');
	}
	// ⚠️ `session/end-seed` 必须跳过：它只在电脑日志最开头出现，写进去会让后续
	// 全部错位。这条**本身**不写入任何内容，所以它不需要 `seq` 也安全。
	//
	// ## ⚠️⚠️ 这里**不能**动 `skippedSeeds`
	//
	// 原实现是 `if (state.skippedSeeds === 0) state.skippedSeeds = 1;` ——
	// 语义是「电脑端有一条 end-seed，所以折算时整体减一」。但折算的方向取决于
	// **哪一端多了一条**，而两端的建会话路径并不对称：
	//
	// - 手机端走 `sessionController.create`（为了落盘），它带 seed ⇒ 本地日志里
	//   **有一条** `session/end-seed`（`dsh-session/lib/index.js:1352`）；
	// - 电脑端那条链路**没有**这条。
	//
	// 也就是说偏移量应当由**本地日志**决定（见 `mirrorStateOf` 的注释），
	// 而电脑端是否也发来一条 end-seed 与它无关。原来在这里再置一次 1，正好
	// 把「本地已有 end-seed」这个事实**抹掉**，于是电脑 `seq=3`（插入用户消息）
	// 折算成 3 而本地已到 4 ⇒ 被当成「已写过」丢弃 ⇒ 后面那条
	// `agent/inbox/spliced`（`removedCount:1`）找不到可删的项 ⇒ 投影器炸：
	// `invalid persisted inbox splice at session seq 5`。
	//
	// 所以这里只跳过，**不碰 state**。
	if (event.type === 'session/end-seed') {
		return false;
	}
	// ⚠️ **`seq` 必须是数字，不是数字一律拒收**（外部审计 5.2c / L8）。
	//
	// 原来的写法是 `if (typeof event.seq === 'number') { ...对齐检查... }` ——
	// 也就是说 `seq` 为 `undefined` / 字符串 / `null` 时**整段对齐检查被跳过**，
	// 事件直接 `append` 进去。而这条流的对端是**公网上的那台电脑**：它（或链路
	// 中间任何人）只要发一条没有 `seq` 的事件，就能往手机本地会话里插一条
	// 不被对齐检查约束的记录 —— 后面所有事件的 seq 随之错位，搬运逻辑失效。
	//
	// 对齐检查是**正确性的地基**，它的前置条件不能是「攻击者给面子」。
	// 所以这里倒过来写：先证明 `seq` 合法，再谈对齐；不合法就不认这条事件。
	if (typeof event.seq !== 'number' || !Number.isInteger(event.seq) || event.seq < 0) {
		throw new Error(`事件流里的 seq 不是非负整数（${JSON.stringify(event.seq)}），已拒收`);
	}
	{
		// ## seq 折算：**首条事件校准一次 delta**（不再按固定条数猜）
		//
		// 原来的写法是「本地有没有 `session/end-seed` ⇒ offset = 1 / 0」，
		// 假定两端的差异**只有**那一条 seed。这个假定在同步会话上不成立：
		// 本地日志里除了 end-seed，还可能有
		//
		//   · 占位轮次 2 条（`turn/start` + `turn/end`，见 `PLACEHOLDER_TURN`）；
		//   · 补写的 `session/title` 1 条（见 `markSessionNonBlank`）；
		//   · 可能存在的 `session/end-seed` 1 条。
		//
		// 实测两个同步会话一个本地 6 条、一个 10 条 —— 差异条数**因会话而异**，
		// 任何固定数字都必然有一边对不上，症状就是「事件全被当成缺口丢掉，
		// 手机上一片空白」或「重复写入、seq 错乱」。
		//
		// 现在改成**校准一次**：第一条真正要写的事件到来时，用
		//
		//     delta = 本地 seq - 电脑 seq
		//
		// 记下这个差值，之后所有事件都用 `localSeq = desktopSeq + delta`。
		// 这样不管本地多了几条、是哪种事件，都能自动对齐。
		//
		// ⚠️ delta 必须缓存在 `state` 上（每个会话一份），且**只在第一次写入时
		// 校准**。之后的每条事件都用同一个 delta，否则一旦某条事件被跳过，
		// delta 就会被重新算错。
		if (typeof state.seqDelta !== 'number') {
			state.seqDelta = session.seq - event.seq;
			void debugLog(
				`seq 校准 session=${session.id} 本地 seq=${session.seq} 电脑 seq=${event.seq} ⇒ delta=${state.seqDelta}`,
			);
		}
		const expected = event.seq + state.seqDelta;
		if (expected < session.seq) return false; // 重连补发的历史，已写过
		if (expected > session.seq) {
			throw new Error(
				`事件流有缺口：电脑 seq=${event.seq}（折算 ${expected}，delta=${state.seqDelta}），手机本地 seq=${session.seq}（等重连补齐）`,
			);
		}
	}
	// ⚠️ **大小上限**（外部审计 5.2b）：会话日志只增不减，一条巨型事件就能把
	// 手机存储写满，之后 App 连自己的配置都写不进去。这里在 `append` **之前**判，
	// 因为 `append` 一旦落盘就没法撤。
	//
	// 判据用 `JSON.stringify` 后的字节数（`Buffer.byteLength`）：`event.data`
	// 的结构千变万化（正文、工具输出、patch 文本），逐个字段估不现实，
	// 序列化长度才是真正会落盘的那份体积。
	let serialized;
	try {
		serialized = JSON.stringify(event.data ?? null);
	} catch (error) {
		throw new Error(`事件 data 无法序列化（${String(error?.message ?? error)}），已拒收`);
	}
	if (Buffer.byteLength(serialized, 'utf8') > MIRROR_EVENT_LIMIT) {
		throw new Error(
			`事件 data 超过 ${Math.floor(MIRROR_EVENT_LIMIT / (1024 * 1024))} MiB（type=${event.type}），已拒收`,
		);
	}
	const opts = {};
	if (event.surfaceOp !== undefined) opts.surfaceOp = event.surfaceOp;
	if (event.sourceEventSeqs !== undefined) opts.sourceEventSeqs = event.sourceEventSeqs;
	// ## ⚠️⚠️ 轮次编号整体后移（`turnOffsets`）
	//
	// 同步会话在本地写了一个**合法的占位轮次** `turn: 1`（成对闭合，
	// 见 `PLACEHOLDER_TURN`），目的是把 `blank` 翻成 false 让侧边栏可见。
	//
	// 而电脑端给这个会话派的第一个真实轮次**也是 1**。不处理的话，
	// 电脑端的 `step/start {turn:1, step:1}` 会撞上本地那个已闭合的 turn 1：
	//
	//     SessionFormatError: step/start does not match the open turn and next step
	//
	// 所以凡是带 `turn` 字段的轮次类事件，都把编号整体加上偏移量
	//（占位占了 1 个，就 +1）。这样本地 turn 1 = 占位、电脑 turn 1 → 本地 turn 2，
	// 两条序列各自连续、永不相交。
	//
	// ⚠️ 只改**轮次作用域内**的事件。`session/title`、`agent/inbox/*` 之类
	// 不带 `turn` 字段的不能动（它们没有轮次归属）。
	const turnOffset = typeof state.turnOffset === 'number' && state.turnOffset > 0 ? state.turnOffset : 0;
	let data = event.data;
	if (turnOffset > 0 && data !== null && typeof data === 'object' && typeof data.turn === 'number') {
		data = { ...data, turn: data.turn + turnOffset };
	}
	session.append(event.type, data, opts);
	return true;
}

/**
 * 从 durable 事件派生「电脑这一轮是否正在跑」，并通知前端。
 *
 * ## 为什么必须自己派生（这是「手机没有暂停按钮」的根因）
 *
 * 官方前端的整个输入区形态都由 `useSession(s => s.running)` 驱动：
 *
 * ```js
 * // dsh-api-session-controller/lib/index.js:1817
 * running: this.ctx.agents.get(session.id)?.status === 'running'
 * ```
 *
 * 而手机端的镜像会话**刻意不建 live agent**（一旦有 agent，官方 `prompt` 就会
 * 把消息交给手机本地模型真跑，这是本方案能成立的前提）。于是
 * `ctx.agents.get(id)` 永远是 `undefined` ⇒ `running` 恒为 `false` ⇒
 * ① 输入框永远只有「发送」，没有「停止生成」；
 * ② 「深度求索中 用时 N 秒」永不出现；
 * ③ 顶部「N 个后台任务运行中」也不出现。
 *
 * **三个症状是同一个根值。**
 *
 * ## 为什么这不费流量（本方案的第一准则）
 *
 * `turn/start` / `turn/end` 本来就是 durable 事件，**已经在现有的
 * `session/event` 通道里传过来了**（见 `mirrorEvent` 的无条件透传）。
 * 所以这里不需要新通道、不需要轮询、不需要额外请求 —— 纯粹是拿已经收到的
 * 数据翻一个布尔值，每个 turn 只 `emit` 两次（一开一关），几十字节。
 *
 * 「用时 N 秒」更是零流量：官方 `TurnStatus`（`dsh-client-ui-chat`）是**纯前端
 * 本地计时器**，anchor 取 `turn.start.time`，每秒 `setInterval` 自增，手机自己数就行。
 *
 * ## 为什么用 `api-session/status`
 *
 * 那是官方**已经声明**的转发事件（`typert.host.js` 里 `mode: "emit"`），
 * 前端在 `types/client/index.js:27` 用 `ctx.remote.$on('api-session/status')`
 * 订阅、经 `handleSessionStatus → handleRunning` 送到 `running`。
 * 官方服务端自己就是 `ctx.emit('api-session/status', agent.id, …)`，
 * 插件在同一个引擎里 emit 同一个事件即可，**不需要碰前端、不需要新协议**。
 *
 * @param ctx - Cordis 上下文（只用 `emit` 与 `logger`）。
 * @param sessionId - 会话 id（手机与电脑同名）。
 * @param event - 刚处理的那条 durable 事件。
 * @param state - 该会话的搬运状态；`running` 字段记录上一次已广播的值。
 */
function deriveRunning(ctx, sessionId, event, state) {
	// 只认这两条。`turn/start` 开、`turn/end` 关 —— 官方也是这么判的
	//（见 `dsh-client-ui-chat` 的 `runningTurnStartTime` 与 `turn/end` 处理）。
	if (event.type !== 'turn/start' && event.type !== 'turn/end') return;
	const running = event.type === 'turn/start';
	// ⚠️ **去重不是优化，是正确性**：SSE 重连时电脑会补发全部历史，
	// 同一个 `turn/start` 会再来一遍。不去重的话每重连一次就多 emit 一条，
	// 白白多跑一轮前端状态机（`recordMutation` 会建新数组、触发重渲染）。
	if (state.running === running) return;
	state.running = running;
	// 诊断：环形记最近 20 条派发记录，前端用 `debugRunning` 桥读出来。
	// 排查「同步会话没有运行态」时，第一件事就是确认这条到底有没有被走到。
	try {
		runningDiag.push({ at: Date.now(), sid: sessionId, running, via: 'derive' });
		if (runningDiag.length > 20) runningDiag.shift();
	} catch {
		/* 诊断失败不影响主流程 */
	}
	try {
		ctx.emit('api-session/status', sessionId, running);
	} catch (error) {
		ctx.logger?.warn?.(
			`[dsh-remote-mobile] 广播运行态失败 (${sessionId}): ${String(error?.message ?? error)}`,
		);
	}
}

/** 降级（= 拿不到引擎判据）只告警一次，别刷屏。 */
let bridgeDegradedWarned = false;

/**
 * 问一次电脑「这个会话此刻在不在跑」，并把结果派发给前端。
 *
 * ## 为什么必须有这个函数（用户 2026-10-20 报障）
 *
 * 用户原话：
 *   「发消息可以正常显示状态信息，但是如果用户退出 APP 之后，后面再回来的话，
 *    他的状态信息就不显示了，就是不显示那个深度求索中了。也不能暂停任务了。」
 *
 * ## 根因：运行态是「当前状态」，事件流只能给「历史」
 *
 * `running` 靠 `turn/start` / `turn/end` 两条 durable 事件翻布尔值
 *（见 `deriveRunning`）。但 App 重启后：
 *
 *   · `mirrorStates` 是模块级 Map，进程一死就空；
 *   · 而这一轮是**在退出期间开始的**，那条 `turn/start` 早在断流窗口里发过了；
 *   · 重连时 `fromSeq = cursor`，电脑**不会**重发它已经算过账的事件。
 *
 * ⇒ 手机从没见过这一轮的 `turn/start` ⇒ `running = false`；
 *   而电脑那边这一轮还在跑。于是「深度求索中」「停止生成」双双消失。
 *
 * **这不是补发机制能解决的**：「此刻在不在跑」是状态查询，不是事件历史。
 * 事件流里根本没有「我现在还在跑」这句话，只能问一次。
 *
 * ## 代价（本方案第一准则：不费流量）
 *
 * 每次建流一次 RPC，**不是轮询** —— 正常连着的时候一次都不发。
 * 与 `follow()`（非同步会话那条路）在 `:2139` 的做法完全一致。
 *
 * ## 为什么是模块级函数
 *
 * 它被 `followSynced`（模块级）和 `controller.follow` 拦截器（`apply()` 内）
 * 两边调用。写成 `apply()` 里的闭包，`followSynced` 就看不见它
 *（本文件这个坑踩过三次：`pendingPlans`、`desktopCache`、`desktopCatalog`）。
 * 所以依赖全部走参数传进来，不读闭包。
 *
 * ⚠️ **不 await**：校正失败（电脑离线、隧道断）就当没发生，绝不能挡住事件流。
 *
 * @param ctx - Cordis 上下文（只用 `emit` 与 `logger`）。
 * @param config - 连接配置（`{endpoint, token}`）。
 * @param sessionId - 会话 id。
 * @param isLive - 可选：返回 `false` 表示这次校正已过期（如流已断），丢弃结果。
 */
function reconcileRunning(ctx, config, sessionId, isLive) {
	callRemote(config, 'sessionStatus', { sessionId })
		.then((result) => {
			if (typeof isLive === 'function' && isLive() === false) return;
			const actual = result?.running === true;
			// ⚠️ 这里**不能**用 `deriveRunning`：那个函数只认 turn 事件，
			// 而且要去重 `state.running`。而校正的前提正是「手机的状态是错的」，
			// 所以必须直接对比电脑的真实值。
			//
			// `state.running === undefined`（App 刚重启、Map 是空的）时
			// `undefined !== false` 成立 ⇒ 电脑说 false 也会派发一次，
			// 这是对的：前端需要收到一个明确的初值，否则会停在「未知」。
			const state = reconcileStateOf(sessionId);
			if (state.running === actual) return;
			state.running = actual;
			try {
				runningDiag.push({ at: Date.now(), sid: sessionId, running: actual, via: 'reconcile' });
				if (runningDiag.length > 20) runningDiag.shift();
			} catch {
				/* 诊断失败不影响主流程 */
			}
			try {
				ctx.emit('api-session/status', sessionId, actual);
			} catch (error) {
				ctx.logger?.warn?.(
					`[dsh-remote-mobile] 校正运行态广播失败 (${sessionId}): ${String(error?.message ?? error)}`,
				);
			}
		})
		.catch(() => {
			// 校正纯属兜底，失败不影响事件流。
		});
}

/**
 * `reconcileRunning` 用的运行态状态表。
 *
 * ⚠️ **必须是模块级**，理由同 `reconcileRunning` 的注释：调用方一半在模块级
 * 函数里，看不见 `apply()` 的闭包。
 *
 * ⚠️⚠️ **这张表是运行态的【唯一】真相，`mirrorStates` 不再参与运行态判定。**
 *
 * 原来有两张表：`mirrorStates`（`apply()` 闭包里，`deriveRunning` 用）和
 * 这张（校正用）。两条路各写各的，于是：
 *
 *   · 校正说「在跑」⇒ 写这张表 ⇒ 派发；
 *   · 紧接着一条 `turn/end` 补发到 ⇒ `deriveRunning` 读 `mirrorStates`，
 *     那里还是 `undefined` ⇒ `undefined !== false` ⇒ **又派发一次**；
 *   · 反过来也一样，同一轮里同一个状态被派发两次。
 *
 * 所以现在**所有**运行态读写都走本表（`deriveRunning` 的 `state` 参数
 * 由调用方从这里取），`mirrorStates` 只留 `skippedSeeds` 那个用途。
 */
const reconcileStates = new Map();

/**
 * 取（必要时建）某个会话的校正状态。
 *
 * @param sessionId - 会话 id。
 * @returns 至少含 `running` 字段的对象。
 */
function reconcileStateOf(sessionId) {
	let state = reconcileStates.get(sessionId);
	if (state === undefined) {
		state = { running: undefined };
		reconcileStates.set(sessionId, state);
	}
	return state;
}

/**
 * 运行态派发的诊断环形缓冲（最近 20 条）。
 *
 * ⚠️ **必须是模块级**：`deriveRunning` 是模块级函数，看不见 `apply()` 里的变量
 *（这个坑在本文件里踩过三次：`pendingPlans`、`desktopCache`、`desktopCatalog`）。
 *
 * 排查「同步来的会话没有运行态」时，第一件事就是看这张表里有没有记录：
 * 有记录 ⇒ `deriveRunning` 被走到了，问题在 `emit` 之后的转发；
 * 没记录 ⇒ 根本没走到，问题在事件流本身。
 */
const runningDiag = [];

/**
 * `markSessionNonBlank` 的诊断环形缓冲（最近 30 条）。
 *
 * 排查「重新同步也补不上标题」专用：每条记 `cold`（进来时是不是冷会话）、
 * `woke`（唤醒有没有成功）、`wrote`（日志有没有真写进去）、`error`。
 *
 * 判读：`cold: true, woke: false` ⇒ 唤醒失败（`error` 里是原因）；
 * `wrote: true` 但侧边栏仍显示「未命名」⇒ 写进去了但投影没更新。
 *
 * ⚠️ 必须模块级（同 `runningDiag`）。
 */
const markDiag = [];

/**
 * `/api/dsh-remote/rpc` 请求体的上限（48 MiB）。
 *
 * ## 为什么必须有限制（外部审计 5.2a）
 *
 * 原来 handler 里是裸的 `req.on('data', c => chunks.push(c))`：
 *
 * - **没有上限**：手机上任一能打 loopback 的进程（或重载窗口内的任一页面）
 *   只要 `POST` 一个无限长的 body，就能让引擎进程把内存吃干净
 *   ⇒ OOM / 引擎重启（会话全中断）。这是**DoS**，不是「资源紧张」。
 * - **没有 `content-type` 校验**：跨站表单可以直接 `POST` 一个
 *   `text/plain` 的 body（那是**不会触发 CORS 预检**的三种 content-type 之一），
 *   于是「body 必须是 JSON」这层防护可以绕过 —— 配合 `seq` 非数字等
 *   （见 `mirrorEvent`）就能构造非预期的写入。
 *
 * ## 为什么是 48 MiB
 *
 * 图片附件是 **base64 内联**在 body 里的（见 `controller.prompt` 的注释），
 * base64 会膨胀 4/3，再算上 JSON 转义 ⇒ 32 MiB 的原图约 43 MiB 的 body。
 * 48 MiB 够放一张 32 MiB 的图，同时远小于「能把引擎搞死」的量级。
 * 手机拍照一般 3–8 MiB，正常使用碰不到这个上限。
 */
const RPC_BODY_LIMIT = 48 * 1024 * 1024;

/**
 * 电脑 → 手机的 SSE 单条消息上限（8 MiB，外部审计 5.2b）。
 *
 * `follow()` 里用 `buffer` 累积到 `\n\n` 才切一条消息。对手（公网上的那台电脑，
 * 或链路中间任何人）只要**一直不发空行**，`buffer` 就会无限增长 ⇒ 手机 OOM。
 * 8 MiB 对正常的一帧（AI 正文分片，KB 量级）有巨大余量，
 * 超过就说明对端不是在发 SSE，直接判为协议错误断开。
 */
const SSE_BUFFER_LIMIT = 8 * 1024 * 1024;

/**
 * 一次 SSE 连接活过这么久，就认为它是**健康**的，重连退避归零。
 *
 * 桌面端每 15s 发一条 `: ping` 心跳，所以一条正常连接必然能活过 15s 以上。
 * 取 30s（两个心跳周期）：既能确认链路真的通着，又不会因为偶发的一次
 * 早断就把退避永久抬高。
 *
 * ⚠️ 这条常量是为了修「退避只增不减」的 bug 而加的，见 `run()` 里的注释。
 */
const SSE_STABLE_MS = 30 * 1000;

/**
 * 读一个请求体，**带上限和类型检查**（外部审计 5.2a）。
 *
 * @param req - HTTP 请求。
 * @param limit - 上限字节数。
 * @returns `{ ok:true, text }` 或 `{ ok:false, code, message }`。
 */
function readBodyLimited(req, limit) {
	return new Promise((resolve) => {
		const chunks = [];
		let size = 0;
		let settled = false;
		const done = (value) => {
			if (settled) return;
			settled = true;
			resolve(value);
		};
		req.on('data', (chunk) => {
			if (settled) return;
			size += chunk.length;
			if (size > limit) {
				// ⚠️ 超限就**立刻**停：不能继续 push 到内存里再判断，
				// 那样上限形同虚设（攻击者还是把内存吃光了）。
				done({
					ok: false,
					code: 413,
					message: `请求体过大（上限 ${Math.floor(limit / (1024 * 1024))} MiB）`,
				});
				return;
			}
			chunks.push(chunk);
		});
		req.on('end', () => done({ ok: true, text: Buffer.concat(chunks).toString('utf8') }));
		req.on('error', (error) =>
			done({ ok: false, code: 400, message: `读取请求体失败: ${String(error?.message ?? error)}` }),
		);
	});
}

/**
 * 桥路由的准入检查。
 *
 * ## ⚠️ 为什么必须自己加（外部审计 D5）
 *
 * 引擎的 `webserver` 只按 pathname 查表，**路由命中前不做任何鉴权**；
 * 引擎自己的准入（Host / `sec-fetch-site` / Origin / 浏览器会话 cookie）加在
 * **它自己那条 `/api` 前缀路由的 handler 里**。而 `match()` 是「**精确表先命中**」——
 * 本插件的 `kind:'exact'` 路由 `/api/dsh-remote/rpc` 会比引擎的 `/api` 前缀
 * **先**被选中，于是引擎那层门根本不会被调用。
 *
 * 后果（审计原文）：手机上任一其它 App（Android loopback 跨 App 可达）或任一
 * 页面都能打 `/api/dsh-remote/rpc` —— `pair`（把手机改指到别人的电脑）、
 * `unpair`、`listDesktop`、`createSession` 全都能打；`GET /api/dsh-remote/stream`
 * 更是直接读走**全部远程会话的流式 AI 正文**。
 *
 * ## 做法：**复用引擎自己的判据**，不自己重写一套
 *
 * `connection` 服务暴露了 `requestRejection(request)` —— 它内部就是
 * ①`isTrustedApiRequest`（Host 必须 loopback 或受信 + `sec-fetch-site !== 'cross-site'`
 * + Origin 与 Host 同源）②`browserAuth.isAuthenticated`（**浏览器会话 cookie**）。
 *
 * ⚠️ **不自己照抄这两条的实现**：引擎升级判据时（比如加一条新的 fetch metadata
 * 检查），复用它就自动跟上；照抄的副本会悄悄过期，那才是真正的隐患。
 *
 * ⚠️ **第二条（cookie）才是关键**：只判 Host 是 loopback 挡不住同机的其它 App ——
 * 它们打 `127.0.0.1` 时 Host 也是 loopback。而 WebView 页面世界的请求天然带
 * 引擎签发的会话 cookie，原生 App 拿不到。所以这道门对页面透明、对其它 App 关闭。
 *
 * ## ⚠️ 取不到 `connection` 一律 **fail-closed（503）**，绝不降级（外部审计 5.1）
 *
 * 第一版在这里回退到「Host + Origin + sec-fetch-site」的弱判据 —— 那是
 * **fail-open**：弱判据里**没有 cookie 这一条**，而非浏览器客户端（本机任意
 * 其它 App）天然不带 `sec-fetch-site`，于是全过。等于把上面刚关上的门又打开。
 *
 * 更糟的是它**有一个必然出现的窗口**：cordis 的 `ctx.get(name, strict)` 对
 * 尚未 ACTIVE 的提供者返回 `undefined`（`cordis/lib/index.js:770`），而
 * client-connection 的 `apply` 里 `await BrowserAuth.create(...)` 排在
 * `provide('connection')` **之前** ⇒ 引擎启动 / 插件重载 / `cordis.patch.yml`
 * HMR 期间，本插件同步注册的路由**已经在收请求**，而 `connection` 还没就绪。
 * 审计已用手机 `engine.bin` 自带的那份 cordis 合成复现：窗口内探针全放行。
 *
 * 所以这里只有两种结局：**能拿到服务 → 用引擎的判据**；**拿不到 → 503**。
 * 「服务缺失」是暂时不可用，「降级」是一直不安全 —— 后者严重得多，不能拿
 * 可用性换。同理，`requestRejection` 自己抛错也一律 503：判据没跑成，
 * 就等于没有判据，不能默认放行。
 *
 * @param ctx - Cordis 上下文。
 * @param req - 请求。
 * @returns 拒绝时的 HTTP 状态码，放行时 `undefined`。
 */
function bridgeRejection(ctx, req) {
	const connection = ctx.get?.('connection');
	if (connection === undefined || typeof connection.requestRejection !== 'function') {
		if (!bridgeDegradedWarned) {
			bridgeDegradedWarned = true;
			ctx.logger?.warn?.(
				'[dsh-remote-mobile] connection 服务尚未就绪，桥路由一律 503（fail-closed，不降级到弱判据）',
			);
		}
		return 503;
	}
	try {
		return connection.requestRejection(req);
	} catch (error) {
		ctx.logger?.warn?.(
			`[dsh-remote-mobile] connection.requestRejection 抛错，桥路由一律 503（fail-closed）: ${String(error?.message ?? error)}`,
		);
		return 503;
	}
}

/**
 * 插件入口。
 *
 * @param ctx - Cordis 上下文。
 */
export function apply(ctx) {
	const config = loadConfig();
	// ⚠️ 先把白名单区名切成配置里那个，再走任何网络请求。
	// 漏了这一步的症状很具体：配对成功、隧道也通，但每次请求都被
	// `assertTrustedHost` 拦住，报「拒绝对 <你的域名> 发送凭据」。
	if (typeof config.zone === 'string' && config.zone !== '') activeZone = config.zone;
	if (typeof config.pairEndpoint === 'string' && config.pairEndpoint !== '') activePairEndpoint = config.pairEndpoint;
	ctx.logger?.info?.(
		`[dsh-remote-mobile] 远端: ${config.endpoint}（凭据白名单: ${activeZone}，配对服务: ${activePairEndpoint}）`,
	);
	// 诊断：确认 `userQuestions` 服务真的注入进来了。
	//
	// 这条是「电脑的 ask 能不能显示在手机上」的前置条件：拿不到这个服务，
	// `showAskViaOfficial()` 只能放弃，手机就永远不弹卡片。
	// `inject` 里声明成 `required: false`，所以缺失时插件照常加载 ——
	// 必须靠这行日志才能发现它是 undefined。
	ctx.logger?.info?.(
		`[dsh-remote-mobile] userQuestions 服务: ${ctx.userQuestions === undefined ? '缺失' : '有'}，ask 方法: ${typeof ctx.userQuestions?.ask}`,
	);
	// 诊断落盘：手机端 `ctx.logger.info` 在真机上一条都看不到（logcat 与
	// app.log 都被吞），排查只能靠写文件。只写一行，不影响任何业务逻辑。
	//
	// ⚠️ `debugLog` 定义在**模块级**（见文件头），这里直接用，不要再定义一份局部
	// 的 —— 局部版本会让模块级函数（`mirrorEvent`）看不见它而抛 ReferenceError。
	try {
		appendFileSync(
			join(STATE_DIR, 'mobile-debug.log'),
			`${new Date().toISOString()} 启动: userQuestions=${ctx.userQuestions === undefined ? '缺失' : '有'} ask=${typeof ctx.userQuestions?.ask} agents=${ctx.get('agents') === undefined ? '缺失' : '有'}\n`,
			'utf8',
		);
	} catch {
		// 诊断失败绝不影响启动
	}
	if (config.token === '') {
		ctx.logger?.info?.('[dsh-remote-mobile] 尚未配对：请到「添加工作区 → 电脑」填配对码（配对后自动连接）');
	}

	/** 远程会话 id 集合：这些会话的 prompt 转发电脑，不在手机本地跑模型。 */
	const remoteSessions = loadRemoteSessions();

	/**
	 * **从电脑同步来的历史对话**的 id 集合（`remoteSessions` 的子集）。
	 *
	 * 与 `remoteSessions` 的区别只有一件事：它们的**历史内容不在手机本地**，
	 * 所以读历史（官方 `controller.page` / `controller.follow`）要转发电脑；
	 * 而手机自己新建的远程会话（`createSession` 桥建的）历史在本地，
	 * 读本地即可，不该多跑一趟隧道。
	 */
	const syncedSessions = loadSyncedSessions();

	/**
	 * `sessionId → 电脑路径` 的落盘映射（见 `loadRemotePaths` 的注释）。
	 *
	 * ⚠️ **落盘**，与 `pendingPlans`（故意的内存 Map）不同：这份要活过 App 重启，
	 * 否则「退出 App 回来发不了消息」。写入时机是同步 / 建会话那一刻，
	 * 那时一定知道电脑路径。
	 */
	const remotePaths = loadRemotePaths();

	/**
	 * `sessionId → 用户在手机上选定的模型`（**落盘**，见 `loadRemoteModels`）。
	 *
	 * ## 为什么需要（用户报障「手机选 A、电脑用 B」的根因）
	 *
	 * 模型的去向原来只有 `pendingPlans`（内存 Map）里的 `plan.model` 一条路，
	 * 而它活不过 App 重启 / 被杀后台。丢了之后 `prompt` 拦截器走老路径、
	 * `body` 不带 `model`，电脑就退回**它自己的默认模型**去请求 —— 若那个模型
	 * 不在用户的供应商里，直接 502。
	 *
	 * 这份落盘映射是那条路的**兜底**：`prompt` 时 `plan.model` 优先，
	 * 没有就读这里。用户只要在手机上选过一次，之后哪怕重启也照样带得过去。
	 */
	const remoteModels = loadRemoteModels();

	/**
	 * `sessionId → 电脑上的标题`（**落盘**，见 `loadDesktopTitles`）。
	 *
	 * ## 为什么需要（侧边栏「未命名」的直接修复）
	 *
	 * 同步会话在手机本地**不落盘**（历史按需从电脑拉），所以本地没有
	 * `session/title` 事件可读。而 `controller.follow` 的首帧必须带
	 * `projections`，否则前端把会话摘要的 `title` 覆盖成 undefined，
	 * 侧边栏显示「未命名」——**每次切换会话都会重新发生一次**。
	 *
	 * 所以同步时把电脑上的标题记在这里，`follow` 首帧直接取用。
	 *
	 * ⚠️ **必须落盘**：只存内存的话，App 重启后 `follow` 又拿不到标题，
	 * 侧边栏重新变「未命名」—— 而用户不会去重新同步一遍。
	 */
	const desktopTitles = loadDesktopTitles();

	/**
	 * `sessionId → 本地轮次偏移量`（**落盘**，见 `loadTurnOffsets`）。
	 *
	 * 同步会话在本地写了合法的占位轮次 `turn: 1`，占住了本地的 turn 1；
	 * 电脑端的真实轮次从 1 开始，所以要整体加上这里的偏移量才不撞车
	 *（见 `mirrorEvent` 里对 `data.turn` 的处理）。
	 *
	 * ⚠️ **必须落盘**：`mirrorEvent` 在 App 重启后也要知道该不该 +1，
	 * 否则重启后同步事件会被写成 turn 1、和占位轮次撞车 ⇒ 会话再次 corrupt。
	 */
	const turnOffsets = loadTurnOffsets();

	/** 镜像工作区登记表：`<slug> → <电脑路径>`（见 `WORKSPACES_PATH` 的注释）。 */
	const workspaceMap = loadWorkspaceMap();

	/** 每个远程会话正在跟随的电脑事件流。 */
	const follows = new Map();

	/**
	 * 每个远程会话的搬运状态。
	 *
	 * - `skippedSeeds`：已跳过几条 `session/end-seed`（seq 折算用）。
	 * - `running`：上一次已广播给前端的运行态；`undefined` = 还没广播过。
	 *   见 `deriveRunning` —— 它同时用于去重，避免重连补发时重复 emit。
	 */
	const mirrorStates = new Map();

	/**
	 * 每个远程会话「在电脑上还没建出来」时的待发送计划。
	 *
	 * ## 为什么会有这个东西（用户原话）
	 *
	 * 「我们在电脑上创建一个新对话，顺序是：点击"新对话" → 选择工作区 →
	 *  选择预设 → 选择模型 → 发消息。前四步其实它不是一个有效的新建对话，
	 *  电脑也是必须发消息之后，它才创建对话的。」
	 *
	 * 「我觉得新建对话、选择预设、选择模型还有发送消息这么多个步骤，你可以
	 *  打包成一个网络请求发过去就可以了，电脑拿到这个请求之后再解析就OK了」
	 *
	 * 所以：**选工作区/预设/模型全是手机本地的状态**，一次网络都不发；等到用户
	 * 真的发第一条消息，才把「在哪个电脑目录 + 哪个预设 + 哪个模型 + 消息正文」
	 * **打包成一个 `prompt` 请求**发过去，由电脑自己解析并依次 create → selectModel
	 * → prompt。
	 *
	 * 于是「选目录 → 发送」全程只跟电脑沟通**一次**（用户明确要求的那一条）。
	 *
	 * ## 结构
	 *
	 * `sessionId → { desktopPath, presetId, model }`
	 *
	 * - `desktopPath`：电脑上的绝对路径（`C:\…`），必填；
	 * - `presetId`：`agentPreset` id，没有则不传（用电脑默认预设）；
	 * - `model`：`{provider, model, reasoningEffort?}`，没有则不设（用电脑默认模型）。
	 *
	 * ⚠️ **发完就删**：会话一旦在电脑上建好，后续消息不需要再带这些。留着也不会
	 * 出错（电脑端幂等），但会让每次发送多带几十字节 —— 而带宽是这套方案的第一
	 * 硬约束。
	 *
	 * ⚠️ **不持久化**：这是「这条会话还没在电脑上出生」的瞬时状态。重启后
	 * `remoteSessions` 仍在（已落盘），但那时电脑上其实已经建过了（或者用户放弃
	 * 了那次发送），重新推一遍预设/模型没有意义，反而可能把用户后来在电脑上改
	 * 过的设置覆盖掉。所以只存内存。
	 */
	const pendingPlans = new Map();

	/**
	 * `prompt` 拦截器诊断环形缓冲（最近 20 条）。
	 *
	 * 排查「发消息报 session not found」专用：宿主是 Node 进程，没有 `window`，
	 * 插件日志也进不了 `adb logcat`，所以只能自己存着、由 `debugPrompt` 桥读出来。
	 * 每条记 `hit`（拦截器是否命中）、`local`（会话在不在手机本地）、`plan`（有没有待发送计划）。
	 */
	const promptDiag = [];

	/**
	 * `controller.page` 拦截器诊断环形缓冲（最近 20 条）。
	 *
	 * 排查「历史加载失败：page did not end at its requested cursor」专用。
	 * 记的是前端**真正传进来**的 `throughSeq` / `beforeSeq` / `maxMessages`，
	 * 以及电脑端**实际返回**的 tail —— 两者一对比就知道差在哪。
	 * 由 `debugPage` 桥读出来。
	 */
	const pageDiag = [];

	/** 取某会话的待发送计划（没有则 `undefined`）。 */
	const planOf = (sessionId) => pendingPlans.get(sessionId);

	/**
	 * 读某会话当前选中的**预设 id**（读手机本地投影，同步、不联网）。
	 *
	 * 官方把预设存在 session 投影里：`sessionProjections.stateOf(session, 'agentPreset')`
	 *（见 `dsh-api-session-controller` 的 `presetForSession`，它就这一行）。
	 * 手机本地会话虽然**没有 live agent**，但投影是跟着会话日志走的，所以照读不误 ——
	 * 这正是「选预设」这个动作在手机侧留下的唯一痕迹。
	 *
	 * @returns 预设 id；读不到返回 `undefined`。
	 */
	function presetIdOf(ctx, sessionId) {
		try {
			const session = ctx.sessions?.get?.(sessionId);
			if (session === undefined) return undefined;
			const value = ctx.sessionProjections?.stateOf?.(session, 'agentPreset');
			return typeof value === 'string' && value !== '' ? value : undefined;
		} catch {
			// 投影服务未注册、或该 unit 没注册 —— 都当成「没选预设」。
			// 这不该阻断发送（电脑会用它的默认预设）。
			return undefined;
		}
	}

	/**
	 * 取（或建）某会话的搬运状态。
	 *
	 * ## ⚠️ `skippedSeeds` 的初始值必须**看本地日志**，不能写死 0
	 *
	 * 原实现写死 `skippedSeeds: 0`，前提是注释里那句「手机端走
	 * `sessions.create(id, {meta})`，不带 seed ⇒ 那条自动插入不触发」。
	 * **这个前提已经不成立**：`createSession` 现在走
	 * `ctx.sessionController.create({sessionId, cwd})`（为了让日志真正落盘，
	 * 见那里的注释），而官方链路会带 seed，于是
	 * `dsh-session/lib/index.js:1352`
	 *   `seed !== void 0 && ... && this.log.at(-1)?.type !== "session/end-seed"
	 *    ⇒ this.append("session/end-seed", {})`
	 * 在**手机端本地**插了一条 `session/end-seed`。
	 *
	 * 而电脑端那条链路**没有**这条（它的日志里 seq=3 直接是
	 * `agent/inbox/spliced` 插入用户消息）。两端从此差一条，后果是致命的：
	 *
	 * - 电脑 `seq=3`（插入）折算成 `3 - 0 = 3`，而手机本地 `session.seq` 已是 4
	 *   （0/1/2/end-seed）⇒ 判成「重连补发的历史，已写过」⇒ **直接丢弃**；
	 * - 电脑 `seq=5`（消费 `removedCount:1`）折算成 5，正好接上 ⇒ 写入；
	 * - 于是手机端 `next-turn` 从来没被插入过，却收到一条「删掉第 0 项」。
	 *
	 * 投影器重放时就在这一步炸掉，真机报错原文：
	 *   `SessionQueryError: failed to project session "…": invalid persisted
	 *    inbox splice at session seq 5`
	 * 而这条报错会把**整个会话**变成不可打开（`session/list` 里投影失败），
	 * 用户看到的是「新建会话失败」。
	 *
	 * 所以判据改成「本地日志里到底有没有那条 end-seed」：有就置 1，让电脑端
	 * 事件整体减一后对齐。这样两种建会话路径（带 seed / 不带 seed）都成立。
	 *
	 * ## ⚠️⚠️ 判据是「日志里有没有」，不是「`log[0]` 是不是」
	 *
	 * 这一条踩过一次，后果很重：当时写的是 `log[0]?.type === 'session/end-seed'`，
	 * 想当然以为它在最前面。**它不是。** 真机解压出来的手机端日志是
	 *
	 *     seq=0 permission/preset
	 *     seq=1 sandbox/mode
	 *     seq=2 approval/policy
	 *     seq=3 session/end-seed      ← 在第 4 位
	 *
	 * `create` 会先写三条环境事件，end-seed 排在其后。所以 `log[0]` 判断
	 * **永远为假** ⇒ `skippedSeeds` 恒为 0 ⇒ 电脑端每条事件折算后都比本地
	 * 大 1 ⇒ `mirrorEvent` 每次都抛「事件流有缺口」 ⇒ `mirrored` 恒为 false
	 * ⇒ **`deriveRunning` 一次都不被调用** ⇒ 手机端拿不到运行态：
	 *
	 * - 没有「深度求索中 用时 N 秒」；
	 * - 输入框没有「停止生成」（用户原话「不能点击暂停」）；
	 * - 顶部不显示「N 个后台任务运行中」。
	 *
	 * 三个症状同一个根值，正是用户 2026-10-18 深夜报的那一组。
	 */
	const mirrorStateOf = (sessionId) => {
		let state = mirrorStates.get(sessionId);
		if (state === undefined) {
			// ⚠️ `turnOffset`：这个会话的电脑端轮次要不要整体后移。
			//
			// 同步会话在本地写了一个合法的占位轮次 `turn: 1`（成对闭合，
			// 见 `PLACEHOLDER_TURN`），占住了本地 turn 1。而电脑端第一个
			// 真实轮次也是 1 ⇒ 必须 +1 才不会撞车。
			//
			// 初值从落盘表读（`loadTurnOffsets`），因为 App 重启后内存里没有；
			// 另外再按本地日志兜底判一次 —— 万一 `sessions.json` 丢了，
			// 只要日志里看得见「已闭合的 turn 1」，就说明占位轮次在，该 +1。
			state = { skippedSeeds: 0, running: undefined, turnOffset: 0, seqDelta: undefined };
			try {
				state.turnOffset = typeof turnOffsets?.[sessionId] === 'number' ? turnOffsets[sessionId] : 0;
			} catch {
				state.turnOffset = 0;
			}
			// 只读本地已有日志，不触发任何加载。
			//
			// ⚠️ 必须 `some()` 扫一遍，不能只看 `log[0]`（见上面的注释）。
			try {
				const session = ctx.sessions?.get?.(sessionId);
				const log = session?.log;
				if (Array.isArray(log)) {
					if (log.some((entry) => entry?.type === 'session/end-seed')) {
						state.skippedSeeds = 1;
					}
					// 兜底：日志里有「已闭合的 turn 1」⇒ 那是占位轮次 ⇒ 该 +1。
					if (state.turnOffset === 0) {
						const hasClosedTurn1 =
							log.some((entry) => entry?.type === 'turn/start' && entry?.data?.turn === PLACEHOLDER_TURN) &&
							log.some((entry) => entry?.type === 'turn/end' && entry?.data?.turn === PLACEHOLDER_TURN);
						if (hasClosedTurn1) state.turnOffset = 1;
					}
				}
			} catch {
				// 读不到就当没有 —— 与旧行为一致。
			}
			mirrorStates.set(sessionId, state);
		}
		return state;
	};

	/** 浏览器侧全局 SSE 订阅者。 */
	const watchers = new Set();

	/**
	 * `sessionId → true`：该会话的「同步会话落盘失败」是否已经提示过。
	 *
	 * 只为避免同一个会话刷屏 —— 对齐失败会**每收到一条事件就抛一次**，
	 * 不设闸的话日志几秒钟就能涨到几十兆。
	 */
	const mirrorWarnedAt = new Map();

	/**
	 * 拿到「同步会话」在手机本地的 session 对象，拿不到就尝试建一个。
	 *
	 * ## 为什么需要它
	 *
	 * `tokenUsage` / `contextPressure` 两个投影是对**本地会话日志**做 fold
	 * 算出来的。而同步来的对话在手机上原本是**纯空壳**（历史按需从电脑拉），
	 * 本地一条事件都没有 ⇒ 投影是 `undefined` ⇒ 底部那行 token / 上下文
	 * 统计整块不渲染（用户 2026-10-21 报障）。
	 *
	 * 所以这里把它唤醒（必要时建出来），让 `followSynced` 能把事件补进本地日志。
	 *
	 * ## 三条路依次尝试
	 *
	 * 1. `ctx.sessions.get()` —— 会话已经在内存里（用户刚同步过、或之前唤醒过）。
	 * 2. `ctx.sessionController.create({ sessionId, workspaceId })` —— 冷会话。
	 *    官方 `create` 对**已存在的 sessionId 是幂等的**（会加载而不是报错），
	 *    所以这条路可以反复走。
	 * 3. 都失败 ⇒ 返回 `undefined`，调用方跳过落盘、**照常转发**。
	 *
	 * ⚠️ 不传 `cwd`：同步会话的 cwd 在电脑上，手机上不存在那个目录，
	 *    传了会让 `create` 抛错。只传 `workspaceId`，让官方用工作区自己的 path。
	 *
	 * @param sessionId - 会话 id。
	 * @returns 本地 session 对象，或 `undefined`。
	 */
	/**
	 * 删掉一个**本地日志已损坏**的镜像会话目录，让它下次从电脑重新同步。
	 *
	 * 官方把会话日志放在 `<DSH_HOME>/sessions/<slug>/<sessionId>/session.v4.jsonl.zstd`，
	 * 其中 `<slug>` 是工作区 cwd 的可逆编码。镜像会话的 cwd 是本插件建的
	 * `dsh-remote/workspaces/<slug>` 目录，所以这里**按 sessionId 在整个
	 * `sessions/` 下扫一遍**，比自己拼 slug 可靠（真机上 `/data/data` 与
	 * `/data/user/0` 会互相软链，拼出来的路径经常对不上，见
	 * `desktopPathOfMirror` 的注释）。
	 *
	 * ⚠️ 只删**目录名恰好等于 sessionId** 的那一层，不碰任何别的会话。
	 *
	 * @returns 是否真的删掉了目录。
	 */
	async function removeCorruptMirror(sessionId, errorMessage = '') {
		// ## 路径来源：优先用**报错文案里给的那个**
		//
		// 官方 `SessionQueryError` 会把真实路径原样带出来：
		//
		//   ... (raw log: /data/user/0/com.dsh.android/files/home/.dsh/sessions/
		//        --...--/session-cfa85567-.../session.v4.jsonl.zstd)
		//
		// 这比自己拼 `join(DSH_HOME,'sessions',slug,sessionId)` 可靠得多：
		// 真机上 `/data/data` 与 `/data/user/0` 是互相软链，`DSH_HOME` 环境变量
		// 也未必设了（实测 `process.env.DSH_HOME` 为空），拼出来的路径经常对不上。
		const fromMessage = /\(raw log:\s*(.+?session\.v4\.jsonl\.zstd)\)/i.exec(errorMessage)?.[1];
		if (typeof fromMessage === 'string' && fromMessage !== '') {
			const dir = fromMessage.slice(0, fromMessage.lastIndexOf('/'));
			try {
				rmSync(dir, { recursive: true, force: true });
				void debugLog(`removeCorruptMirror(${sessionId}) 已按报错路径删除: ${dir}`);
				return true;
			} catch (error) {
				void debugLog(
					`removeCorruptMirror(${sessionId}) 按报错路径删除失败: ${String(error?.message ?? error)}`,
				);
			}
		}
		const home = process.env.DSH_HOME ?? '';
		if (home === '') {
			void debugLog(`removeCorruptMirror(${sessionId}) 兜底跳过：DSH_HOME 未知`);
			return false;
		}
		const root = join(home, 'sessions');
		let entries;
		try {
			entries = readdirSync(root, { withFileTypes: true });
		} catch {
			return false;
		}
		for (const entry of entries) {
			if (!entry.isDirectory()) continue;
			const target = join(root, entry.name, sessionId);
			if (!existsSync(target)) continue;
			try {
				rmSync(target, { recursive: true, force: true });
				return true;
			} catch {
				// 删不掉（权限/占用），继续找下一个 slug。
			}
		}
		return false;
	}

	async function ensureLocalMirror(sessionId) {
		const existing = ctx.sessions?.get?.(sessionId);
		if (existing !== undefined && existing !== null && typeof existing.append === 'function') {
			return existing;
		}
		// 已经试过并失败过的会话，别每条事件都再试一次（create 是异步的，
		// 失败时每条事件都重试会把事件流拖慢）。
		//
		// ⚠️ 损坏自愈时要把这个标记**清掉**再重试，否则删了坏日志也永远不再试
		//（见下面 catch 里的 `mirrorWarnedAt.delete`）。
		if (mirrorWarnedAt.get(sessionId) === true) return undefined;
		if (typeof ctx.sessionController?.create !== 'function') {
			void debugLog(`ensureLocalMirror(${sessionId}) 放弃：sessionController.create 不可用`);
			return undefined;
		}
		try {
			const request = { sessionId };
			// ## 拿 `workspaceId` 的正确方式（2026-10-22 修）
			//
			// 原来写的是 `workspaceMap.get(sessionId)` —— 这是**类型错误**：
			// `workspaceMap` 是 `slug → 电脑路径` 的普通对象（`loadWorkspaceMap`），
			// 既没有 `.get()` 方法，键也不是 sessionId。所以它必然抛
			// `workspaceMap.get is not a function`，被 catch 吞掉后返回 undefined
			// ⇒ 每个会话都「唤醒失败」⇒ 事件全丢 ⇒ 手机上永远没有回复。
			//
			// 正解：用落盘的 `remotePaths[sessionId]`（电脑路径）反推镜像工作区，
			// 也就是 `syncConversations` / `createSession` 用的同一个
			// `ensureDesktopWorkspace()` —— 它对同一目录是幂等的，会返回既有工作区。
			const desktopPath = typeof remotePaths[sessionId] === 'string' && remotePaths[sessionId] !== ''
				? remotePaths[sessionId]
				: desktopPathOfMirror(workspaceMap, ctx.sessions?.get?.(sessionId)?.header?.cwd);
			let workspaceId;
			if (typeof desktopPath === 'string' && desktopPath !== '') {
				try {
					const workspace = await ensureDesktopWorkspace(ctx, desktopPath, workspaceMap);
					if (typeof workspace?.id === 'string' && workspace.id !== '') workspaceId = workspace.id;
				} catch (error) {
					void debugLog(`ensureLocalMirror(${sessionId}) 建工作区失败: ${String(error?.message ?? error)}`);
				}
			}
			if (typeof workspaceId === 'string' && workspaceId !== '') request.workspaceId = workspaceId;
			void debugLog(
				`ensureLocalMirror(${sessionId}) 尝试 create workspaceId=${workspaceId ?? '(无)'} desktopPath=${desktopPath ?? '(无)'}`,
			);
			await ctx.sessionController.create(request);
			const created = ctx.sessions?.get?.(sessionId);
			if (created !== undefined && created !== null && typeof created.append === 'function') {
				void debugLog(`ensureLocalMirror(${sessionId}) 成功 seq=${created.seq}`);
				return created;
			}
			void debugLog(`ensureLocalMirror(${sessionId}) create 返回了但 sessions.get 仍为空`);
		} catch (error) {
			void debugLog(`ensureLocalMirror(${sessionId}) create 抛错: ${String(error?.message ?? error)}`);
			// ## ⚠️⚠️ 本地日志损坏 ⇒ **删掉重建**（2026-10-22 修）
			//
			// 真机实测（session-cfa85567）：
			//
			//   create 抛错: stored log is corrupt: SessionFormatError:
			//     session/title messageSeqs must cite distinct earlier human
			//     user/message events
			//
			// 这是**镜像写入历史遗留的坏账**：早期版本补洞/写盘时留下了一个
			// 自相矛盾的日志（实测该文件只剩 4 条、seq 从 2 直接跳到 4，
			// `session/title` 引用了不存在的事件）。官方校验器读一次抛一次，
			// 于是这个会话**永远**唤不醒 ⇒ 每条事件都「唤醒失败」⇒ 手机上
			// 这个会话永远没有新内容。
			//
			// 光靠「等重连」自愈不了：坏的是磁盘上的文件，不是流。唯一出路是
			// 把这份坏日志删掉，让下一次 `create` 从电脑重新同步一份干净的。
			//
			// ⚠️ 只删**这一个会话的目录**，且只在报错文案明确含 `corrupt` /
			// `SessionFormatError` / `SessionQueryError` 时才动 —— 别的错误
			//（比如工作区建不出来）删了没用，还会白白丢掉本地历史。
			const message = String(error?.message ?? error);
			const corrupt = /corrupt|SessionFormatError|SessionQueryError/i.test(message);
			if (corrupt && mirrorWarnedAt.get(sessionId) !== true) {
				try {
					const removed = await removeCorruptMirror(sessionId, message);
					void debugLog(
						`ensureLocalMirror(${sessionId}) 本地日志损坏，已删除重建（${removed ? '成功' : '未找到目录'}）`,
					);
					// 删完**立刻重建一次**：官方 `create` 对已存在的 id 是幂等的，
					// 日志被删掉之后它会从电脑重新同步一份干净的。
					// 不重试的话要等下一条 SSE 事件才有人再调本函数，而那时
					// `mirrorWarnedAt` 已经把会话标成放弃状态了。
					if (removed) {
						try {
							await ctx.sessionController.create(request);
							const rebuilt = ctx.sessions?.get?.(sessionId);
							if (rebuilt !== undefined && rebuilt !== null && typeof rebuilt.append === 'function') {
								void debugLog(`ensureLocalMirror(${sessionId}) 重建成功 seq=${rebuilt.seq}`);
								return rebuilt;
							}
							void debugLog(`ensureLocalMirror(${sessionId}) 重建后 sessions.get 仍为空`);
						} catch (rebuildError) {
							void debugLog(
								`ensureLocalMirror(${sessionId}) 重建失败: ${String(rebuildError?.message ?? rebuildError)}`,
							);
						}
					}
				} catch (removeError) {
					void debugLog(
						`ensureLocalMirror(${sessionId}) 删除损坏日志失败: ${String(removeError?.message ?? removeError)}`,
					);
				}
				// 无论重建成功与否都记下「试过了」，避免每条事件都删一次文件。
				mirrorWarnedAt.set(sessionId, true);
			}
			ctx.logger?.info?.(
				`[dsh-remote-mobile] 唤醒同步会话 ${sessionId} 失败（不影响显示）: ${message}`,
			);
		}
		return undefined;
	}

	/**
	 * 电脑侧的静默缓存：`{ catalog, presets, directory, at }`。
	 *
	 * ## 为什么要缓存（用户需求原文）
	 *
	 * 「用户配对成功之后 就可以静默的拉取一些数据了，比如模型列表和预设列表，
	 * 以及目录树这样体验会好很多……因为一般项目目录是不会变的，可以提前加载」。
	 *
	 * 配对后立刻拉一次，之后前端一打开模型菜单就有内容，不用等一轮往返。
	 *
	 * ⚠️ **`directory` 缓存「根」那一层**（`listDirectory` 不给 path 时返回的
	 * home），因为「点开电脑页签」是每次都逃不掉的第一步，而它现在必然要过一轮
	 * 隧道往返 —— 用户看到的就是那个转圈（原话：「还是有一个加载过程，
	 * 不能预加载吗」）。
	 *
	 * ⚠️ **`dirs` 是「用户正在看的那一层」的下一层**（`电脑路径 → listing`）。
	 * 只缓存根层还不够：用户报障原文「你现在好像只缓存了顶级目录，打开某个
	 * 文件夹还是要加载」—— 点进任何一个子目录仍要过一次隧道往返（实测
	 * 700～1200ms）。而用户看到列表到点击之间的那一两秒，完全够把这一层的
	 * 子目录拉回来（见 `prefetchDirs`）。
	 *
	 * **整棵目录树不进缓存**：电脑上几万个目录，全拉一遍既慢又没意义，而且
	 * 会大面积显示过期内容。所以 `dirs` 只装「用户实际看过的那几层的下一层」，
	 * 上限见 `DESKTOP_DIRS_MAX`。过期窗口由「刷新」按钮关闭。
	 *
	 * ⚠️ **`catalog` / `presets` / `directory` 三项同时落盘**（见
	 * `DESKTOP_CACHE_PATH`）：重开 App、甚至电脑隧道断了，这三样也从本地读，
	 * 读路径**完全不碰网络**。`dirs` **不落盘** —— 它是按层预取的临时加速，
	 * 数量会膨胀到几百条，存下来既无必要也让文件变大。
	 */
	const desktopCache = { catalog: null, presets: null, directory: null, dirs: emptyDirCache(), at: 0 };

	// 启动即从磁盘恢复上次拉到的电脑配置 —— 这是「配对后只拉一次」的落地点。
	const restoredCache = loadDesktopCache();
	if (restoredCache !== null) {
		desktopCache.catalog = restoredCache.catalog;
		desktopCache.presets = restoredCache.presets;
		desktopCache.directory = restoredCache.directory;
		desktopCache.dirs = restoredCache.dirs;
		desktopCache.at = restoredCache.at;
	}

	/** 把当前内存里的三项落盘（`dirs` 不存，见 `desktopCache` 的注释）。 */
	const persistDesktopCache = () => {
		saveDesktopCache({
			catalog: desktopCache.catalog,
			presets: desktopCache.presets,
			directory: desktopCache.directory,
			at: desktopCache.at,
		});
	};

	/** `desktopCache.dirs` 的容量上限（按插入顺序淘汰最旧的那些）。 */
	const DESKTOP_DIRS_MAX = 200;

	/**
	 * 把用户**正在看的这一层**的子目录拉回手机本地。
	 *
	 * ## 为什么要有这个（用户报障原文）
	 *
	 * 「你现在好像只缓存了顶级目录，打开某个文件夹还是要加载」—— 根层命中缓存后
	 * 不再打隧道了，可点进任何一层子目录仍然要过一趟 Cloudflare，实测
	 * 700～1200ms，用户看到的就是那个转圈。
	 *
	 * 而**用户看列表、再决定点哪个文件夹，中间至少有一两秒**。这段时间完全够
	 * 把这一层的子目录并发拉回来 —— 用户点下去时就已经在本地了。
	 *
	 * ## 只预取「一层」而不是整棵树
	 *
	 * 电脑上一个项目根目录动辄几十上百个子目录，整棵树递归下去是几千次往返，
	 * 既慢又会大面积显示过期内容。**只拉下一层**：用户真正会点的就是这一层里
	 * 的那几个，而下下一层等他进了这一层再预取（`listDesktop` 每次都会触发）。
	 *
	 * ## 并发上限
	 *
	 * 隧道是单个 Cloudflare 连接，一次打几十个并发没有意义还会互相挤。所以
	 * 分批（每批 `DESKTOP_PREFETCH_BATCH` 个）顺序推，**不 await**：这是后台
	 * 动作，调用方（`listDesktop`）要立刻把当前层回给前端。
	 *
	 * @param listing - 用户当前看到的那一层（`listDirectory` 的产物）。
	 */
	const prefetchDirs = (listing) => {
		if (config.token === EMPTY_TOKEN) return;
		const dirs = listing?.entries?.filter?.((entry) => entry?.isDirectory === true) ?? [];
		if (dirs.length === 0) return;
		// ⚠️ **已经缓存过的不再拉**：来回翻页时不能把同一批目录重复请求一遍。
		const todo = dirs
			.map((entry) => entry.path)
			.filter((path) => typeof path === 'string' && path !== '' && desktopCache.dirs[path] === undefined);
		if (todo.length === 0) return;
		void (async () => {
			for (let at = 0; at < todo.length; at += DESKTOP_PREFETCH_BATCH) {
				const batch = todo.slice(at, at + DESKTOP_PREFETCH_BATCH);
				await Promise.all(
					batch.map(async (path) => {
						// 期间用户可能已经点「刷新」清过缓存，那就别再写回去了。
						if (desktopCache.dirs[path] !== undefined) return;
						try {
							const child = await callRemote(config, 'listDirectory', { path });
							rememberDir(path, child);
						} catch {
							/* 单个目录拉不到（被删 / 无权限）不影响其它目录，静默跳过 */
						}
					}),
				);
			}
		})();
	};

	/**
	 * 每批并发数（见 `prefetchDirs` 的注释）。
	 *
	 * ⚠️ 原来是 6，一层 120 个目录要分 20 批、每批一轮隧道往返（700～1200ms），
	 * 整层预取完要十几秒 —— 用户根本等不到，点下去还是「加载中」。
	 * 提到 24 之后大约 5 批就能把一层铺完，落在用户看列表的那一两秒里。
	 */
	const DESKTOP_PREFETCH_BATCH = 24;

	/**
	 * 写一格目录缓存，超出上限就按插入顺序淘汰最旧的。
	 *
	 * ⚠️ 淘汰必须用 `Object.keys` 的**插入顺序**：JS 对象的字符串键就是这个顺序，
	 * 而 `desktopCache.dirs` 正好是按用户浏览顺序写进去的 —— 最旧的键就是最早
	 * 看过的那些层，淘汰它们对用户体感最没影响。
	 *
	 * @param path - 电脑上的绝对路径。
	 * @param listing - 该层的列表结果。
	 */
	function rememberDir(path, listing) {
		// ⚠️ 拒掉 `__proto__` / `constructor`（外部审计 5.2e）。配合
		// `emptyDirCache()` 的无原型对象，这是双保险：前者防语义，后者防意外。
		if (!isSafeCacheKey(path)) return;
		desktopCache.dirs[path] = listing;
		const keys = Object.keys(desktopCache.dirs);
		for (let at = 0; at + DESKTOP_DIRS_MAX < keys.length; at += 1) {
			delete desktopCache.dirs[keys[at]];
		}
		// ⚠️ **只是写内存不够**：不落盘的话 App 一重启 `dirs` 就没了，用户点进
		// 任何一层子目录又要过一趟隧道 —— 那正是用户报的「打开文件夹还是要加载」。
		// 节流：预取时一层会并发写 6 个目录，逐次落盘会把主线程拖死。
		schedulePersistDirs();
	}

	/** 子目录缓存落盘的防抖句柄（见 `schedulePersistDirs`）。 */
	let persistDirsTimer = null;

	/**
	 * 把子目录缓存落盘，**合并 800ms 内的多次写入**。
	 *
	 * 用 `unref()` 让这个定时器不阻止进程退出；写盘本身在
	 * `saveDesktopCache` 里已经全程 try/catch，失败不影响使用。
	 */
	function schedulePersistDirs() {
		if (persistDirsTimer !== null) return;
		persistDirsTimer = setTimeout(() => {
			persistDirsTimer = null;
			persistDesktopCache();
		}, 800);
		if (typeof persistDirsTimer?.unref === 'function') persistDirsTimer.unref();
	}

	/**
	 * 静默预拉电脑的模型目录与预设列表。
	 *
	 * ⚠️ **失败必须完全静默**：这是配对后的后台动作，用户没点任何东西。
	 * 弹错误只会让人以为配对坏了 —— 而实际上配对是成功的，只是这台电脑的
	 * 某个 provider 没配好（官方 `modelCatalog` 会把这种失败放进 `failures`，
	 * 根本不会抛）。
	 *
	 * @param why - 日志里标明是谁触发的（配对 / 手动刷新）。
	 */
	const prefetchDesktop = async (why) => {
		if (config.token === EMPTY_TOKEN) return;
		const [catalog, presets, directory] = await Promise.all([
			callRemote(config, 'modelCatalog', {}).catch((error) => {
				ctx.logger?.warn?.(`[dsh-remote-mobile] 预拉模型列表失败（${why}）: ${String(error?.message ?? error)}`);
				return null;
			}),
			callRemote(config, 'presetList', {}).catch((error) => {
				ctx.logger?.warn?.(`[dsh-remote-mobile] 预拉预设列表失败（${why}）: ${String(error?.message ?? error)}`);
				return null;
			}),
			// 根目录那一层：不带 path ⇒ 电脑返回它的 home，正是用户点开
			// 「电脑」页签第一眼看到的东西。见 `desktopCache.directory` 的注释。
			callRemote(config, 'listDirectory', {}).catch((error) => {
				ctx.logger?.warn?.(`[dsh-remote-mobile] 预拉目录树失败（${why}）: ${String(error?.message ?? error)}`);
				return null;
			}),
		]);
		if (catalog !== null) desktopCache.catalog = catalog;
		if (presets !== null) desktopCache.presets = presets;
		if (directory !== null) desktopCache.directory = directory;
		desktopCache.at = Date.now();
		// ⚠️ 只有**真的拉到东西**才落盘。全失败时保留上次的本地快照，
		// 而不是把缓存写成空 —— 用户要的是「上次拉到的结果存死在本地」。
		if (catalog !== null || presets !== null || directory !== null) persistDesktopCache();
		const models = catalog?.groups?.reduce((n, g) => n + (g.models?.length ?? 0), 0) ?? 0;
		const dirs = directory?.entries?.length ?? directory?.items?.length ?? 0;
		ctx.logger?.info?.(
			`[dsh-remote-mobile] 已预拉电脑数据（${why}）：${models} 个模型、${presets?.presets?.length ?? 0} 个预设、根目录 ${dirs} 项`,
		);
	};

	/**
	 * 取电脑的模型目录，缓存为空就**当场拉一次**。
	 *
	 * ⚠️ **不能只依赖配对时的预拉**。那是异步的后台动作，而前端页面一加载
	 * 就会调一次 `modelCatalog`（`ModelCatalogDirectory` 构造后立刻 `load()`，
	 * 见 `dsh-client-ui-model-selection/lib/client.js:354`）—— 谁先到谁决定
	 * 用户看到什么。拿不到电脑那份就返回手机本地列表，症状正是用户报的
	 * 「预设列表正确地加载了电脑预设，但模型列表还是没同步」：预设走
	 * `remoteExportList`，**没有缓存层**、每次重读，所以它是对的。
	 *
	 * @returns 电脑的模型目录；拉不到时 `null`。
	 */
	const desktopCatalog = async () => {
		if (desktopCache.catalog !== null) return desktopCache.catalog;
		try {
			desktopCache.catalog = await callRemote(config, 'modelCatalog', {});
			// 第一次拉到就落盘：此后重开 App / 电脑下线都从本地读，不再联网。
			desktopCache.at = Date.now();
			persistDesktopCache();
			return desktopCache.catalog;
		} catch (error) {
			ctx.logger?.warn?.(
				`[dsh-remote-mobile] 取电脑模型列表失败: ${String(error?.message ?? error)}`,
			);
			return null;
		}
	};

	/**
	 * 把电脑的模型目录并进手机本地的目录。
	 *
	 * ## ⚠️ 为什么是「并」而不是「换」
	 *
	 * 官方前端的模型目录是**全局唯一一份**（`ModelCatalogDirectory` 由
	 * `ModelDirectoryResolver` 持有，所有会话共享，见
	 * `dsh-client-ui-model-selection/lib/client.js:340`），请求本身**不带 sessionId**。
	 * 所以「远程会话看电脑的、本地会话看手机的」这种按会话切换，在**这一层**
	 * 表达不出来。
	 *
	 * 折中：这里把两份**并起来**（电脑的排前面，同 id 的 provider 以电脑为准），
	 * 由前端按「当前会话的工作区是不是电脑镜像目录」再筛一次
	 * （见 client.js 的 `filterCatalogForWorkspace`）。合并时必须给电脑那几组
	 * 打上 `desktop: true`，否则前端分不出哪些是电脑的 —— 用户需求原文：
	 * 「用户如果当前选择的是 app 本地文件夹，那么就不要出现电脑的模型，
	 * 以这个文件夹为隔离条件，互不干预」。
	 *
	 * ⚠️ **`localGroups` / `localFailures` 必须一起带上**。因为同 id 的 provider
	 * 以电脑为准 ⇒ 本地那一组被**丢掉**了；而本地会话的 `current` 很可能正指向
	 * 它（手机本地的 `deepseek-official` 与电脑的同名）。前端要「本地工作区只显示
	 * 本地模型」，就得能把被覆盖的那组**还原**回来 —— 只靠 `groups` 做不到，
	 * 那组已经不在里面了。这两个字段是**本插件加的**，官方 `syncInputs` 只读
	 * `groups` / `failures` / `default`，多带两个不会影响渲染。
	 *
	 * ⚠️ `desktop` 同理是**我们加的非官方字段**。前端渲染只读 `id` / `name` /
	 * `models`（见 `optionsOf`），多一个字段无害。
	 *
	 * @param local - 手机本地的模型目录。
	 * @param desktop - 电脑的模型目录（`buildModelCatalog` 的产物）。
	 * @returns 合并后的目录（额外带 `localGroups` / `localFailures`）。
	 */
	function mergeCatalog(local, desktop) {
		const groups = (desktop?.groups ?? []).map((group) => ({ ...group, desktop: true }));
		const seen = new Set(groups.map((group) => group.id));
		for (const group of local?.groups ?? []) {
			if (!seen.has(group.id)) groups.push(group);
		}
		// ## ⚠️⚠️ `default` 必须用**电脑的**，不能跟着 `...local` 走
		//
		// 用户报障原话：「我选择的非官方模型的时候，还是显示了 1017 这个标签，
		// 不管选哪个模型都有这个标签。」
		//
		// 根因就在这一行：原来写的是 `...local`，于是 `default` 取的是**手机本地**
		// 的默认模型（`deepseek-official` / `deepseek-flash`）。
		//
		// 而官方前端是这么算「当前选中」的
		//（`dsh-client-ui-model-selection/lib/client.js:225`）：
		//
		//     const current = projected.next ?? catalog.value.default;
		//
		// 只要会话投影里还没有 `model/selection`（新建会话、会话日志刚被重建、
		// 或者 `selectModel` 因日志损坏没写成），`current` 就退回
		// `catalog.default` —— 也就是上面那个**官方的** `deepseek-flash`。
		// 于是不管用户在菜单里点哪个第三方模型，前端算出来的 provider 永远是
		// `deepseek-official`，官方 1017 插件按
		// `provider.id === 'deepseek-official' && model.id === 'deepseek-flash'`
		// 判断，必然命中 ⇒ 标签永远挂着、换哪个都不消失。
		//
		// 用户在电脑上选的默认模型才是该端生效的那个（电脑才是真源），所以这里
		// 优先用电脑的 `default`；电脑没给才退回本地。
		const desktopDefault = desktop?.default;
		const useDesktopDefault =
			desktopDefault !== null
			&& typeof desktopDefault === 'object'
			&& typeof desktopDefault.provider === 'string'
			&& desktopDefault.provider !== ''
			&& typeof desktopDefault.model === 'string'
			&& desktopDefault.model !== '';
		return {
			...local,
			...(useDesktopDefault ? { default: { ...desktopDefault } } : {}),
			groups,
			// failures 同样打标记：本地工作区里不该看到电脑 provider 的加载失败。
			failures: [
				...(desktop?.failures ?? []).map((failure) => ({ ...failure, desktop: true })),
				...(local?.failures ?? []),
			],
			// ⚠️ 前端「本地工作区只显示本地模型」的还原依据（见上面注释）。
			localGroups: local?.groups ?? [],
			localFailures: local?.failures ?? [],
			// ⚠️ 本地默认也要带上：前端在**本地工作区**里要能还原回手机自己的默认
			//（与 `localGroups` / `localPresets` 同一个用途）。
			localDefault: local?.default,
			routableProviders: [
				...new Set([...(local?.routableProviders ?? []), ...(desktop?.routableProviders ?? [])]),
			],
		};
	}

	/**
	 * 把电脑的预设并进手机本地的预设名单（同 id 以电脑为准）。
	 *
	 * ⚠️ **与 `mergeCatalog` 同构，必须给电脑那几条打 `desktop: true`。**
	 *
	 * `remoteExportList()` 是**全局 RPC**（`dsh-agent-preset-registry/lib/index.js:592`
	 * 的 `async remoteExportList()` 连一个参数都不收），宿主侧**拿不到当前会话**，
	 * 所以「远程会话看电脑的、本地会话看手机的」在这一层表达不出来 ——
	 * 与模型目录完全同一个限制（见 `mergeCatalog` 的注释）。
	 *
	 * 折中做法也照抄模型目录那一套：这里把两份**并起来**并打标记，
	 * 由前端按「当前会话的工作区是不是电脑镜像目录」再筛一次
	 * （见 client.js 的 `pickWorkspacePresets`）。
	 *
	 * 不打标记的后果就是用户报的：「模型列表能够快速切换回来了，但是预设列表
	 * 没有切换」—— 预设一直是两份合并的全集，切到哪个工作区都一样。
	 *
	 * ⚠️ **`localPresets` 必须一起带上**：同 id 以电脑为准 ⇒ 本地那一条被丢掉了，
	 * 而本地工作区要能把它还原回来（前端只靠 `presets` 做不到，那条已经不在里面）。
	 * 这个字段是本插件加的，官方 `presetOptions()` 只读 `id` / `name` /
	 * `description` / `broken`（`dsh-client-ui-agent-preset/lib/client.js:1138`），
	 * 多带一个不会影响渲染。
	 *
	 * @param local - 手机本地的 `{presets}`。
	 * @param desktop - 电脑的 `{presets}`。
	 * @returns 合并后的名单（额外带 `localPresets`）。
	 */
	function mergePresets(local, desktop) {
		const presets = (desktop?.presets ?? []).map((preset) => ({ ...preset, desktop: true }));
		const seen = new Set(presets.map((preset) => preset.id));
		for (const preset of local?.presets ?? []) {
			if (!seen.has(preset.id)) presets.push(preset);
		}
		return { ...local, presets, localPresets: local?.presets ?? [] };
	}

	/**
	 * 把电脑发起的 ask 显示成**手机上的官方卡片**。
	 *
	 * ## 为什么调官方而不是自绘（用户原话：「手机的原声引擎也是有这个卡片的呀，
	 *    你就不能直接调用吗？」）
	 *
	 * 对，能直接调，而且这才该是正解。手机本地引擎里就装着
	 * `dsh-client-ui-user-questions` / `dsh-client-ui-approval`，它们挂在
	 * 本地 `user-questions/request` / `approval/request` waterfall 上。
	 * 这里只要调一次 `ctx.userQuestions.ask()`，官方就把那张卡片原样渲染出来 ——
	 * 带选项序号、带「推荐」标、带跳过/提交、带多题翻页，与手机自己提问
	 * 长得一模一样。
	 *
	 * 自绘那套（`showAskCard`）的代价是维护一套平行 UI，官方改版就落后；
	 * 而且窄屏下手写的按钮很难和官方的观感对齐。所以改走这条路。
	 *
	 * ## 为什么不传 `agent`
	 *
	 * `ask()` 的 `request.agent` 是**可选**的：传了会先过 `assertLiveRoot()`
	 * 校验「必须是当前存活的运行时根」，不传则直接派发到根 waterfall。
	 * 电脑那边的 Agent 实例在手机进程里根本不存在，传任何一个手机本地的
	 * agent 都只会让 `assertLiveRoot` 抛 `CALLER_NOT_LIVE`。
	 * 所以不传 —— 走根作用域，由官方 UI 接手。
	 *
	 * @param ctx - 宿主上下文。
	 * @param sessionId - 这个 ask 属于哪个远程会话（回传时要用）。
	 * @param payload - 电脑端推来的 `{ requestId, kind, questions?, toolName? }`。
	 */
	async function showAskViaOfficial(ctx, sessionId, payload) {
		const svc = ctx.userQuestions;
		// 落盘诊断（真机上看不到 logger 输出，见 `apply` 里那段注释）。
		const trace = (line) => {
			try {
				appendFileSync(join(STATE_DIR, 'mobile-debug.log'), `${new Date().toISOString()} ${line}\n`, 'utf8');
			} catch {
				// 忽略
			}
		};
		trace(
			`收到 ask: kind=${payload.kind} requestId=${payload.requestId} 有userQuestions=${svc !== undefined && svc !== null} questions=${Array.isArray(payload.questions) ? payload.questions.length : 'n/a'}`,
		);
		ctx.logger?.info?.(
			`[dsh-remote-mobile] 收到 ask: kind=${payload.kind} requestId=${payload.requestId} 有userQuestions=${svc !== undefined && svc !== null} questions=${Array.isArray(payload.questions) ? payload.questions.length : 'n/a'}`,
		);
		// 引擎里没有这个服务（老版本 / 被关掉）⇒ 静默放弃。
		// 电脑端会一直等到 ASK_TIMEOUT_MS 再走官方兜底，不会永久卡住。
		if (svc === undefined || svc === null) {
			ctx.logger?.warn?.('[dsh-remote-mobile] 本机没有 userQuestions 服务，电脑的 ask 无法显示');
			return;
		}

		try {
			if (payload.kind === 'approval') {
				// ── 权限授权 ──
				//
				// 官方的 approval 卡片是 `ctx.approval.request()` 触发的，
				// 而手机插件没 inject approval（它是引擎内部服务，调用方必须是
				// 真实发起工具调用的那一方，凭空构造一个「谁请求了什么」在
				// 语义上不成立）。
				//
				// 所以授权这类**不借官方卡片**，仍用自绘的那张两按钮卡片：
				// 它的信息量本来就小（工具名 + 允许/拒绝），自绘反而更清楚。
				broadcast('ask', { sessionId, ...payload });
				return;
			}

			const questions = Array.isArray(payload.questions) ? payload.questions : [];
			if (questions.length === 0) return;

			// 找一个「当前存活的运行时根 agent」。
			//
			// 为什么必须找：`ask()` 里 `assertLiveRoot()` 要求传入的 agent
			// 满足 `agents.get(id) === agent` 且 `agents.roots().includes(agent)`，
			// 否则抛 `CALLER_NOT_LIVE` / `DELEGATED_CALLER`。
			//
			// 为什么优先选「这个远程会话对应的」：官方 UI 用
			// `ctx.sessions.scopeOf(owner)` 决定卡片挂在哪个会话下。选错 agent
			// 会让卡片出现在**另一个会话**里 —— 用户看着当前会话，卡片却在别处。
			//
			// ⚠️ 一个都找不到就放弃：手机端这个远程会话没在本地跑 agent 时，
			//    强行传一个别的 agent 会把卡片挂错地方，比不弹更糟。
			const agents = ctx.get('agents');
			let liveRootAgent;
			if (agents !== undefined && agents !== null) {
				try {
					const roots = typeof agents.roots === 'function' ? agents.roots() : [];
					// 先找 session 匹配的
					liveRootAgent = roots.find((a) => a?.session?.id === sessionId);
					// 没有就退而取第一个（卡片至少能弹出来，用户能选）
					if (liveRootAgent === undefined && roots.length > 0) liveRootAgent = roots[0];
				} catch (error) {
					ctx.logger?.warn?.(`[dsh-remote-mobile] 取 agents 失败: ${String(error?.message ?? error)}`);
				}
			}
			ctx.logger?.info?.(
				`[dsh-remote-mobile] ask 的 agent: ${liveRootAgent === undefined ? '找不到活的根 agent（将不带 agent 调用）' : liveRootAgent.session?.id ?? 'unknown'}`,
			);

			// 把线上形状翻成官方 `QuestionSpec`。
			//
			// ⚠️ `id` 必须是字符串且唯一：官方拿它当 React key，也拿它回传
			// `questionId`。电脑那边生成的是短 id，直接沿用即可。
			const spec = questions.map((q, index) => ({
				id: typeof q.id === 'string' && q.id !== '' ? q.id : `remote-${index}`,
				question: String(q.question ?? ''),
				...(typeof q.header === 'string' && q.header !== '' ? { header: q.header } : {}),
				...(q.multiSelect === true ? { multiSelect: true } : {}),
				options: (Array.isArray(q.options) ? q.options : []).map((o) => ({
					label: String(o.label ?? ''),
					...(typeof o.description === 'string' && o.description !== '' ? { description: o.description } : {}),
				})),
			}));

			const answer = await svc.ask({
				questions: spec,
				// ⚠️⚠️ **必须传 agent，而且必须是「当前存活的运行时根」。**
				//
				// 一开始我没传（以为不传就走根 waterfall 更宽松），结果是
				// **官方 UI 直接 `next()`、没人接、抛 `NO_PROVIDER`**。
				//
				// 依据 `dsh-client-ui-user-questions` 的 `answerQuestion()`：
				//
				//     const sessionId = ctx.sessions.scopeOf(owner);
				//     if (sessionId === undefined) return next();
				//
				// `owner` 就是派发时的 carrier（`this`）。不传 agent ⇒
				// `waterfall("user-questions/request", request, noAnswerer)`
				// 没有 carrier ⇒ `scopeOf` 返回 undefined ⇒ 官方放行 ⇒ 无人应答。
				//
				// 传了 agent 还要过 `assertLiveRoot()`：
				//     agents.get(agent.id) !== agent  → CALLER_NOT_LIVE
				//     !agents.roots().includes(agent) → DELEGATED_CALLER
				// 所以必须是 `ctx.agents` 里**真实的那个实例**，不能自己 new。
				//
				// 找不到活的根 agent（手机端这个会话没在跑）⇒ 放弃，
				// 让电脑端超时后走官方兜底 —— 至少不会卡死。
				...(liveRootAgent === undefined ? {} : { agent: liveRootAgent }),
			});
			trace(`官方 ask 返回: ${JSON.stringify(answer)?.slice(0, 200)}`);

			// 用户在手机上选完了 ⇒ 原样回传电脑。
			//
			// 官方返回 `{ answers: [{ questionId, selected: [...], custom? }] }`，
			// 与电脑端 answerer 期望的形状一致，不需要再转换。
			//
			// ⚠️ 用闭包里那份 `config`（`apply()` 顶部 `loadConfig()` 的产物），
			// 不重新读盘：配对会 `Object.assign` 就地改写它，读盘拿到的可能
			// 是**上一台电脑**的地址。
			const result = await callRemote(config, 'answerAsk', {
				requestId: payload.requestId,
				answer,
			});
			trace(`回传结果: ${JSON.stringify(result)?.slice(0, 120)}`);
			if (result?.ok !== true) {
				ctx.logger?.warn?.(
					`[dsh-remote-mobile] 答案没送回去（${String(result?.reason ?? 'unknown')}），电脑端会自行兜底`,
				);
			}
		} catch (error) {
			// 用户点了「跳过」/ 这一轮被中断 / 电脑端已超时 —— 都到这里。
			// 这**不是故障**，别往日志里写 error 级别，也别弹提示。
			trace(`远程 ask 结束/失败: ${String(error?.message ?? error)} code=${String(error?.code ?? '')}`);
			ctx.logger?.info?.(`[dsh-remote-mobile] 远程 ask 结束: ${String(error?.message ?? error)}`);
		}
	}

	/** 往所有浏览器订阅者推一条 SSE。 */
	const broadcast = (eventName, payload) => {		if (watchers.size === 0) return;
		const chunk = `event: ${eventName}\ndata: ${JSON.stringify(payload)}\n\n`;
		for (const res of watchers) {
			try {
				res.write(chunk);
			} catch {
				watchers.delete(res);
			}
		}
	};

	/**
	 * 跟随电脑的一个会话：durable 事件写手机本地，流式帧广播给浏览器。
	 *
	 * ⚠️ **未配对时直接不跟**（外部审计 I2）。
	 *
	 * `callRemote` 有「令牌为空 ⇒ 抛错」的守卫，但 `follow()` 原来没有：它会拿着
	 * **空令牌**去连 `config.endpoint`，然后套着退避重连循环一直重试下去。两个后果：
	 *
	 * - 未配对的状态下（首次装、解绑之后、用户手改 `mobile.json` 清空令牌），
	 *   后台持续打一个空令牌的请求 —— 那是**对着一台陌生服务器敲门**，
	 *   日志里刷满失败，还白耗手机流量与电。
	 * - 如果 `endpoint` 恰好指向某台仍在跑的电脑，那条空令牌请求会拿到 401，
	 *   但重连退避会一直重试（永不放弃），这类循环在移动端是电量和流量杀手。
	 *
	 * 调用方（`handleBridge` 的 `createSession` / 恢复流程）在未配对时本来就
	 * 不会走到这里 —— 这道守卫是**兜住所有调用路径**，不依赖调用方记得检查。
	 *
	 * @param sessionId - 两端同名的会话 id。
	 */
	const follow = (sessionId) => {
		if (config.token === EMPTY_TOKEN) {
			void debugLog(`follow(${sessionId}) 跳过：未配对`);
			return;
		}
		if (syncedSessions.has(sessionId)) {
			void debugLog(`follow(${sessionId}) 跳过：是同步会话（走 followSynced）`);
			return;
		}
		if (follows.has(sessionId)) {
			void debugLog(`follow(${sessionId}) 跳过：已有 follow 循环在跑`);
			return;
		}
		void debugLog(`follow(${sessionId}) 启动 SSE 循环`);
		const controller = new AbortController();
		follows.set(sessionId, controller);

		/**
		 * 连一次电脑的 SSE 并消费到断开。
		 *
		 * 断线是**常态**（Cloudflare 隧道会掐空闲连接、手机休眠会断网），所以
		 * 外面套一层退避重连：重连后电脑会补发全部历史，`mirrorEvent` 靠 seq
		 * 折算把已写过的跳过，不会重复。
		 */
		const once = async () => {
			// ⚠️⚠️ **令牌走 header，不走 URL query**（外部审计 M3）。
			// `?token=…` 会进 Cloudflare 隧道日志、任何反代/网关的 access log、
			// 以及浏览器历史（如果哪天这条被页面直接打开）。桌面端本来就支持
			// `x-dsh-remote-token` 头，所以这里换掉没有任何兼容代价 ——
			// 而这条是 node 发起的请求，**本来就不需要** query 形式
			//（query 形式只为浏览器 `EventSource` 保留，它设不了自定义头）。
			const url = `${config.endpoint}/api/dsh-remote/stream?session=${encodeURIComponent(sessionId)}`;
			// ⚠️⚠️ **必须带上 `fromSeq`，否则「聊得越大越卡」**（用户报障根因）。
			//
			// 电脑端原来的开场补发是**无条件全量重放**，而隧道掐空闲连接、手机
			// 休眠断网都是常态 —— 每次重连都推一遍整份日志。实测本机最大的会话
			// 明文有 155 MB / 29638 个事件，一次重连就全推过来。
			//
			// 这里把手机**已经收到的最大 seq** 报上去，电脑只补 `seq > fromSeq`。
			// 正常重连（没丢事件）时增量就是 0 条。
			//
			// ⚠️ 取 `session.seq`（= 日志长度）再减 1 才是「最后一条的 seq」：
			// `dsh-session/lib/index.js:1195` 的 `get seq() { return this.log.length }`。
			// 会话不存在 / 还没事件时退回 -1（= 从头补，等价于老行为）。
			let fromSeq = -1;
			try {
				const local = ctx.sessions.get(sessionId);
				if (local !== undefined && local.seq > 0) fromSeq = local.seq - 1;
			} catch {
				// 拿不到就按 -1 走全量，功能不受影响，只是这一轮流量大些。
			}
			const streamUrl = `${url}&fromSeq=${fromSeq}`;
			// ⚠️ 必须走 `remoteFetch`（自带 dispatcher）：全局 fetch 的 dispatcher
			// 在 Android 上连不通公网，见 `remoteFetch` 的注释。
			const res = await remoteFetch(streamUrl, {
				headers: { accept: 'text/event-stream', 'x-dsh-remote-token': config.token },
				signal: controller.signal,
			});
			if (!res.ok || res.body === null) throw new Error(`HTTP ${res.status}`);

			// ⚠️ **连上了就校正一次运行态**，因为断流期间的事件永远补不回来。
			//
			// 补发机制只补 **durable 事件**，而「这一轮已经结束了」这件事在断流
			// 期间可能只是一条 `turn/end`：重连后它会被补发、`deriveRunning`
			// 也能正确复位。真正的洞是**反过来**的场景 —— 手机以为在跑、
			// 电脑其实早跑完了，且 `turn/end` 恰好落在断流的窗口里丢了。
			//
			// 所以这里问一次电脑的真实状态。**代价是每次断流重连一次 RPC**，
			// 不是轮询：正常连着的时候一次都不发。
			//
			// 统一走 `reconcileRunning`（原来是内联实现）：两条路（`follow` 与
			// `followSynced`）必须用**同一张状态表**，否则一边校正了 `running`、
			// 另一边还拿着旧值去判重，会漏派发或重复派发。
			reconcileRunning(ctx, config, sessionId, () => !controller.signal.aborted);

			const decoder = new TextDecoder();
			let buffer = '';
			for await (const chunk of res.body) {
				buffer += decoder.decode(chunk, { stream: true });
				// ⚠️ **buffer 必须有上限**（外部审计 5.2b）：SSE 靠空行分隔消息，
				// 若对端一直不发空行（恶意电脑 / 中间人 / 隧道注入），
				// `buffer` 会无限增长直到手机 OOM。这是**输入侧**的 DoS，
				// 而这条流的对端是「公网上的那台电脑」—— 正是不可信的那一侧。
				//
				// 上限取 8 MiB：正常一帧（AI 正文分片）是几百字节到几 KB 量级，
				// 8 MiB 已经远超任何合法帧；超了说明对端不是在发 SSE。
				if (buffer.length > SSE_BUFFER_LIMIT) {
					throw new Error(`SSE 单条消息超过 ${Math.floor(SSE_BUFFER_LIMIT / (1024 * 1024))} MiB，已断开`);
				}
				// SSE 以空行分隔消息；末段可能不完整，留在 buffer 里。
				let split;
				while ((split = buffer.indexOf('\n\n')) >= 0) {
					const raw = buffer.slice(0, split);
					buffer = buffer.slice(split + 2);
					let eventName = 'message';
					let dataLine = '';
					for (const line of raw.split('\n')) {
						if (line.startsWith('event:')) eventName = line.slice(6).trim();
						else if (line.startsWith('data:')) dataLine += line.slice(5).trim();
					}
					if (dataLine === '') continue; // 心跳/注释行
					let payload;
					try {
						payload = JSON.parse(dataLine);
					} catch {
						continue;
					}
					if (eventName === 'event') {
						// ⚠️ `let` 不是 `const`：下面「会话不在内存」的分支会把它换成
						// `ensureLocalMirror()` 唤醒出来的那个实例。
						let session = ctx.sessions.get(sessionId);
						// ⚠️⚠️ **会话不在内存时，仍然要派发运行态（不能整条丢掉）。**
						//
						// 原来这里是 `if (session === undefined) continue;` ——
						// 一条 `continue` 把这条事件彻底扔了，连 `deriveRunning`
						// 都不执行。而**运行态根本不依赖本地会话**：它只是把
						// `turn/start` / `turn/end` 翻成一个布尔值广播出去
						//（见 `deriveRunning` 的注释）。
						//
						// 真机后果（用户报障）：
						//   「我就仅仅发了一句你好，然后它其实已经完成回复了，
						//    但是上面一直显示深度求索中，大概 30 多秒才结束」
						//
						// 链条：会话在跑的过程中被卸载/冷掉 ⇒ `sessions.get()`
						// 返回 undefined ⇒ `turn/end` 被这条 `continue` 扔掉 ⇒
						// 前端 `running` 一直是 true ⇒「深度求索中」不消失；
						// 直到几十秒后某次重连走了 `sessionStatus` 校正才复位。
						//
						// 所以把运行态派发提到判空**之前**：镜像写入需要 session
						//（没有就没法 append），但广播运行态不需要。
						if (session === undefined) {
							// ## ⚠️⚠️ 会话不在内存 ⇒ **先唤醒它，再写盘**（2026-10-22 修）
							//
							// 原来的写法是「不在内存就只派发运行态、`continue` 跳过写盘」。
							// 这个分支在真机上是**常态**，不是边缘情况：
							// App 重启后所有会话都是冷的，`ctx.sessions.get()` 一律
							// 返回 `undefined`。
							//
							// 真机实测（1.2.1 加诊断后抓到，session-4b46cd8a）：
							//
							//     event 丢弃（会话不在内存）type=user/message    seq=24
							//     event 丢弃（会话不在内存）type=assistant/message seq=25
							//     event 丢弃（会话不在内存）type=turn/end        seq=27
							//
							// 也就是说**电脑的回复完整地到了手机**，却被这一条 `continue`
							// 全部扔掉：不落盘、不广播，界面上什么都没有。用户看到的就是
							// 「手机没有任何回复，电脑上也没有对应的历史」。
							//
							// `ensureLocalMirror()` 就是为这件事写的（`followSynced`
							// 那条路一直在用它）：它会 `sessionController.create({sessionId})`
							// 把冷会话加载起来（官方 `create` 对已存在的 id 是幂等的）。
							// 这里同样用一次即可。
							//
							// ⚠️ 唤醒是**异步**的，而这条 SSE 读循环不能因此停摆太久
							//（同一连接上还有别的会话）。所以：
							//   · 先派发运行态（不依赖 session，用户立刻看到「深度求索中」）；
							//   · 再 `await` 唤醒，成功就照常走下面的写盘分支。
							// 唤醒失败（真的建不出来）才退化成只派发运行态。
							deriveRunning(ctx, sessionId, payload, reconcileStateOf(sessionId));
							let woken;
							try {
								woken = await ensureLocalMirror(sessionId);
							} catch (error) {
								void debugLog(
									`ensureLocalMirror 抛错 session=${sessionId} err=${String(error?.message ?? error)}`,
								);
								woken = undefined;
							}
							if (woken === undefined || woken === null) {
								void debugLog(
									`event 丢弃（唤醒失败）session=${sessionId} type=${payload?.type} seq=${payload?.seq}`,
								);
								continue;
							}
							void debugLog(
								`会话已唤醒 session=${sessionId} type=${payload?.type} seq=${payload?.seq} 本地seq=${woken.seq}`,
							);
							session = woken;
						}
						let wrote = false;
						try {
							wrote = mirrorEvent(session, payload, mirrorStateOf(sessionId));
						} catch (error) {
							// 诊断：同上，必须落盘（`ctx.logger.warn` 真机不可见）。
							void debugLog(
								`mirrorEvent 抛错 session=${sessionId} type=${payload?.type} seq=${payload?.seq} 本地seq=${session.seq} err=${String(error?.message ?? error)}`,
							);
							ctx.logger?.warn?.(
								`[dsh-remote-mobile] 写入事件失败 (${payload?.type}): ${String(error?.message ?? error)}`,
							);
							// ## ⚠️⚠️ 有缺口就**当场补齐**，不能干等重连（2026-10-22 修）
							//
							// 原来的做法是抛错 → 丢掉这条事件 → 等下一次 SSE 重连
							// 从 `fromSeq` 补发。但重连**只补发它之后的事件**，
							// 缺口本身**永远不会被填上**：
							//
							//   真机实测（session-f40a3c76，用户报「选任何模型都显示 1017」）：
							//     本地 seq 停在 28，电脑已经推到 29/30/31
							//     三条全是 `model/selection`
							//     ⇒ 模型选择**永远写不进手机** ⇒ 投影一直是旧模型
							//     ⇒ 前端 `current` 拿到旧的 provider/model
							//     ⇒ 官方 1017 标签一直挂在界面上，换哪个模型都不消失。
							//
							// 这不是「等一会儿就好」，是**永久卡死**：缺口在 28，
							// 而重连只会从 28 之后补，28 那条本身早就不在电脑的
							// SSE 缓冲里了（`pushBacklog` 只回溯有限条）。
							//
							// 所以这里用 `fillGap()` 主动把 `(本地seq-1, 电脑seq-1)`
							// 这段捞回来逐条写进去，再重试当前这条。`fillGap` 已经
							// 在 `followSynced` 那条路上验证过（首尾必须精确对齐）。
							if (typeof payload?.seq === 'number' && session.seq < payload.seq) {
								try {
									const state = mirrorStateOf(sessionId);
									const delta = typeof state.seqDelta === 'number' ? state.seqDelta : 0;
									// ## 区间怎么算（这里踩过一次，写清楚）
									//
									// 对齐关系是 `localSeq = desktopSeq + delta`，反过来
									// `desktopSeq = localSeq - delta`。
									//
									// `session.seq` 是**本地最后一条已写入**的 seq（官方语义：
									// 下一条要写的 seq 就等于 `session.seq`）。所以：
									//   · 本地「已追平」到的那条，对应电脑 `session.seq - delta`；
									//   · 我们要补的是它**之后**的那条开始，到当前这条之前。
									//
									// ⚠️ 原写法是 `wantFrom = session.seq - 1 - delta`，多退了一条
									// ⇒ `fillGap` 从更早的位置拉，首条折算后 ≤ 本地已写 ⇒ 被当成
									// 「已写过」跳过，真正缺的那条反而没补上，于是**每次都失败**。
									// 真机日志（session-f40a3c76，本地 31 / delta 3 / 电脑 29→折算 32）：
									//     补洞写入失败 seq=29 … 本地 seq=31
									//     补洞后仍失败 seq=32
									// 就是这条多退一算出来的。
									const wantFrom = session.seq - delta;
									const wantTo = payload.seq - 1;
									if (wantTo > wantFrom) {
										const filled = await fillGap(config, sessionId, wantFrom, wantTo);
										if (filled !== null && filled.length > 0) {
											let recovered = 0;
											for (const missing of filled) {
												try {
													mirrorEvent(session, missing, state);
													recovered += 1;
												} catch (inner) {
													void debugLog(
														`补洞写入失败 seq=${missing?.seq} type=${missing?.type} err=${String(inner?.message ?? inner)}`,
													);
													break;
												}
											}
											void debugLog(
												`补洞成功 session=${sessionId} 补了 ${recovered}/${filled.length} 条，本地seq=${session.seq}`,
											);
											// 补完再试当前这条。
											try {
												wrote = mirrorEvent(session, payload, state);
											} catch (retryError) {
												void debugLog(
													`补洞后仍失败 session=${sessionId} seq=${payload?.seq} err=${String(retryError?.message ?? retryError)}`,
												);
											}
										} else {
											void debugLog(
												`补洞拿不到数据 session=${sessionId} want(${wantFrom},${wantTo})`,
											);
										}
									}
								} catch (gapError) {
									void debugLog(
										`补洞抛错 session=${sessionId} err=${String(gapError?.message ?? gapError)}`,
									);
								}
							}
						}
						// ⚠️ 运行态跟**事件流**推进，不看 `wrote`。
						//
						// `wrote === false` 表示「这条已写过」（重连补发历史时全是 false），
						// 但那些 `turn/start` / `turn/end` 恰恰是重建运行态的依据：
						// App 重启后 `mirrorStates` 是空的，补发历史里的 turn 事件
						// 必须能把状态补出来，否则「显示已配对、界面却是死」的
						// 老问题会以新形态回来（按钮永不出现）。
						// 重复 emit 由 `deriveRunning` 里的去重挡住。
						//
						// `mirrored === false` 说明 `mirrorEvent` 抛了错（seq 有缺口），
						// 此时这条事件**不能写进本地日志**（会造成 seq 空洞）。
						//
						// ⚠️⚠️ **但运行态照样要派发。**
						//
						// 原来这里是 `if (mirrored) deriveRunning(...)` —— 把运行态
						// 和「能不能落盘」绑在一起了。而这两件事毫不相干：
						// `turn/end` 的语义是明确的（这一轮结束了），哪怕本地 seq
						// 对不上、这条事件写不进去，前端那个「深度求索中」也必须停。
						//
						// 真机后果正是用户报的那句「已经完成回复了，但一直显示
						// 深度求索中 30 多秒」：seq 一旦出现缺口，`mirrorEvent` 连续
						// 抛错，包括 `turn/end` 在内的所有事件都被拒收，运行态就
						// 卡在 true，直到某次重连的 `sessionStatus` 校正才复位。
						//
						// 重复 emit 由 `deriveRunning` 里的去重挡住，所以放宽是安全的。
						deriveRunning(ctx, sessionId, payload, reconcileStateOf(sessionId));
						// ⚠️ 必须告诉浏览器「这一轮的 durable 结算到了」。
						//
						// 浏览器侧的逐字动画是我们自己注入的 transient 行，而官方的
						// 清理点（`MutableSessionEventSource.settleAssistant`）只挂在
						// `ClientAssistantStream.acceptEventChange` 上 —— 那条链是**远程
						// 事件流**专用的，手机本地的 durable 是同步 append 进会话的，
						// 走不到它。没人清的话，逐字残片会永久留在窗口里，和随后到达的
						// 完整消息叠成两份。
						if (wrote && (payload?.type === 'assistant/message' || payload?.type === 'assistant/attempt')) {
							broadcast('settled', {
								sessionId,
								turn: payload?.data?.turn,
								step: payload?.data?.step,
							});
						}
					} else if (eventName === 'frame') {
						// 带上 sessionId：浏览器侧一条连接服务所有会话。
						broadcast('frame', { sessionId, frame: payload });
					} else if (eventName === 'ask') {
						// ══ 电脑端发起了 ask 提问 / 权限授权，转给浏览器弹卡片 ══
						//
						// ## 这条通道为什么必须存在（用户 2026-10-21 报）
						//
						// 「手机在远程访问电脑的时候，电脑这边工作发起了 ask 弹窗
						//   或者权限授权弹窗，手机那边都没有正常的弹出，导致用户
						//   无法选择任务卡住」
						//
						// ## 根因
						//
						// 这两类交互**不走会话事件流**：它们是 cordis waterfall
						// （`user-questions/request` / `approval/request`），只在
						// 进程内同步派发，不落盘、不进 `session/event`。所以原来
						// 这里只处理 `event` 和 `frame` 两种，`ask` 一条都到不了手机。
						//
						// 桌面端现在注册了 answerer 并把请求经这条 SSE 推过来
						//（见 dsh-remote-desktop 的 `askPhone`）。手机这里只负责
						// **转给浏览器渲染**，用户点完经 RPC `answerAsk` 回传。
						//
						// ⚠️ 不做任何本地状态缓存：卡片是一次性的，超时/中断由
						// 电脑端负责（它有 ASK_TIMEOUT_MS）。手机缓存反而会在
						// 重连后弹出一个已经作废的卡片。
						//
						// ══ 走**官方卡片**，不自己画 ══
						//
						// 手机本地引擎本来就带 `dsh-client-ui-user-questions`
						// 与 `dsh-client-ui-approval`，它们挂在本地 waterfall 上。
						// 只要这里调一次 `ctx.userQuestions.ask()`，官方就会把
						// 那张卡片原样渲染出来（A/B/C 选项、跳过/提交、多题翻页）。
						//
						// 这比自绘好在：观感与手机自己提问完全一致、不用维护一套
						// 平行 UI、官方以后改版自动跟随。
						//
						// ⚠️ 不 await：这条 SSE 读循环不能被用户的选择卡住 ——
						// 一旦卡住，同一连接上后续会话的 `event` / `frame` 全部
						// 停摆（表现为「弹了卡片之后整个远程界面不动了」）。
						// 所以丢给一个独立 promise，失败只记日志。
						void showAskViaOfficial(ctx, sessionId, payload);
					}
				}
			}
		};

		const run = async () => {
			let wait = 1000;
			while (!controller.signal.aborted) {
				const startedAt = Date.now();
				let lived = 0;
				try {
					await once();
					lived = Date.now() - startedAt;
					// 诊断：`once()` **正常返回**（不是抛错）说明连接被对端或自身
					// 干净地关掉了。真机上这是「SSE 连上就断」的唯一可见证据 ——
					// `ctx.logger.warn` 在真机不可见，只能落盘。
					void debugLog(`SSE once() 正常返回 session=${sessionId} 持续=${lived}ms 下次等待=${wait}ms`);
				} catch (error) {
					if (controller.signal.aborted) return;
					lived = Date.now() - startedAt;
					void debugLog(
						`SSE once() 抛错 session=${sessionId} 持续=${lived}ms 下次等待=${wait}ms err=${String(error?.message ?? error)}`,
					);
					ctx.logger?.warn?.(
						`[dsh-remote-mobile] 事件流中断(${wait}ms 后重连): ${String(error?.message ?? error)}`,
					);
				}
				if (controller.signal.aborted) return;
				// ## ⚠️⚠️ 退避必须**在连接成功存活过之后归零**（用户 2026-10-22 报障）
				//
				// 原来的写法是：
				//
				//     let wait = 1000;
				//     while (...) { await once(); await sleep(wait); wait = min(wait*2, 15000) }
				//
				// `wait` **只增不减**。于是只要历史上断过一次，之后每次断线都固定等
				// 15 秒 —— 即使这次连接本来很健康。真机实测桌面端日志是
				//
				//     SSE 订阅 ... 该会话订阅者=0     ← 每 16 秒一条
				//
				// 而 16s ≈ 15s 退避 + 1s 连接。那 15 秒的窗口里电脑推来的事件
				// （包括 `turn/end` 和 `assistant/message`）**全部丢失**，
				// 用户看到的就是「手机上永远没有回复」。
				//
				// 现在：连接活过 `STABLE_MS` 就认为它是健康的，退避归零回 1s；
				// 只有**快速失败**（连上就断）才继续指数退避 —— 这才是退避的本意。
				if (lived >= SSE_STABLE_MS) wait = 1000;
				else wait = Math.min(wait * 2, 15000);
				await new Promise((resolve) => setTimeout(resolve, wait));
			}
		};

		run().finally(() => {
			if (follows.get(sessionId) === controller) follows.delete(sessionId);
		});
	};

	/**
	 * 补洞：把 `(fromSeq, toSeq)` **之间**缺的事件从电脑捞回来。
	 *
	 * 为什么需要它：前端 `RemoteJournalStream.acceptEntry`
	 *（`dsh-api-gateway/lib/client.js:1127`）对每个事件要求
	 * `first === lastCursor + 1`。缺一条它就转 `replaceThrough()` →
	 * `assertPageThrough(page, requiredCursor)`，而那个断言要求补页的 tail
	 * **精确等于** cursor，做不到就抛
	 * `session event stream page did not end at its requested cursor`
	 * ⇒ 界面「历史加载失败」。
	 *
	 * 正常情况下不会走到这里（电脑端 SSE 已经保证从 `fromSeq` 之后连续推送），
	 * 这是最后一道防线：宁可多一次 RPC，也不让跳号事件进前端。
	 *
	 * @returns 补到的**有序事件数组**（已按 seq 去重、只保留 `(fromSeq, toSeq)`
	 *   区间内的）；补不齐（首条不是 `fromSeq + 1`、或末条不是 `toSeq`）返回
	 *   `null` —— 调用方据此断开重连，而不是硬把缺口放过去。
	 */
	async function fillGap(config, sessionId, fromSeq, toSeq) {
		// 缺口太大说明流已经彻底错乱（不是「丢了一两条」），补也没意义。
		if (toSeq - fromSeq > 200) return null;
		let page;
		try {
			page = await callRemote(config, 'readEventRange', { sessionId, fromSeq: fromSeq + 1, toSeq });
		} catch (error) {
			ctx.logger?.warn?.(
				`[dsh-remote-mobile] 补洞 ${sessionId} (${fromSeq}, ${toSeq}) 失败: ${String(error?.message ?? error)}`,
			);
			return null;
		}
		const events = page !== null && typeof page === 'object' && Array.isArray(page.events) ? page.events : null;
		if (events === null || events.length === 0) return null;
		const sorted = events
			.filter((event) => isWireEvent(event) && event.seq > fromSeq && event.seq <= toSeq)
			.sort((left, right) => left.seq - right.seq);
		if (sorted.length === 0) return null;
		// 必须从 `fromSeq + 1` 开始且到 `toSeq` 结束，中间不能有洞。
		if (sorted[0].seq !== fromSeq + 1) return null;
		if (sorted[sorted.length - 1].seq !== toSeq) return null;
		for (let index = 1; index < sorted.length; index += 1) {
			if (sorted[index].seq !== sorted[index - 1].seq + 1) return null;
		}
		return sorted;
	}

	/**
	 * 「同步来的对话」专用的实时流：转发 + **顺带在本地补一份日志**。
	 *
	 * ## 为什么要落盘（用户 2026-10-21 报障根因）
	 *
	 * 用户原话：「底部的 token 信息以及上下文信息，都没有显示了」。
	 *
	 * 原来这里**只转发、不落盘**，理由是「同步来的对话本地一份都没有，它的
	 * seq 体系完全属于电脑，落盘必然错位」。转发这条路本身是对的（界面能正常
	 * 显示消息、能继续对话），但它把**两个投影**饿死了：
	 *
	 *   · `tokenUsage`      —— `UsagePill` 的唯一数据源
	 *   · `contextPressure` —— 上下文环（`12%`）的唯一数据源
	 *
	 * 依据 `dsh-client-ui-chat/lib/client.js`：
	 *
	 *     const usage = useProjection("tokenUsage");
	 *     if (usage === void 0 || billedInputTokens(usage) === 0 && usage.outputTokens === 0)
	 *       return null;                      // ← 整块不渲染，不是显示 0
	 *
	 * 而这两个投影都是**对本地会话日志做纯 fold** 算出来的（见
	 * `dsh-session-stats` 的 `sessionStatsProjectionDefinition`）。本地日志
	 * 一条都没有 ⇒ 投影是 `undefined` ⇒ 药丸 `return null` ⇒ 底部整行消失。
	 *
	 * ## 为什么现在敢落盘
	 *
	 * 关键在于**同步时会先 `sessionController.create({ sessionId })`**
	 *（见 `syncConversations`），本地会建出一个带 `session/end-seed` 的空会话。
	 * 于是：
	 *
	 *   · 本地 `session.seq` 从 0 起，电脑端 seq 从 1 起；
	 *   · `mirrorEvent` 里那段 offset 折算（`skippedSeeds`）正好把两者对齐 ——
	 *     它现算 `session.log` 里有没有 `session/end-seed`，有就减 1。
	 *
	 * 所以「错位」这个担心在 `mirrorEvent` 的对齐检查面前已经不成立了：对齐
	 * 不上它会**抛错并拒收**（不会写脏数据），我们捕获后照常转发即可 ——
	 * 也就是退化成改动前的行为，不会比原来更差。
	 *
	 * ## 为什么落盘失败不能影响转发
	 *
	 * 转发是主功能（用户看得见消息），落盘只是给投影补数据。所以整段包在
	 * try/catch 里：写不进去就只记一条诊断，事件**照常** yield 给前端。
	 *
	 * @param sessionId - 会话 id（电脑与手机共用同一个 id）。
	 * @param fromSeq - 已经交给前端的最后一条的电脑 seq；之后只要更新的。
	 * @param signal - 前端取消（关掉会话 / 切走）时中止。
	 */
	async function* followSynced(sessionId, fromSeq, signal) {
		// 诊断：确认这条流真的被建起来了（排查「同步会话没有运行态」第一步）。
		try {
			runningDiag.push({ at: Date.now(), sid: sessionId, via: 'followSynced-enter', fromSeq });
			if (runningDiag.length > 20) runningDiag.shift();
		} catch {
			/* 诊断失败不影响主流程 */
		}
		let wait = 1000;
		let cursor = fromSeq;
		// snapshot 里给的基线是 `assistantStream: {revision: 0}`，
		// 所以第一帧 assistant-stream 的 revision 从 1 开始（见下面的自增处）。
		let assistantRevision = 0;
		while (!signal?.aborted) {
			const controller = new AbortController();
			const onAbort = () => controller.abort();
			signal?.addEventListener?.('abort', onAbort);
			try {
				const streamUrl = `${config.endpoint}/api/dsh-remote/stream?session=${encodeURIComponent(
					sessionId,
				)}&fromSeq=${cursor}`;
				const res = await remoteFetch(streamUrl, {
					headers: { accept: 'text/event-stream', 'x-dsh-remote-token': config.token },
					signal: controller.signal,
				});
				if (!res.ok || res.body === null) throw new Error(`HTTP ${res.status}`);
				wait = 1000;

				// ⚠️ **连上了就校正一次运行态**（用户 2026-10-20 报障根因）。
				//
				// 「退出 App 再回来就看不到『深度求索中』、也不能暂停」的真正原因，
				// 是运行态只能靠 `turn/start` / `turn/end` 事件翻布尔值，而 App
				// 重启后 `mirrorStates` 是空的、那一轮的 `turn/start` 早已发过、
				// 重连的 `fromSeq` 又不会让电脑重发它 —— 手机永远等不到那个开关。
				//
				// 完整论证见 `reconcileRunning` 的注释。这里只负责在**每次建流**
				// 时问一次电脑的真实状态（一问一答，不是轮询）。
				//
				// ⚠️ 传 `isLive`：流断掉之后这次校正的结果已经过期，
				// 不能拿旧结果去覆盖新状态。
				reconcileRunning(ctx, config, sessionId, () => !controller.signal.aborted);

				const decoder = new TextDecoder();
				let buffer = '';
				for await (const chunk of res.body) {
					buffer += decoder.decode(chunk, { stream: true });
					if (buffer.length > SSE_BUFFER_LIMIT) {
						throw new Error(`SSE 单条消息超过 ${Math.floor(SSE_BUFFER_LIMIT / (1024 * 1024))} MiB，已断开`);
					}
					let split;
					while ((split = buffer.indexOf('\n\n')) >= 0) {
						const raw = buffer.slice(0, split);
						buffer = buffer.slice(split + 2);
						let eventName = 'message';
						let dataLine = '';
						for (const line of raw.split('\n')) {
							if (line.startsWith('event:')) eventName = line.slice(6).trim();
							else if (line.startsWith('data:')) dataLine += line.slice(5).trim();
						}
						if (dataLine === '' || (eventName !== 'event' && eventName !== 'frame')) continue;
						let payload;
						try {
							payload = JSON.parse(dataLine);
						} catch {
							continue;
						}
						if (eventName === 'frame') {
							// ⚠️ **`revision` 必须由我们自己递增。**
							//
							// 电脑端发的是原始的 `agent/assistant-stream` 帧
							//（`dsh-remote-desktop/lib/index.js` 的
							// `push(agent.session.id, 'frame', frame)`），里面**没有**
							// `revision` 字段 —— 那个字段是官方宿主在
							// `SessionHistoryController.follow` 里用
							// `++assistantStreamOrdinal` 现加的。
							//
							// 而前端 `SessionEventStream.follow`
							//（`dsh-api-session-controller/lib/client.js:410`）要求
							// `frame.frame.revision === 上一帧 + 1`，否则抛
							// `session assistant stream skipped revision N`，
							// **整条流当场终止**。
							//
							// 所以这里按到达顺序自增。基线是 snapshot 里的
							// `assistantStream: {revision: 0}`，所以第一帧从 1 开始。
							assistantRevision += 1;
							yield { type: 'assistant-stream', frame: { ...payload, revision: assistantRevision } };
							continue;
						}
						// ⚠️ 只认合法的事件信封：前端 `assertSessionWireEvent`
						//（`dsh-api-session-controller/lib/client.js:290`）会逐字段校验，
						// **一条坏事件就让整条流抛错**（不是跳过那一条），
						// 界面当场变成错误态。所以这里提前把不合格的丢掉。
						if (!isWireEvent(payload)) continue;
						if (payload.seq <= cursor) continue; // 重连补发的重复段
						// ⚠️⚠️ **跳号必须先补洞，不能直接交给前端。**
						//
						// 前端 `RemoteJournalStream.acceptEntry`
						//（`dsh-api-gateway/lib/client.js:1127`）对每个事件要求
						// `first === lastCursor + 1`（`follows()` 是精确 +1）。只要
						// 中间缺一条，它就转 `replaceThrough()` →
						// `assertPageThrough(page, requiredCursor)`，那个断言要求补页的
						// **tail 精确等于 cursor**，做不到就抛
						// `session event stream page did not end at its requested cursor`
						// ⇒ 界面「历史加载失败」。
						//
						// 电脑端已经把「冷会话也补齐」修掉了（那是主因），但隧道中断、
						// 电脑侧推送丢失仍可能造成跳号。这里做最后一道防线：缺多少就
						// 用 `readHistory` 把 `(cursor, payload.seq)` 之间那一段捞回来，
						// 逐条先发。捞不回来就**整条流重开**（断开连接，让外层退避重连
						// 并从新的 cursor 重新对齐），绝不把跳号事件放给前端。
						if (payload.seq !== cursor + 1) {
							const filled = await fillGap(config, sessionId, cursor, payload.seq);
							if (filled === null) {
								// 补不齐 ⇒ 断开重连（外层会从 `cursor` 重新拉流）。
								throw new Error(
									`事件跳号且无法补齐（本地 ${cursor} → 收到 ${payload.seq}），重连对齐`,
								);
							}
							for (const event of filled) {
								if (!isWireEvent(event)) continue;
								if (event.seq <= cursor) continue;
								cursor = event.seq;
								// 补洞捞回来的那一段里也可能有 turn/start / turn/end，
								// 同样要派发运行态（理由见下面正式 yield 处的注释）。
								deriveRunning(ctx, sessionId, event, reconcileStateOf(sessionId));
								yield { type: 'event', event };
							}
							// 补完之后如果还是接不上，同样断开重连。
							if (payload.seq !== cursor + 1) {
								throw new Error(`补齐后仍不连续（本地 ${cursor} → 收到 ${payload.seq}），重连对齐`);
							}
						}
						cursor = payload.seq;
						// 顺带在本地补一份日志 —— 这是 `tokenUsage` / `contextPressure`
						// 两个投影的唯一数据源（见函数头注释）。
						//
						// ⚠️ 包在 try/catch 里、**绝不能影响转发**：对齐不上时
						// `mirrorEvent` 会抛错拒收，那只是「这条没写进本地」，
						// 前端照样要拿到它。
						try {
							const local = await ensureLocalMirror(sessionId);
							if (local !== undefined) {
								mirrorEvent(local, payload, mirrorStateOf(sessionId));
							}
						} catch (error) {
							// 只在真出问题时记一次，别刷屏（对齐失败是常态：
							// 同步会话的本地日志可能落后很多条）。
							if (mirrorWarnedAt.get(sessionId) !== true) {
								mirrorWarnedAt.set(sessionId, true);
								ctx.logger?.info?.(
									`[dsh-remote-mobile] 同步会话本地落盘跳过（不影响显示）: ${String(error?.message ?? error)}`,
								);
							}
						}
						// ⚠️⚠️ **同步会话也必须派发运行态**（用户报障根因）。
						//
						// 用户原话：
						//   「手机的画面不仅不能点击暂停，而且它没有深度求索中用时
						//    多少分多少秒的那个状态显示，如果模型调用了后台任务，
						//    也没有显示了」
						//
						// 三个症状是同一个根值：**这条 `followSynced` 从来不调
						// `deriveRunning`**。
						//
						// `follow()`（非同步会话那条路）在 `:2067` 有这一句，
						// 但同步来的会话在 `:1936` 就被提前 return 了 —— 它的实时
						// 事件全部走本函数、直接 yield 给前端，不落本地日志，
						// 于是没有任何人把 `turn/start` / `turn/end` 翻成运行态。
						//
						// 而运行态是**独立于日志**的一条通道（`api-session/status`
						// 事件，见 `deriveRunning` 的注释），所以这里不落盘也完全
						// 可以、也应该派发。前端拿到后才会出现：
						//   - 「深度求索中 用时 N 秒」；
						//   - 输入框的「停止生成」按钮；
						//   - 顶部「N 个后台任务运行中」。
						deriveRunning(ctx, sessionId, payload, reconcileStateOf(sessionId));
						yield { type: 'event', event: payload };
					}
				}
			} catch (error) {
				if (signal?.aborted) return;
				ctx.logger?.warn?.(
					`[dsh-remote-mobile] 同步会话实时流中断(${wait}ms 后重连): ${String(error?.message ?? error)}`,
				);
			} finally {
				signal?.removeEventListener?.('abort', onAbort);
			}
			if (signal?.aborted) return;
			await new Promise((resolve) => setTimeout(resolve, wait));
			wait = Math.min(wait * 2, 15000);
		}
	}

	// ---- 拦截官方 create：备选区选中「电脑目录」时自动绑成远程会话 ----
	//
	// ⚠️ **这是「第二次起就变本地」的根因修复，不是锦上添花。**
	//
	// 用户第一次是在「电脑文件夹」对话框里选的目录，走 `createSession` 桥 ——
	// 那条路会把 sessionId 记进 `remoteSessions`，prompt 因此被转发电脑。
	//
	// 但第二次他是**在新建对话首页的备选区里**点上次那个工作区。官方前端此时
	// 直接 `sessionController.create({workspaceId})`，**完全不经过本插件**，
	// 于是 `remoteSessions` 里没有这个 sessionId ⇒ prompt 拦截不命中 ⇒
	// 消息交给手机本地 agent（真跑手机模型、cwd 是手机路径）。
	// 用户看到的就是「问他工作目录，答的是安卓目录」。
	//
	// 这里在同一个路口把它认出来并补上绑定：镜像目录 → 电脑路径 → 让电脑建同名会话。
	ctx.effect(() => {
		const controller = ctx.sessionController;
		if (controller === undefined || typeof controller.create !== 'function') return;
		const originalCreate = controller.create.bind(controller);
		controller.create = async (request) => {
			const result = await originalCreate(request);
			const sessionId = result?.sessionId;
			if (typeof sessionId !== 'string' || sessionId === '') return result;
			// 已经绑过的（走对话框那条路）不用再绑。
			if (remoteSessions.has(sessionId)) return result;
			// 解析这个会话的 cwd：优先 `workspaceId`（备选区走的就是这条），
			// 退回请求里的 `cwd`（直接给路径的情形）。
			let dir = request?.cwd;
			if (request?.workspaceId !== undefined) {
				const workspace = ctx.get?.('workspaceRegistry')?.get?.(request.workspaceId);
				if (workspace?.path !== undefined) dir = workspace.path;
			}
			const desktopPath = desktopPathOfMirror(workspaceMap, dir);
			if (desktopPath === undefined) return result;
			// 没配对时**不能**绑：绑了之后 prompt 会去打一个不存在的电脑，
			// 用户看到的是「发消息就报错」，比退回本地会话更难理解。
			if (config.token === EMPTY_TOKEN) {
				ctx.logger?.warn?.(
					`[dsh-remote-mobile] 会话 ${sessionId} 选的是电脑目录「${desktopPath}」，但还没配对，先当本地会话`,
				);
				return result;
			}
			// ⚠️ **这里不再联网建会话**（用户三轮反馈后的定稿）。
			//
			// 原来这里 `await callRemote('createSession')` —— 那是「选目录」和
			// 「建对话」被绑在一起留下的痕迹。官方在电脑上也是**发了消息才 create**，
			// 选工作区只是面板上的一个状态。
			//
			// 现在只登记一份「待发送计划」，等用户在 `prompt` 里真的发消息时，
			// 把 `desktopPath` / `presetId` / `model` 打包进**同一个** prompt 请求
			// 发过去（见 `prompt` 拦截器）。好处是双重的：
			//
			// 1. 选目录这一步**零延迟**（纯内存登记，不等隧道）；
			// 2. 从「选目录 → 发第一条消息」全程只跟电脑沟通**一次**。
			//
			// 而且失败面变好了：原来电脑离线要等到选目录之后才知道，界面上已经
			// 收了选择器、没地方提示；现在电脑离线是在**发送那一刻**报错，
			// 用户正看着输入框，能正常看到「发不出去」。
			//
			// `presetId` 取自请求（官方 create 支持 `agentPreset`）；模型不在这里
			// —— 用户可能在选完工作区**之后**才改模型，由 `selectModel` 拦截器
			// 往计划里补写（见下面 ②）。
			pendingPlans.set(sessionId, {
				desktopPath,
				...(typeof request?.agentPreset === 'string' && request.agentPreset !== ''
					? { presetId: request.agentPreset }
					: {}),
			});
			remoteSessions.add(sessionId);
			// ⚠️ **必须落盘电脑路径**：`pendingPlans` 是内存 Map，App 重启后清空，
			// 那时再发消息就只能靠这份落盘映射反解（见 `loadRemotePaths` 的注释）。
			remotePaths[sessionId] = desktopPath;
			saveRemoteSessions(remoteSessions, ctx.logger, syncedSessions, remotePaths, desktopTitles, turnOffsets, remoteModels);
			// ⚠️ **事件流先不开**：会话在电脑上还不存在，此时连 SSE 只是挂一条
			// 空连接，白白占着隧道的并发。`prompt` 里发完消息才 `follow()`。
			ctx.logger?.info?.(
				`[dsh-remote-mobile] 会话 ${sessionId} 的工作区是电脑目录「${desktopPath}」，等第一条消息时再在电脑上建会话`,
			);
			return result;
		};
		return () => {
			controller.create = originalCreate;
		};
	}, 'dsh-remote-mobile: create 拦截（备选区选电脑目录）');

	// ---- 拦截官方 prompt：远程会话转发电脑，本地会话走原逻辑 ----
	ctx.effect(() => {
		const controller = ctx.sessionController;
		if (controller === undefined) return;
		const originalPrompt = controller.prompt.bind(controller);
		// ⚠️ **签名必须带上第二个参数 `signal`**（`(request, signal)`）。
		//
		// 官方 `dsh-api-session-controller/lib/index.js:3096` 是
		// `prompt(request, signal) { signal.throwIfAborted(); ... }` —— 它**无条件**
		// 对 `signal` 调 `throwIfAborted()`。我们的包装若只声明 `(request)`，
		// 转发给 `originalPrompt(request)` 时 `signal` 就是 `undefined`，
		// 于是发消息必报：
		//   `Cannot read properties of undefined (reading 'throwIfAborted')`
		// 症状是**消息根本发不出去**，而报错文案完全看不出是参数漏传。
		controller.prompt = async (request, signal) => {
			const hit = remoteSessions.has(request.sessionId);
			// 诊断：环形记最近 20 条，前端用 `debugPrompt` 桥读出来。
			// ⚠️ 宿主是 Node 进程，没有 `window`，不能往页面塞。
			promptDiag.push({
				at: Date.now(),
				sid: request.sessionId,
				hit,
				local: ctx.sessions.get(request.sessionId) !== undefined,
				plan: planOf(request.sessionId) === undefined ? 'none' : 'yes',
				// 计划丢了时能不能反解出电脑路径 —— 排查「重启后发不出去」就看这一项。
				mirror: desktopPathOfMirror(workspaceMap, ctx.sessions.get(request.sessionId)?.header?.cwd) ?? null,
			});
			if (promptDiag.length > 20) promptDiag.shift();
			if (!hit) return originalPrompt(request, signal);
			// ⚠️ **必须把附件一起转过去**，不能只挑 `text` part。
			//
			// 用户报的就是这个：「照片就会被吞了 AI 只能看到我的文字」。前端的
			// `serializeAttachments()`（`dsh-client-ui-conversation/lib/client.js:3299`）
			// 给图片发的是 `{type:'image', mediaType, data(base64)}` —— **字节是内联的**，
			// 直接透传，电脑会用自己的 `attachments.admitPromptContent` 落盘。
			//
			// `{type:'file', receiptId}` 不转（receipt 属于手机的 `fileUploads`，
			// 电脑解不开），由电脑端降级成一句说明（见其 `prompt` 分支的注释）。
			const content = Array.isArray(request.content) ? request.content : [];
			const text = content
				.filter((part) => part.type === 'text')
				.map((part) => part.text)
				.join('\n');
			if (ctx.sessions.get(request.sessionId) === undefined) {
				// ⚠️⚠️ **不要在这里抛错**（这是「退出 App 回来发不了消息」的根因）。
				//
				// 原来这句是 `throw new Error('远程会话 "…" 不在手机本地')`。
				// 它的意图是「本地没有这个会话就没法反解电脑路径」，但那个前提
				// 在 App 重启后**必然成立**：`ctx.sessions.get()` 只返回活着的
				// 会话，而重启后本地日志虽在磁盘上却没被载入内存。
				//
				// 真机报障（用户原话）：
				//   「我退出 APP 之后，后面再回来就发不了消息了」
				// 界面红条：
				//   远程会话 "session-c0d12a80-…" 不在手机本地 (gateway/internal)
				//
				// 而这条消息**根本不需要**本地会话存在：要发给电脑的只有
				// `sessionId` + 正文 + `desktopPath`，手机本地那份日志只是镜像。
				// 所以这里改成「尽力反解路径，解不出才报错」，判据挪到下面
				// `fallbackDesktopPath` 那里统一处理。
				ctx.logger?.info?.(
					`[dsh-remote-mobile] 远程会话 ${request.sessionId} 不在手机本地内存里（多半是 App 重启过），改用落盘路径继续转发`,
				);
			}
			// ⚠️ 手机端**不**自己写 user/message。
			//
			// 手机本地日志是电脑日志的镜像，seq 必须逐条对齐；本地多写一条会让
			// 后续所有事件的 seq 错位，事件搬运随即失效。而电脑端 prompt 后
			// 本来就会 append 一条 user/message 并流回来 —— 那才是唯一真源。
			//
			// 把前端的 requestId 原样带过去：电脑端会把它写进
			// `source.rpcId`，前端凭这个结算「提交中」状态并显示用户气泡。
			//
			// ⚠️⚠️ **`requestId` 是这条调用在跨通道重试下的幂等键，必须总是有**
			// （外部审计 L6）。原来写的是
			// `...(request.requestId === undefined ? {} : { requestId: request.requestId })`
			// —— 前端偶尔不带（某些调用路径、旧缓存的前端包）时，这里就**没有幂等键**，
			// 而 `remoteFetch` 在第一条通道失败后会**原样重发**这个 body。
			// 后果是用户在电脑上**看到同一条消息发了两次**，AI 也就跑了两轮。
			//
			// 所以缺省时在这里补一个：用 `randomUUID()` 而不是「前端那个可能重复的
			// 时间戳」，因为它必须每次都是新的（否则去重逻辑会把正常消息当成重复的）。
			// ⚠️ 传了就用传的 —— 前端的 requestId 是它结算 UI 的依据，不能换。
			//
			// ## 为什么这里可能带着「建会话」的全部信息（用户原话）
			//
			// 「我觉得新建对话、选择预设、选择模型还有发送消息这么多个步骤，你可以
			//  打包成一个网络请求发过去就可以了，电脑拿到这个请求之后再解析就OK了」
			//
			// `plan` 存在 ⇒ 这条会话在电脑上**还没建出来**（见 `pendingPlans` 的注释）。
			// 此时把 `desktopPath` / `presetId` / `model` 一并塞进**同一个** `prompt`
			// 请求，电脑端自己按 create → selectModel → prompt 的顺序执行。
			//
			// 这样「选目录 → 发第一条消息」全程只过一趟隧道：不是三次 RPC，是一次。
			// 这正是用户要的「全程仅需要从电脑那边沟通一次」。
			//
			// `plan === undefined` ⇒ 老路径（会话早就在电脑上建好了），body 与改动前
			// **逐字一致**，不带任何多余字段。
			const plan = planOf(request.sessionId);
			const body = {
				sessionId: request.sessionId,
				text,
				// 附件形态一并带上（没有附件时给空数组，电脑端会退回纯文本）。
				content,
				requestId:
					typeof request.requestId === 'string' && request.requestId !== ''
						? request.requestId
						: randomUUID(),
			};
			// ## 计划丢了也要能发（这是「原来能发、重启后不能」的根因）
			//
			// `pendingPlans` 是**内存** Map（见其注释：故意不落盘），所以手机 App
			// 重启后必然清空。此时用户点进那个会话继续发消息，`plan` 是
			// `undefined` ⇒ 老路径 ⇒ body 不带 `desktopPath` ⇒ 电脑端
			// `sessions.get()` 没有它会话，官方命令层直接抛
			// `session "…" not found`，用户看到红条、消息发不出去。
			//
			// 而 `desktopPath` **本来就不需要额外存**：手机本地会话的 cwd 就是那个
			// 镜像目录，目录名（slug）就是电脑路径的可逆编码，`desktopPathOfMirror()`
			// 能原样解回来。
			//
			// ⚠️ 只在**电脑上确认没有这个会话**时才该带。可这里拿不到那个事实
			//（那正是要问电脑的东西），所以**总是带**——电脑端对已存在的会话
			// 有幂等保护（`sessions.get()` 有值就跳过 create/selectModel），
			// 所以重复带一次只是多几十字节，不会推翻已有历史。
			//
			// ⚠️ 顺带保住「预设/模型」：计划没了就读不到了，只能靠电脑端那次
			// 幂等跳过；也就是说重启后第一句话用的是电脑上的既有设置 ——
			// 这与用户预期一致（电脑才是真源），不需要额外补救。
			// ## 反解电脑路径：**落盘的那一份优先**
			//
			// 三个来源，按可靠性排序：
			//   ① `remotePaths[sessionId]` —— 同步/建会话时落盘的，**不依赖会话活着**；
			//   ② `ctx.sessions.get(id)?.header?.cwd` —— 会话活着时才有；
			//   ③ 都没有 ⇒ 这条消息发不出去（下面统一报错）。
			//
			// ⚠️ ① 必须优先：App 重启后 ② 恒为 `undefined`，只有 ① 还在。
			const persistedPath = typeof remotePaths[request.sessionId] === 'string' && remotePaths[request.sessionId] !== ''
				? remotePaths[request.sessionId]
				: undefined;
			const fallbackDesktopPath = plan !== undefined
				? undefined
				: persistedPath !== undefined
					? persistedPath
					: desktopPathOfMirror(workspaceMap, ctx.sessions.get(request.sessionId)?.header?.cwd);
			if (plan !== undefined) {
				body.desktopPath = plan.desktopPath;
				// 预设：以**手机本地会话投影**上的值为准（用户可能在选完工作区、
				// 甚至在建完会话之后才改预设，那时 `plan.presetId` 是旧的）。
				// 投影读不到才退回创建时记下的那个。
				const presetId = presetIdOf(ctx, request.sessionId) ?? plan.presetId;
				if (typeof presetId === 'string' && presetId !== '') body.presetId = presetId;
				if (plan.model !== undefined && typeof plan.model.provider === 'string') {
					body.model = plan.model;
				}
			} else if (typeof fallbackDesktopPath === 'string' && fallbackDesktopPath !== '') {
				// 计划丢了（多半是 App 重启过），用落盘路径 / 镜像目录反解补上。
				body.desktopPath = fallbackDesktopPath;
			} else {
				// 三个来源全空 ⇒ 真的没法转发。这是**唯一**该在这里报错的情形。
				//
				// 原来这个判据是「本地会话存不存在」，而重启后它必然为假 ——
				// 于是用户在 App 重启后**每条消息**都撞这条红条。现在判据换成
				// 「电脑路径解不解得出」，它才真正等价于「这条消息发不出去」。
				//
				// 正常情况走不到这里：`remotePaths` 在同步/建会话时一定写了。
				// 只有「装了旧版本、那时还没落盘 paths」的会话会命中，
				// 重新同步一次即可恢复。
				throw new Error(
					`远程会话 "${request.sessionId}" 的电脑路径未知，无法转发（请在侧边栏重新「同步对话」一次）`,
				);
			}
			// ## ⚠️⚠️ 模型：`plan` 里没有就**读落盘的兜底**（用户报障的直接修复）
			//
			// 用户原话：「我在手机选的是 wk 供应商的 DeepSeek V4.1 flash，然而电脑
			// 发起请求的却是 GLM 5.3 flash，我的 API 供应商没有这个模型，报错了。」
			//
			// 原来 `body.model` **只在 `plan !== undefined` 时**才赋值。而
			// `pendingPlans` 是内存 Map，App 重启 / 被系统杀后台就空了 ⇒
			// 走 else 分支 ⇒ **body 完全没有 model 字段** ⇒ 电脑端
			// `params.model` 是 undefined ⇒ 跳过 `selectModel` ⇒ 电脑用**它自己的
			// 默认模型**去请求。用户的供应商里没有那个默认模型 ⇒ 502。
			//
			// 现在：`plan.model` 优先（同一次会话里的最新选择），没有就读
			// `remoteModels`（落盘，见 `loadRemoteModels`）。只要用户在手机上选过
			// 一次，重启后也照样把正确的模型带过去。
			//
			// ⚠️ 顺序不能反：`plan.model` 是「用户刚选的」，比磁盘上的新。
			if (body.model === undefined) {
				const persistedModel = remoteModels[request.sessionId];
				if (persistedModel !== undefined && typeof persistedModel.provider === 'string') {
					body.model = persistedModel;
					void debugLog(
						`prompt 用落盘模型 session=${request.sessionId} ${persistedModel.provider}/${persistedModel.model}`,
					);
				}
			}
			// ⚠️⚠️ **不 await 转发** —— 这是「像手机本地一样零延迟」的关键。
			//
			// 原来的写法是 `await callRemote(config, 'prompt', body)`。
			// 手机到电脑要走 Cloudflare 隧道，实测一次往返 **700–1900ms**
			//（取决于网络），而 `controller.prompt` 是前端**同步等待**的：
			// 官方前端拿到 `{accepted:true}` 才把用户这条消息渲染上屏。
			// 于是用户每次按发送，都要先干等这 0.7–1.9 秒，界面才有反应 ——
			// 本地会话是 0ms（同步 append），远程会话却是「按下去没动静」。
			// 这正是用户要的那句「在操作电脑群的时候，也能像手机本地一样零延迟无感」。
			//
			// 现在改成**先返回、再转发**：
			// · 前端立刻收到 `{accepted:true}` ⇒ 消息瞬间上屏、输入框立刻清空；
			// · 转发在后台继续跑，成功就 `follow()` 接上事件流（电脑的回复照常流回来）；
			// · 失败则通过 SSE 的 `prompt-failed` 通道通知前端弹错误，并把
			//   那一条标记成发送失败 —— 用户能看见、能重发，不会「以为发出去了」。
			//
			// 为什么这样做是安全的：`prompt` 的返回值只表示「受理了」，
			// 不携带任何前端需要的业务数据（唯一字段就是 `accepted`）。
			// 真正的回复内容全部走事件流，与这条返回值无关。
			const forward = async () => {
				await callRemote(config, 'prompt', body);
				// ⚠️ **发成功才清计划**：`callRemote` 抛错时留着，用户重发下一句还能
				// 用上同样的预设/模型 —— 否则一次网络抖动就让这次选择永久丢失，
				// 用户看到的是「我选的模型没生效」。
				//
				// 电脑端这一步是幂等的（会话已存在就跳过 create），所以重复带一次
				// 不会重复建会话，最多多几十字节。
				if (plan !== undefined) pendingPlans.delete(request.sessionId);
				// 确保事件流在线（断线时重连，补齐这段时间的事件）。
				//
				// ⚠️ **放在 `callRemote` 成功之后、且不放在 catch 里**：失败时说明电脑
				// 没受理这条消息，此时连上去也没有事件可收；而用户一重发就会走到这里。
				follow(request.sessionId);
			};
			forward().catch((error) => {
				// 转发失败：前端已经把这句消息画上屏了，必须让它知道失败了，
				// 否则用户会一直等一个永远不会来的回复。
				ctx.logger?.warn?.(
					`[dsh-remote-mobile] 转发消息失败(${request.sessionId}): ${String(
						error?.message ?? error,
					)}`,
				);
				broadcast('prompt-failed', {
					sessionId: request.sessionId,
					requestId: body.requestId,
					message: String(error?.message ?? error),
				});
			});
			return { accepted: true };
		};

		// 「停止生成」同样要转发到电脑。
		// ⚠️ 不拦的话，官方 `cancel` 会 `resolveAgent()` → 在手机上 resume 一个 agent，
		// 那会真去建目录、真跑手机本地模型。
		const originalCancel = controller.cancel.bind(controller);
		controller.cancel = async (request) => {
			if (!remoteSessions.has(request.sessionId)) return originalCancel(request);
			await callRemote(config, 'cancel', { sessionId: request.sessionId });
			// ⚠️ **停完要立刻把运行态复位**，不能干等 `turn/end`。
			//
			// 这是「点了停止、按钮却还停在停止」的那条路径：`turn/end` 走的是
			// 上行事件流，中途断一下（隧道掐空闲连接、手机休眠）就到不了手机，
			// 于是 `running` 永远为 true，输入框卡在「停止生成」出不来，用户
			// 只能杀应用。而 cancel 是**一问一答**的同步 RPC，回执到了就说明
			// 电脑已经受理，此时复位是确定的。
			//
			// 电脑随后补发的 `turn/end` 到手机时，`deriveRunning` 会看到
			// `running` 已经是 false 而直接跳过 —— 不会重复 emit。
			//
			// ⚠️ 必须用 `reconcileStateOf`（与 `deriveRunning` / `reconcileRunning`
			// 同一张表）。用 `mirrorStateOf` 的话，这里复位了「那张表」，
			// 而事件流读的是「这张表」，两边不一致 ⇒ 按钮卡住不消失。
			const state = reconcileStateOf(request.sessionId);
			if (state.running !== false) {
				state.running = false;
				ctx.emit('api-session/status', request.sessionId, false);
			}
			return { accepted: true };
		};

		return () => {
			controller.prompt = originalPrompt;
			controller.cancel = originalCancel;
		};
	}, 'dsh-remote-mobile: prompt/cancel 拦截');

	// ---- 屏蔽远程会话在手机本地的标题生成 ----
	//
	// ⚠️ 官方 `@deepseek-ai/dsh-session-title/lib/index.js` 的
	// `ctx.on("session/event")` 对**任何** `source.kind === 'user'` 的
	// `user/message` 都会在**本地**写一条 `session/title`
	//（`onUserMessage()` → `ensureFallback()`，源码 379-403 行），并在首轮再置
	// `state.pending`，让标题 LLM 补一条 `session/title-llm-request`
	//（`onRequestHeader()` 消费，源码 386 行那个条件）。
	//
	// 手机本地日志是电脑日志的**逐条镜像**，`mirrorEvent` 靠 seq 折算对齐
	//（见其注释）。镜像进来的 `user/message` 同样满足那个条件，于是手机端
	// **本地多写 2 条**事件，把电脑端同一 seq 上的 2 条真实事件挤掉 —— 而
	// `mirrorEvent` 对「折算 seq < 本地 seq」的事件是按「重连补发的历史」
	// **静默丢弃**的，被挤掉的那 2 条在手机上永久缺失。
	//
	// 真机实测（探针会话 `session-dc52fa34`，两轮对话，`_test/compare-logs.mjs`）：
	// 电脑端 41 条、手机端 41 条，seq 0-40 只有 **seq 9 与 seq 16** 不一致，
	// 丢的是电脑端的 workspace 说明（`system-reminder` 那条 `user/message`）
	// 与电脑端自己的 `session/title`，而手机端那两格是**本地生成**的
	// （phone seq=9 `time=1792088906625` `source:{kind:"fallback"}`，与电脑端
	// 同一条的 `time` 不同 ⇒ 确认不是镜像来的）。本轮丢的是说明文字，
	// 时序一变被挤掉的就会是 `assistant/message` —— 也就是**用户看不到 AI 回复**。
	//
	// 标题的真源在电脑端：电脑端生成后会把 `session/title` 流过来，所以远程
	// 会话在手机本地**一律不生成**标题。这样本地 seq 不再被抢占，两端日志
	// 恢复逐条一致，也省掉一次白烧用户额度的本地标题 LLM 调用。
	ctx.effect(() => {
		/** 已完成的还原函数（插件卸载时逐个执行）。 */
		const undo = [];
		let done = false;

		/**
		 * 给标题服务挂上「远程会话直接短路」的包装。
		 *
		 * @param titleService - `ctx.sessionTitle` 实例；未就绪时为 undefined。
		 * @returns 是否已经完成（含此前已完成）。
		 */
		const patchTitleService = (titleService) => {
			if (done) return true;
			if (titleService === undefined || typeof titleService.onUserMessage !== 'function') {
				return false;
			}
			const original = titleService.onUserMessage;
			// ⚠️ 必须用 `function` 而不是箭头函数：官方是 `this.onUserMessage(...)`
			// 这样调的，箭头函数会让 `this` 丢失、`original.call(this, ...)` 里的
			// `this` 落成 undefined，标题服务内部读 `this.registration` 即抛错。
			titleService.onUserMessage = function (session, event) {
				if (remoteSessions.has(session.id)) return undefined;
				return original.call(this, session, event);
			};
			undo.push(() => {
				titleService.onUserMessage = original;
			});
			done = true;
			return true;
		};

		// ⚠️ 两条路径都各自包 try/catch：屏蔽失败只是退化成「远端事件可能错位」，
		// 但**绝不能拖垮整个插件** —— 那会让远程功能完全不可用，比错位严重得多。
		//（`ctx.get(name)` 在服务未注册时返回 `undefined`，不抛错；这里防的是
		//  `ctx.get` / `ctx.inject` 本身在引擎版本变化后不再可用。）
		//
		// ⚠️ **两条路要分开 try**：若合在一个 try 里，`ctx.get` 一旦抛错，
		// `ctx.inject` 这条「服务迟一点才注册」的兜底路径会被一并跳过 ——
		// 守卫就这么**静默失效**了，而且日志里只有一条 warn，很难看出少兜了一次。
		const warn = (error) =>
			ctx.logger?.warn?.(
				`[dsh-remote-mobile] 未能屏蔽本地标题生成，远端事件镜像可能错位: ${String(
					error?.message ?? error,
				)}`,
			);

		// 路径一：服务通常在本插件加载前就已注册，先同步打一次。
		let patched = false;
		try {
			patched = patchTitleService(ctx.get('sessionTitle'));
		} catch (error) {
			warn(error);
		}

		// 路径二：还没就绪（或路径一失败）则交给依赖注入，等服务注册后再打。
		if (!patched) {
			try {
				ctx.inject(['sessionTitle'], (scoped) => {
					patchTitleService(scoped.sessionTitle);
				});
			} catch (error) {
				warn(error);
			}
		}

		return () => {
			for (const fn of undo) fn();
		};
	}, 'dsh-remote-mobile: 屏蔽远程会话的本地标题生成');

	// ---- 把电脑的模型目录与预设名单接进官方前端 ----
	//
	// ## 为什么拦在宿主侧，而不是改前端
	//
	// 官方前端的模型目录来自 `ctx.remote.session.modelCatalog()`（宿主 RPC），
	// 预设来自 `ctx.remote.agentPresets.list()`（宿主 RPC）。两个 RPC 的**宿主实现**
	// 就是 `sessionController.modelCatalog` 与 `agentPresets.remoteExportList`
	// （装饰器 `Remote("modelCatalog")` / `Remote("list")`，见
	// `dsh-api-session-controller/lib/index.js:2586`、
	// `dsh-agent-preset-registry/lib/index.js:426`）。
	//
	// 这两处都是 `Remote(...)` 装饰的方法，**经实例动态派发**（与 `create`/`prompt`
	// 同一机制），所以替换实例上的方法就能同时拦到直接调用与 RPC 调用 —— 不必碰
	// 前端 bundle，升级也不会碎。
	//
	// 用户要的是「选了电脑目录之后，模型列表刷新为电脑的」。前端拿到的目录里
	// **电脑的排前面**，手机自己那几组留在后面（原因见 `mergeCatalog` 的注释）。
	ctx.effect(() => {
		const undo = [];

		/** 已配对才值得把电脑的东西并进来；没配对时保持手机本地原样。 */
		const paired = () => config.token !== EMPTY_TOKEN;

		// ① 模型目录
		const controller = ctx.sessionController;
		if (controller !== undefined && typeof controller.modelCatalog === 'function') {
			const originalCatalog = controller.modelCatalog.bind(controller);
			controller.modelCatalog = async (...args) => {
				const local = await originalCatalog(...args);
				if (!paired()) return local;
				const desktop = await desktopCatalog();
				if (desktop === null) return local;
				return mergeCatalog(local, desktop);
			};
			undo.push(() => {
				controller.modelCatalog = originalCatalog;
			});

			// ② 选模型：远程会话的选择必须落到**电脑**上。
			//
			// ⚠️ 不转发的话，用户在前端选的模型只写进手机本地会话的投影，
			// 电脑那边仍用它自己的默认模型 —— 表现是「选了也不生效」。
			//
			// ## 会话还没在电脑上出生时（`planOf` 命中）
			//
			// 用户可能在选完工作区、**还没发消息**的时候改模型。此时电脑上根本没有
			// 这个会话，`selectModel` 过去必然失败（电脑端 `resolveAgent` 找不到）。
			//
			// 所以这种情况下**不发网络请求**，把选择记进待发送计划，等第一条消息
			// 时随 `prompt` 一起打包发过去（见 `prompt` 拦截器）。
			//
			// ⚠️ 但**仍要调一次 `originalSelect`**：它负责把选择写进手机会话的
			// `modelSelection` 投影（见官方 `selectionFor` 读的那个），前端模型菜单
			// 和「当前选中项」靠它更新。不调的话用户会看到菜单点完没反应。
			// 它只操作手机本地内存，不联网、不跑模型，所以是安全的。
			if (typeof controller.selectModel === 'function') {
				const originalSelect = controller.selectModel.bind(controller);
				controller.selectModel = async (request) => {
					if (!remoteSessions.has(request?.sessionId)) return originalSelect(request);
					// ## ⚠️⚠️ 无论走哪条分支，**先把选择落盘**（用户报障的直接修复）
					//
					// 用户原话：「我在手机选的是 wk 的 DeepSeek V4.1 flash，电脑发起
					// 请求的却是 GLM 5.3 flash，我的供应商没有这个模型，报错了。」
					//
					// 根因：选择原来只进 `pendingPlans`（内存），App 重启 / 被杀后台
					// 就没了 ⇒ 发消息时不带 `model` ⇒ 电脑用它自己的默认模型。
					//
					// 所以这里无条件写进 `remoteModels` 并落盘，作为 `plan.model`
					// 之外的**持久兜底**（`prompt` 拦截器会读它）。
					if (typeof request?.provider === 'string' && request.provider !== ''
						&& typeof request?.model === 'string' && request.model !== '') {
						remoteModels[request.sessionId] = {
							provider: request.provider,
							model: request.model,
							...(request.reasoningEffort === undefined
								? {}
								: { reasoningEffort: request.reasoningEffort }),
						};
						saveRemoteSessions(
							remoteSessions, ctx.logger, syncedSessions, remotePaths,
							desktopTitles, turnOffsets, remoteModels,
						);
						void debugLog(
							`selectModel 落盘 session=${request.sessionId} ${request.provider}/${request.model}`,
						);
					}
					const plan = planOf(request?.sessionId);
					if (plan !== undefined) {
						// 先让手机本地投影记住（前端立刻显示选中项），再记进计划。
						const local = await originalSelect(request);
						plan.model = {
							provider: request.provider,
							model: request.model,
							...(request.reasoningEffort === undefined
								? {}
								: { reasoningEffort: request.reasoningEffort }),
						};
						ctx.logger?.info?.(
							`[dsh-remote-mobile] 会话 ${request.sessionId} 的模型先记下（${request.provider}/${request.model}），随第一条消息一起送到电脑`,
						);
						return local;
					}
					// ⚠️ 老路径也**必须调一次 `originalSelect`**：它负责把选择写进
					// 手机本地的 `modelSelection` 投影（前端「当前选中项」读的就是它）。
					// 原来这里直接 `return selected`（电脑的返回值），本地投影从没更新
					// ⇒ 前端 `current` 一直停在旧模型 ⇒ 显示的名字和实际选的不一致。
					let local;
					try {
						local = await originalSelect(request);
					} catch (error) {
						const message = String(error?.message ?? error);
						void debugLog(
							`selectModel 本地投影失败 session=${request.sessionId} err=${message}`,
						);
						// ## ⚠️⚠️ 本地日志损坏 ⇒ 删掉重建后重试一次
						//
						// 真机实测（session-f40a3c76）：
						//   selectModel 本地投影失败 err=resume failed ... stored log is
						//   corrupt: SessionFormatError: session/title messageSeqs must
						//   cite distinct earlier human user/message events
						//
						// 日志坏了 ⇒ `resolveAgent` 起不来 ⇒ `originalSelect` 永远失败
						// ⇒ **手机本地投影永远停在旧模型** ⇒ 前端 `current` 拿到旧值
						// ⇒ 显示的名字和实际选的不一致，官方 1017 标签也跟着判断错。
						//
						// `ensureLocalMirror` 里已有同样的自愈（删掉坏日志重建），
						// 但那条路只在「SSE 事件到达时」触发，`selectModel` 这条
						// 用户主动操作的路径没覆盖到。这里补上。
						if (/corrupt|SessionFormatError|SessionQueryError/i.test(message)) {
							try {
								const removed = await removeCorruptMirror(request.sessionId, message);
								if (removed) {
									await ensureLocalMirror(request.sessionId);
									local = await originalSelect(request);
									void debugLog(
										`selectModel 损坏自愈后投影成功 session=${request.sessionId}`,
									);
								}
							} catch (healError) {
								void debugLog(
									`selectModel 损坏自愈失败 session=${request.sessionId} err=${String(healError?.message ?? healError)}`,
								);
							}
						}
					}
					const selected = await callRemote(config, 'selectModel', {
						sessionId: request.sessionId,
						provider: request.provider,
						model: request.model,
						...(request.reasoningEffort === undefined
							? {}
							: { reasoningEffort: request.reasoningEffort }),
					});
					// 电脑返回它**真正采纳**的模型（可能被它纠正过），以它为准，
					// 同时把本地投影也对齐一次，避免「显示一个、实际另一个」。
					const adopted = selected?.selected;
					if (adopted !== undefined && adopted !== null
						&& typeof adopted.provider === 'string' && typeof adopted.model === 'string') {
						if (adopted.provider !== request.provider || adopted.model !== request.model) {
							remoteModels[request.sessionId] = {
								provider: adopted.provider,
								model: adopted.model,
								...(typeof adopted.reasoningEffort === 'string'
									? { reasoningEffort: adopted.reasoningEffort }
									: {}),
							};
							saveRemoteSessions(
								remoteSessions, ctx.logger, syncedSessions, remotePaths,
								desktopTitles, turnOffsets, remoteModels,
							);
							try {
								await originalSelect({
									sessionId: request.sessionId,
									provider: adopted.provider,
									model: adopted.model,
									...(typeof adopted.reasoningEffort === 'string'
										? { reasoningEffort: adopted.reasoningEffort }
										: {}),
								});
							} catch (error) {
								void debugLog(
									`selectModel 对齐投影失败 session=${request.sessionId} err=${String(error?.message ?? error)}`,
								);
							}
						}
					}
					return local ?? selected;
				};
				undo.push(() => {
					controller.selectModel = originalSelect;
				});
			}
		}

		// ②.5 历史分页：同步来的对话，历史读电脑而不是读本地
		//
		// ## 为什么拦 `controller.page`
		//
		// 官方前端的会话窗口完全靠这个方法翻页
		//（`dsh-api-session-controller/lib/client.js`：
		//  `doOpen()` → `events.open({maxMessages: 50})` 拉尾部一页；
		//  `loadOlder()` → `events.prepend({beforeSeq: this.baseSeq, maxMessages: 50})`；
		//  两者最终都落到 `controller.page()`，见 `lib/index.js:2954`）。
		//
		// 也就是说「只加载最近 N 条 / 上滑加载更多」**前端本来就实现了**，
		// 我只要把这个方法的**数据源**换成电脑即可 —— 不用自己造 UI，也不用
		// 往手机本地灌历史（那样必然要灌全量，155 MB 起）。
		//
		// ## 只对 `syncedSessions` 生效
		//
		// 手机自己新建的远程会话（`createSession` 桥建的）历史在本地，读本地更快，
		// 不该白跑一趟隧道。所以判据是 `syncedSessions.has(...)` 而不是
		// `remoteSessions.has(...)`。
		const originalPage = typeof controller.page === 'function' ? controller.page.bind(controller) : null;
		if (originalPage !== null) {
			controller.page = async (request, signal) => {
				const sessionId = request?.address?.sessionId;
				if (typeof sessionId !== 'string' || !syncedSessions.has(sessionId)) {
					return originalPage(request, signal);
				}
				if (!paired()) return originalPage(request, signal);
				try {
					// 诊断：把前端真正传进来的分页参数记下来（环形 20 条），
					// 用 `debugPage` 桥读出来。排查「历史加载失败」时这是唯一的
					// 事实来源 —— 前端的 `throughSeq` 到底是多少、和电脑的 tail
					// 差多少，一眼就能看出来。
					pageDiag.push({
						at: Date.now(),
						sid: sessionId,
						through: typeof request?.throughSeq === 'number' ? request.throughSeq : null,
						before: typeof request?.beforeSeq === 'number' ? request.beforeSeq : null,
						max: request?.maxMessages,
					});
					if (pageDiag.length > 20) pageDiag.shift();
					// ⚠️⚠️ **`throughSeq` 必须原样透传，不能让电脑端自己算。**
					//
					// 这是 1.0.72 真机踩到的硬坑：前端 `RemoteJournalStream.prepend()`
					// 调 `readPage(request, this.currentCursor())`，拿到 page 后立刻
					// `assertPageThrough(page, through)` —— 判据是
					// **`tail(entries) === through`（精确相等，不是 ≤）**，不等就抛
					// `session event stream page did not end at its requested cursor`，
					// 界面显示「历史加载失败」。
					//
					// 也就是说：**这一页的最后一个事件 seq 必须精确等于 `throughSeq`**。
					// 早先的写法完全忽略 `request.throughSeq`、只按「最后 10 条」算，
					// 只要电脑的 tail 与前端算出的 through 差一点（发过消息之后必然差），
					// 就整页作废。
					//
					// `throughSeq` 在同步会话里就是**电脑的 seq**（窗口全程使用电脑 seq，
					// 见文件头「seq 坐标系」），所以直接转发即可，不需要换算。
					const body = {
						sessionId,
						// ⚠️ 默认只拉 10 条 —— 用户原话：「只加载最近 10 条左右」。
						// 前端传的 `maxMessages: 50` 对手机隧道来说太大：实测一条消息
						// 平均 4 KB 量级，50 条就是 200 KB，而 10 条只要 40 KB。
						// 上滑时前端自己会再要一页，体验上没差别。
						maxMessages: 10,
					};
					// ⚠️ 优先用 `throughSeq`（精确边界，见上）。
					if (typeof request?.throughSeq === 'number' && Number.isSafeInteger(request.throughSeq) &&
						request.throughSeq >= -1) {
						body.throughSeq = request.throughSeq;
					} else if (typeof request?.beforeSeq === 'number' && request.beforeSeq >= 0) {
						// 兜底：没有 `throughSeq` 时按 `beforeSeq - 1` 推（两者语义等价，
						// 因为 `prepend` 的 `through` 恒等于 `before - 1`）。
						body.throughSeq = request.beforeSeq - 1;
					}
					const page = await callRemote(config, 'readHistory', body);
					if (page !== null && typeof page === 'object' && Array.isArray(page.records)) {
						// ⚠️ **必须逐条过滤**：前端 `readPage` 会对每条
						// `assertSessionWireEvent(record.event)`，一条坏数据就抛错，
						// 「加载更多」整页作废。电脑端是官方引擎，正常不会出错，
						// 但链路中间过 Cloudflare 隧道，不能假设一定完好。
						const records = page.records.filter(
							(record) => record !== null && typeof record === 'object' && isWireEvent(record.event),
						);
						// 诊断：记下电脑实际返回的 tail，和前端要的 through 对比。
						const tail = records.length === 0 ? -1 : records[records.length - 1].event.seq;
						const want = typeof body.throughSeq === 'number' ? body.throughSeq : null;
						pageDiag.push({ at: Date.now(), sid: sessionId, phase: 'ret', want, tail, got: records.length, hasMore: page.hasMore === true });
						if (pageDiag.length > 20) pageDiag.shift();
						// ⚠️⚠️ **tail 必须精确等于前端要的 `throughSeq`**，否则前端
						// `assertPageThrough` 会抛
						// `session event stream page did not end at its requested cursor`
						// ⇒ 界面显示「历史加载失败」。
						//
						// 电脑给不出那个位置（日志还没追上 / 那条被截断）时，
						// **宁可返回空页并让前端停在原地**，也不能把一页尾巴不对的
						// 数据交出去 —— 后者会让整个会话报错。
						//
						// 空页的 `tailCursor` 是 `emptyCursor = -1`，所以只有
						// `want === -1` 时空页才合法；其余情况只能靠「让前端不要
						// 请求那个位置」来解决（见 `followSynced` 的 cursor 同步）。
						return { records, hasMore: page.hasMore === true };
					}
					// 电脑给的形状不对（老版本电脑端？）就退回本地，至少不白屏。
					return originalPage(request, signal);
				} catch (error) {
					ctx.logger?.warn?.(
						`[dsh-remote-mobile] 读电脑历史失败（退回本地）: ${String(error?.message ?? error)}`,
					);
					return originalPage(request, signal);
				}
			};
			undo.push(() => {
				controller.page = originalPage;
			});
		}

		// ②.5 读附件：同步来的对话，图片字节要去**电脑**拿
		//
		// ## 为什么必须拦（真机实测出来的）
		//
		// 同步会话在手机本地不落盘、不存附件。而用户气泡里的图片缩略图走的是
		// `sessionAttachment.readAttachment()` → `this.remote.session.attachment({sessionId, attachmentId})`
		// （`dsh-api-session-controller/lib/client.js:1517`），这个 RPC 打到**手机本地
		// 引擎**，本地没有那个附件 ⇒ reject ⇒ 前端 `MessageImage` 的
		// `.catch(() => setError(true))` 命中 ⇒ 缩略图位置显示
		// 「图片加载失败，点击重试」。
		//
		// 真机现象：手机发出去的图片**电脑端正常收到、AI 也看到了**（回复了「收到」），
		// 但手机上自己那条消息里的三个缩略图全是「加载失败」。用户会以为图没发出去。
		//
		// 修法：对同步会话把这次读取**转发到电脑**。电脑端的 `attachment` 分支
		//（`dsh-api-session-controller/lib/index.js:911`）会用
		// `attachments.readImage(ref)` 读出原始字节并 base64 回来，形状与官方
		// RPC 的返回**完全一致**（`{attachment, data}`），所以前端不需要任何改动。
		const originalAttachment = typeof controller.attachment === 'function' ? controller.attachment.bind(controller) : null;
		if (originalAttachment !== null) {
			controller.attachment = async (request, signal) => {
				const sessionId = request?.sessionId;
				if (
					typeof sessionId !== 'string' ||
					!syncedSessions.has(sessionId) ||
					!paired() ||
					typeof request?.attachmentId !== 'string' ||
					request.attachmentId === ''
				) {
					return originalAttachment(request, signal);
				}
				try {
					// ⚠️ 电脑端返回的 `data` 已经是 **base64 字符串**，直接用 ——
					// 这里的 `controller.attachment` 是**服务端**方法（宿主侧），
					// 前端那层才做 `atob` 解成 Uint8Array。
					return await callRemote(config, 'attachment', {
						sessionId,
						attachmentId: request.attachmentId,
					});
				} catch (error) {
					// 电脑离线 / 图片在电脑上也没了：退回本地实现，
					// 让官方的错误路径照常走（前端显示「加载失败」，可重试）。
					ctx.logger?.warn?.(
						`[dsh-remote-mobile] 读电脑上的图片附件失败（退回本地）: ${String(error?.message ?? error)}`,
					);
					return originalAttachment(request, signal);
				}
			};
			undo.push(() => {
				controller.attachment = originalAttachment;
			});
		}

		// ②.6 打开会话：同步来的对话，**整个窗口都从电脑来**
		//
		// ## 为什么光拦 `page` 不够（这是最容易漏的一环）
		//
		// 官方前端打开会话走的是 `doOpen()` → `events.open({maxMessages: 50})`
		//（`dsh-api-session-controller/lib/client.js:1995`），而 `open()` 内部是
		// **`follow()`**（`journal-stream.js:38` → `SessionEventStream.follow`），
		// **不是** `page()`。`page()` 只在「往上翻」时用（`loadOlder`）。
		//
		// 而同步来的会话在手机本地是**空**的（一条事件都没有），所以原生
		// `follow()` 会返回一个空 `snapshot`（`records: []`）—— 界面就是一片空白，
		// 用户看到的是「同步过来的对话点进去什么都没有」。
		//
		// ## 为什么**不能**把电脑的事件 append 进手机本地日志
		//
		// 一开始想的是「把尾部一页写进本地 session，然后照常跑本地流」。这条路
		// 物理上走不通，三个硬事实：
		//
		// ① `dsh-session/lib/index.js:1195` 是 `get seq() { return this.log.length }`，
		//    `append()` 硬编码 `seq: SessionSeq(this.log.length)` —— **本地 seq 只能
		//    从 0 连续涨**，没法从电脑的 seq 开始。电脑那个会话的 seq 是 29637 起，
		//    写进去必然对不上。
		// ② 本文件的 `mirrorEvent()` 有严格的**连续 seq 校验**，缺口直接抛错。
		// ③ 就算绕过校验，本地一旦只有尾部 N 条，`session.seq` 就是 N，而 SSE 桥
		//    会拿它当 `fromSeq` 报给电脑 ⇒ 电脑补发 `seq > N` 的全部历史 ⇒
		//    **又变成 155 MB 全量重放**，正好是本轮要修的那个 bug。
		//
		// 所以同步会话**不落本地日志**：`follow` 直接把电脑的事件流搬给前端，
		// 全程使用**电脑的 seq 体系**（前端只做相对比较，不关心从几开始）。
		const originalFollow = typeof controller.follow === 'function' ? controller.follow.bind(controller) : null;
		/**
		 * 取某会话的投影值，拼成 `follow` 首帧要的 `projections.values`。
		 *
		 * ## 为什么需要（这是侧边栏「未命名」的真正根因）
		 *
		 * `controller.follow` 的第一帧是 `type: 'snapshot'`，里面带 `projections`。
		 * 前端会拿它更新会话摘要。原来这里写死 `values: {}` —— 空投影 ⇒
		 * `title` 是 `undefined` ⇒ 侧边栏回退成「未命名」。
		 *
		 * 于是出现一个很迷惑的现象：数据层完全正确（`session/list` 的
		 * `title` 是「防用户插件崩溃方案」、投影缓存也对），但**每次切换会话**，
		 * 侧边栏上那一个就变成「未命名」—— 切换触发 `follow`，空投影又盖上去。
		 * 刷新能救回别的行、救不回当前行，因为当前行正是刚 follow 过的那个。
		 *
		 * ## ⚠️⚠️ 只填 `title` 是**不够**的（用户 2026-10-21 报障）
		 *
		 * 用户原话：「底部的 token 信息以及上下文信息，都没有显示了」。
		 *
		 * 底部那两段各有**唯一**的数据源，且都只从这一帧来：
		 *
		 *   · `tokenUsage`      → `UsagePill`（`30.7K·缓存94%`）
		 *   · `contextPressure` → 上下文环（`12%`）
		 *
		 * 依据 `dsh-client-ui-chat/lib/client.js`：
		 *
		 *     const usage = useProjection("tokenUsage");
		 *     if (usage === void 0 || …) return null;   // ← 整块不渲染
		 *
		 * 所以原来只塞 `title` 的写法，对同步会话等于把底部整行抹掉了 ——
		 * 而这两个投影**在电脑上早就算好了**（`readHistory` 的 `page` 返回里
		 * 就带完整的 `projections`，见 `dsh-api-session-controller/lib/index.js:1580`
		 * 的 `projectionBlock(source.projections)`），我们只是没往下传。
		 *
		 * ## 取值顺序
		 *
		 * 1. `remoteProjections` —— 电脑那一页带下来的投影。**这是同步会话唯一的
		 *    真实来源**：手机上根本没有它的日志，本地投影永远是空的。
		 * 2. 本地投影服务（活会话才有，手机自己建的远程会话走这条）。
		 * 3. `desktopTitles`（同步时从电脑抄来的标题）。
		 *
		 * @param sessionId - 会话 id。
		 * @param remoteProjections - 电脑 `readHistory` 返回的 `projections`（可缺）。
		 * @returns 至少含 `title` 的 values 对象；都读不到时返回空对象（保持旧行为）。
		 */
		const titleValueFor = (sessionId, remoteProjections) => {
			// ① 电脑的投影：同步会话的正解。
			//
			// ⚠️ 只透传**前端真正消费**的那几个键，不整包转发：
			// `projections.values` 里有 `inbox` / `todos` / `userQuestions` 等
			// 一大堆，它们的 `asOfSeq` 与手机端的 cursor 未必对得上，混进来
			// 反而可能让别的投影显示成电脑上的旧值（比如待办列表）。
			// 这里只补「纯统计、只增不减、错了也不会误导操作」的那两个。
			const remoteValues =
				remoteProjections !== null && typeof remoteProjections === 'object' ? remoteProjections.values : undefined;
			const picked = {};
			if (remoteValues !== null && typeof remoteValues === 'object') {
				if (remoteValues.tokenUsage !== undefined) picked.tokenUsage = remoteValues.tokenUsage;
				if (remoteValues.contextPressure !== undefined) picked.contextPressure = remoteValues.contextPressure;
				if (typeof remoteValues.title === 'string' && remoteValues.title !== '') picked.title = remoteValues.title;
			}
			if (Object.keys(picked).length > 0) return picked;
			// ② 本地投影服务（活会话）。
			try {
				const live = ctx.sessions?.get?.(sessionId);
				if (live !== undefined) {
					const projected = ctx.sessionProjections?.stateOf?.(live, 'title');
					if (typeof projected === 'string' && projected !== '') return { title: projected };
				}
			} catch {
				/* 投影服务不可用就当读不到 */
			}
			// ③ 同步时抄下来的标题。
			const remembered = desktopTitles[sessionId];
			if (typeof remembered === 'string' && remembered !== '') return { title: remembered };
			return {};
		};
		if (originalFollow !== null) {
			controller.follow = async function* (request, signal) {
				const sessionId = request?.address?.sessionId;
				const fromRemote =
					typeof sessionId === 'string' && syncedSessions.has(sessionId) && paired();
				if (!fromRemote) {
					yield* originalFollow(request, signal);
					return;
				}
				// 先把电脑那一页历史拿下来（失败就老老实实走本地，不白屏）。
				let firstPage = null;
				try {
					firstPage = await callRemote(config, 'readHistory', { sessionId, maxMessages: 10 });
				} catch (error) {
					ctx.logger?.warn?.(
						`[dsh-remote-mobile] 同步会话 ${sessionId} 取开场历史失败（退回本地）: ${String(
							error?.message ?? error,
						)}`,
					);
					firstPage = null;
				}
				// 再单独问一次电脑的投影 —— `tokenUsage` / `contextPressure` 只有它
				// 里面有，而 `readHistory` 是拿不到的（`page()` 只返回
				// `{records, hasMore}`，见电脑端 `readProjections` 的注释）。
				//
				// ⚠️ 失败**不能**影响主流程：拿不到就退回只填 title，与改动前一致。
				let remoteProjections;
				try {
					const answer = await callRemote(config, 'readProjections', { sessionId });
					remoteProjections = answer?.projections;
				} catch (error) {
					ctx.logger?.info?.(
						`[dsh-remote-mobile] 取电脑投影失败（底部统计可能不显示）: ${String(error?.message ?? error)}`,
					);
					remoteProjections = undefined;
				}
				// ⚠️ **必须逐条过滤**：前端 `SessionEventStream.follow`
				//（`dsh-api-session-controller/lib/client.js:395`）对每条
				// `assertSessionWireEvent(record.event)`，一条坏数据就让整个
				// 会话打不开（不是跳过那一条）。链路中间过 Cloudflare 隧道，
				// 不能假设一定完好。
				const records =
					firstPage !== null && Array.isArray(firstPage.records)
						? firstPage.records.filter(
								(record) => record !== null && typeof record === 'object' && isWireEvent(record.event),
							)
						: [];
				if (records.length === 0) {
					// ⚠️⚠️ **空历史也要派发运行态，然后才退回本地**（用户报障第二根因）。
					//
					// 原来这里直接 `yield* originalFollow()` 就 return 了 —— 于是
					// `followSynced` **一次都不被调用**，而运行态校正就住在
					// `followSynced` 里（见那里的注释）。结果是：
					//
					//   「历史为空 / 取历史失败」的同步会话，即使电脑正在跑，
					//    手机上也永远不显示「深度求索中」，也没有「停止生成」。
					//
					// 而「退出 App 再回来」恰恰最容易落到这条分支：`readHistory`
					// 走公网隧道，冷启动时隧道刚起、握手慢，很容易超时失败
					// ⇒ `firstPage === null` ⇒ `records` 为空 ⇒ 走到这里。
					//
					// 所以先补一次校正（一问一答，几十字节），再退回本地 ——
					// 校正失败就算了，不能因为「状态问不到」把会话整个卡住。
					reconcileRunning(ctx, config, sessionId);
					yield* originalFollow(request, signal);
					return;
				}
				const hasMore = firstPage !== null && firstPage.hasMore === true;
				// ⚠️ **cursor 必须取最后一条的 seq**：官方 `RemoteJournalStream` 用它作
				// 「已收到哪一条」的水位（`firstCursor` / `lastCursor` 的初值），
				// 也是后续 `prepend` 判据 `follows(last(tail), before)` 的基准。
				// 取 0 或 -1 会让「往上翻」那一页被判成不连续而整页丢弃。
				const cursor = records[records.length - 1]?.event?.seq ?? -1;
				// ⚠️ 形状必须与官方 `follow` 的第一帧**完全一致**：
				// `SessionEventStream.follow` 读的是 `frame.type === 'snapshot'`，
				// 然后取 `records` / `hasMore` / `cursor` / `projections`。
				// `assistantStream` 也必须有（它 `throw` 说「opted-in 却漏了 baseline」）。
				//
				// ## ⚠️⚠️ `projections.values` 不能是空的（侧边栏「未命名」根因）
				//
				// 原来这里写死 `values: {}`。而前端会拿这一帧的 `projections`
				// 去更新会话摘要 —— 空投影意味着 `title` 是 `undefined`，
				// 侧边栏随即回退成 i18n 的「未命名」。
				//
				// 真机表现：数据层完全正确（`session/list` 里 `title` 是
				// 「防用户插件崩溃方案」、投影缓存也对），但**每次切换会话**
				// 侧边栏上那一个就变成「未命名」—— 因为切换触发 `follow`，
				// 这一帧又把空投影盖了上去。刷新能救回别的行、救不回当前行，
				// 也是这个原因（当前行正是刚 follow 过的那个）。
				//
				// 所以这里填**真实的标题**：本地会话能从投影服务读到就直接读；
				// 读不到（同步会话不落盘）就退回电脑上的标题（由调用方查好传进来）。
				yield {
					type: 'snapshot',
					header: ctx.sessions.get(sessionId)?.header ?? { id: sessionId },
					cursor,
					records,
					hasMore,
					projections: {
						asOfSeq: cursor,
						// ⚠️ 把电脑的投影一起传进去 —— `tokenUsage` / `contextPressure`
						// 只有它里面有（同步会话在手机上不落盘，本地投影永远是空的）。
						// 见 `titleValueFor` 与电脑端 `readProjections` 的注释。
						values: titleValueFor(sessionId, remoteProjections),
					},
					assistantStream: { revision: 0 },
				};
				// 之后接着把电脑的**实时事件**搬给前端（从 cursor 之后开始）。
				//
				// 这里直接复用本插件的 SSE 桥（`follow` 函数），它已经处理了断线
				// 重连、`fromSeq` 增量、心跳、buffer 上限。但那条桥的职责是「往本地
				// session 里 append」，而同步会话不落盘，所以这里单独开一条**只转发**
				// 的流。
				yield* followSynced(sessionId, cursor, signal);
			};
			undo.push(() => {
				controller.follow = originalFollow;
			});
		}

		// ③ 预设名单
		const patchPresets = (presets) => {
			if (presets === undefined || typeof presets.remoteExportList !== 'function') return false;
			const originalList = presets.remoteExportList.bind(presets);
			presets.remoteExportList = async (...args) => {
				const local = await originalList(...args);
				if (!paired()) return local;
				// ## ⚠️⚠️ 缓存空就**当场拉一次**（2026-10-22 修）
				//
				// 原来这里是 `if (!paired() || desktopCache.presets === null) return local;`
				// —— 缓存为空就**直接返回手机本地的预设**，不再尝试联网。
				//
				// 后果正是用户报的：「配对成功之后，预设没有自动刷新过来，
				// 必须退出 App 重启才正常。」
				//
				// 链条：配对成功那一刻 `desktopCache.presets` 还是 `null`
				//（预拉是后台异步动作，没跑完）⇒ 前端紧接着调的这一次
				// `remoteExportList` 拿到的是**手机本地预设** ⇒ 页面已经渲染完
				// ⇒ 之后再拉到电脑预设也没人重新渲染 ⇒ 只能重启。
				//
				// 这与模型目录是**同一个坑**，`desktopCatalog()` 早就加了
				// 「缓存为空就当场拉」的处理（见它的注释），预设这边一直漏着。
				// 现在补齐，两边行为一致。
				if (desktopCache.presets === null) {
					try {
						desktopCache.presets = await callRemote(config, 'presetList', {});
						desktopCache.at = Date.now();
						persistDesktopCache();
					} catch (error) {
						ctx.logger?.warn?.(
							`[dsh-remote-mobile] 取电脑预设列表失败: ${String(error?.message ?? error)}`,
						);
						return local;
					}
				}
				if (desktopCache.presets === null) return local;
				return mergePresets(local, desktopCache.presets);
			};
			undo.push(() => {
				presets.remoteExportList = originalList;
			});
			return true;
		};

		// 与标题守卫同构：先同步试一次，没就绪再交给依赖注入兜底。
		// 两条路分开 try，否则 `ctx.get` 一抛错就把 `ctx.inject` 那条兜底也跳过了。
		const warn = (error) =>
			ctx.logger?.warn?.(
				`[dsh-remote-mobile] 未能接上电脑的模型/预设列表: ${String(error?.message ?? error)}`,
			);
		let patched = false;
		try {
			patched = patchPresets(ctx.get('agentPresets'));
		} catch (error) {
			warn(error);
		}
		if (!patched) {
			try {
				ctx.inject(['agentPresets'], (scoped) => {
					patchPresets(scoped.agentPresets);
				});
			} catch (error) {
				warn(error);
			}
		}

		return () => {
			for (const fn of undo) fn();
		};
	}, 'dsh-remote-mobile: 电脑模型目录 / 预设名单接入');

	// ---- 把远程会话的「文件树」接到电脑上（侧边栏 + 文件预览） ----
	//
	// 官方侧边栏的文件面板（`dsh-client-ui-sidebar-files`）以及内容预览
	// （`dsh-client-ui-document-preview`）都只认 host 的 `workspaceFiles` 服务：
	//
	//   list(scope, path)              → 一层目录（含文件）
	//   read(scope, path, {offset})    → 文本分页
	//   stat(scope, path)              → 版本/大小
	//   readBytes(scope, path, opts)   → 二进制分页
	//
	//   scope = {sessionId, workspaceRoot}，由官方按 sessionId 从会话 header 现算。
	//
	// ⚠️ **远程会话不能走本地的 `workspaceFiles`**：它的 `workspaceRoot` 是本插件
	// 在手机上的**镜像空壳目录**（只为了让「备选区」里有个能点的项，见
	// `ensureDesktopWorkspace`）。照本地那条路走，侧边栏里永远是**空的** ——
	// 而用户要的是看到电脑上的目录树。所以这里把这几条调用转给电脑。
	//
	// ⚠️ **`changes`（自动刷新）刻意不转发**：官方那份是流式 RPC，本插件的中继
	// 是一问一答，跨端桥一条长连的代价远大于收益。本地的 watcher 落在镜像空壳上，
	// 会正常回 `ready`、只是永远不报变化 —— 侧边栏不会因此报错，用户用手动
	// 「刷新」按钮看最新内容（产品上也是这么定的）。
	ctx.effect(() => {
		const undo = [];

		/**
		 * 判定会话是不是远程会话，并把路径换算成「相对于工作区」的一段。
		 *
		 * @returns 相对路径（根目录是空串）；不是远程会话时 `undefined`。
		 */
		const mapPath = (scope, path) => {
			const desktopRoot = desktopPathOfMirror(workspaceMap, scope?.workspaceRoot);
			if (desktopRoot === undefined) return undefined;
			if (typeof path !== 'string' || path === '') return undefined;
			// 侧边栏给的路径以会话 cwd 开头（官方 `childPath` 逐层拼出来），而
			// `scope.workspaceRoot` 与它同源（都是会话 header 的 cwd）⇒ 前缀直接剥掉。
			//
			// ⚠️ **只传相对路径，绝不在这里拼电脑的绝对路径**：两端盘符/分隔符不同，
			// 拼接是电脑那一端的事（见文件头「cwd 绝不跨端传」）。
			const a = path.replace(/\\/g, '/');
			const b = String(scope.workspaceRoot).replace(/\\/g, '/').replace(/\/+$/, '');
			if (a === b) return { path: '' };
			if (a.startsWith(`${b}/`)) return { path: a.slice(b.length + 1) };
			// 不在工作区内的路径：原样交给电脑去解析（它自己会判越界）。
			return { path: a };
		};

		/**
		 * 只处理远程会话；本地会话原样交给官方。
		 *
		 * @param name - `workspaceFiles` 上的方法名。
		 * @param original - 该方法**本来的**实现（可能没有）。
		 */
		const forward = (name, original) => async (scope, path, ...rest) => {
			const mapped = remoteSessions.has(scope?.sessionId) ? mapPath(scope, path) : undefined;
			if (mapped === undefined) {
				if (original === undefined) throw new Error(`workspaceFiles.${name} 不可用`);
				return await original(scope, path, ...rest);
			}
			if (name === 'list') {
				return await callRemote(config, 'fileList', { sessionId: scope.sessionId, path: mapped.path });
			}
			if (name === 'read') {
				const range = rest[0] ?? {};
				return await callRemote(config, 'fileRead', {
					sessionId: scope.sessionId,
					path: mapped.path,
					...(range.offset === undefined ? {} : { offset: range.offset }),
				});
			}
			if (name === 'stat') {
				return await callRemote(config, 'fileStat', { sessionId: scope.sessionId, path: mapped.path });
			}
			// readBytes：字节没法直接进 JSON，两端用 base64 过一道。
			//
			// ⚠️ **丢掉 `baseFile`**：官方那份是「相对 baseFile 所在目录」的语义，
			// 而这里的路径已经换算成电脑上的绝对路径了，再带上一个手机路径的
			// baseFile 只会让电脑去解析一个不存在的相对路径。
			const result = await callRemote(config, 'fileReadBytes', {
				sessionId: scope.sessionId,
				path: mapped.path,
				...(rest[0]?.range === undefined ? {} : { range: rest[0].range }),
			});
			const { data, ...meta } = result ?? {};
			return { ...meta, data: new Uint8Array(Buffer.from(String(data ?? ''), 'base64')) };
		};

		const patch = (files) => {
			if (files === undefined || typeof files.list !== 'function') return false;
			let count = 0;
			for (const name of ['list', 'read', 'stat', 'readBytes']) {
				const fn = files[name];
				if (typeof fn !== 'function') continue;
				files[name] = forward(name, fn.bind(files));
				undo.push(() => {
					files[name] = fn;
				});
				count += 1;
			}
			return count > 0;
		};

		const warn = (error) =>
			ctx.logger?.warn?.(
				`[dsh-remote-mobile] 未能接上电脑的文件树: ${String(error?.message ?? error)}`,
			);
		let patched = false;
		try {
			patched = patch(ctx.get('workspaceFiles'));
		} catch (error) {
			warn(error);
		}
		if (!patched) {
			try {
				ctx.inject(['workspaceFiles'], (scoped) => {
					patch(scoped.workspaceFiles);
				});
			} catch (error) {
				warn(error);
			}
		}

		return () => {
			for (const fn of undo) fn();
		};
	}, 'dsh-remote-mobile: 电脑文件树接入（侧边栏 / 预览）');

	// ---- 浏览器桥：目录浏览 / 建会话 ----
	ctx.effect(
		() =>
			ctx.webServer.register({
				kind: 'exact',
				path: ROUTE_RPC,
				handler: (req, res) => {
					const reply = (code, payload) => {
						res.writeHead(code, { 'content-type': 'application/json; charset=utf-8' });
						res.end(JSON.stringify(payload));
					};
					// ⚠️ 前置准入，必须在解析 body 之前（见 `bridgeRejection` 的 docstring）。
					const rejected = bridgeRejection(ctx, req);
					if (rejected !== undefined) {
						res.writeHead(rejected);
						res.end();
						return;
					}
					// ⚠️ **必须校验 content-type**（外部审计 5.2a）：跨站表单能直接发
					// `text/plain`，而那是不触发 CORS 预检的三种类型之一；本插件的门
					// 是非浏览器的（原生 App / 重载窗口内的页面）也过得了。
					// 这一条把「body 必须是 JSON」变成硬约束，而不是靠 JSON.parse 兜底。
					const contentType = String(req.headers['content-type'] ?? '');
					if (!contentType.toLowerCase().includes('application/json')) {
						reply(415, { error: 'content-type 必须是 application/json' });
						return;
					}
					readBodyLimited(req, RPC_BODY_LIMIT).then((read) => {
						if (!read.ok) {
							// ⚠️ 超限/读错时**断开连接**：不把剩下的字节继续收进来，
							// 否则「限制」只是不发回应，内存照样被吃光。
							reply(read.code, { error: read.message });
							req.destroy();
							return;
						}
						let body;
						try {
							body = JSON.parse(read.text);
						} catch (error) {
							reply(400, { error: `bad json: ${String(error?.message ?? error)}` });
							return;
						}
						handleBridge(ctx, config, {
							remoteSessions,
							// 同步来的历史对话（`remoteSessions` 的子集）：它们的
							// **历史不在本地**，读历史要转发电脑（见 `syncConversations`）。
							syncedSessions,
							follow,
							// 解绑要能停掉全部旧连接、清掉全部搬运状态（见 `unpair`）。
							follows,
							mirrorStates,
							// ⚠️ **必须显式传进来**：`handleBridge` 是模块级函数，作用域里
							// 没有 `apply()` 的闭包变量；漏传就是 `workspaceMap is not defined`，
							// 用户点「在此新建对话」时必然失败（1.0.16 的真实报障）。
							workspaceMap,
							// ⚠️ **同上，`pendingPlans` 也必须显式传**（1.0.64 的真实报障：
							// `createSession` 桥里写 `pendingPlans.set(...)`，而 `handleBridge`
							// 是模块级函数 ⇒ `ReferenceError: pendingPlans is not defined`
							// ⇒ 整个桥返回 `{ok:false}`，前端表现为「点了『选择此文件夹』
							// 什么都不发生、按钮还锁死了」）。
							pendingPlans,
							promptDiag,
							// ⚠️ 同 `promptDiag`：`handleBridge` 是模块级函数，
							// 看不见 `apply()` 里的 `pageDiag`。
							pageDiag,
							persist: () => saveRemoteSessions(remoteSessions, ctx.logger, syncedSessions, remotePaths, desktopTitles, turnOffsets, remoteModels),
							// ⚠️ **必须显式传**（同 `pendingPlans` 那个坑）：
							// `handleBridge` 是模块级函数，看不见 `apply()` 闭包里的
							// `remotePaths`。不传就 `rememberPath is not defined`
							// ⇒ 同步会话时落盘路径失败 ⇒ 重启后又发不了消息。
							rememberPath: (sessionId, desktopPath) => {
								if (typeof sessionId !== 'string' || sessionId === '') return;
								if (typeof desktopPath !== 'string' || desktopPath === '') return;
								remotePaths[sessionId] = desktopPath;
							},
							// ⚠️ 同 `rememberPath`：`handleBridge` 是模块级函数，
							// 看不见 `apply()` 闭包里的 `desktopTitles`。
							// 不传就 `rememberTitle is not defined` ⇒ 同步照常成功，
							// 但 `follow` 首帧拿不到标题 ⇒ 侧边栏「未命名」。
							rememberTitle: (sessionId, title) => {
								if (typeof sessionId !== 'string' || sessionId === '') return;
								if (typeof title !== 'string' || title === '') return;
								desktopTitles[sessionId] = title;
								// 立即落盘：`follow` 首帧要用它，重启后也要在
								//（见 `loadDesktopTitles` 的注释）。
								saveRemoteSessions(remoteSessions, ctx.logger, syncedSessions, remotePaths, desktopTitles, turnOffsets, remoteModels);
							},
							// ⚠️ 同 `rememberTitle`：`markSessionNonBlank` 是模块级函数，
							// 看不见 `apply()` 闭包里的 `turnOffsets`。不传的话占位轮次
							// 写进去了却没记账 ⇒ 电脑端真实事件会撞上占位轮次 ⇒
							// `step/start does not match the open turn`（会话又打不开）。
							rememberTurnOffset: (sessionId, offset) => {
								if (typeof sessionId !== 'string' || sessionId === '') return;
								if (typeof offset !== 'number' || !Number.isInteger(offset) || offset <= 0) return;
								turnOffsets[sessionId] = offset;
								// 立即落盘：App 重启后 `mirrorEvent` 还要靠它决定该不该 +1
								//（见 `loadTurnOffsets` 的注释）。
								saveRemoteSessions(remoteSessions, ctx.logger, syncedSessions, remotePaths, desktopTitles, turnOffsets, remoteModels);
							},
							// 模型/预设的静默缓存：配对后预拉、刷新按钮回写（见 desktopCache）。
							desktopCache,
							// ⚠️ 也要显式传：`refreshAll` 是模块级 `handleBridge` 里的
							// case 分支，同样看不见 `apply()` 的闭包变量。
							persistDesktopCache,
							// ⚠️ **同上一行**：`modelCatalog` 分支里 `desktopCatalog()`
							// 也是 `apply()` 的闭包常量（`:1629` 的 `const`），
							// 不传就是 `desktopCatalog is not defined`
							// ⇒ 手机上模型列表刷不出来（1.0.70 自查发现）。
							desktopCatalog,
							prefetch: prefetchDesktop,
							// ⚠️ 同 `workspaceMap`：`handleBridge` 是模块级函数，闭包里的
							// `prefetchDirs` / `rememberDir` 它看不见，必须显式传。
							prefetchDirs,
							rememberDir,
							body,
						}).then(
							(result) => reply(200, { ok: true, result }),
							(error) => reply(200, { ok: false, error: String(error?.message ?? error) }),
						);
					});
				},
			}),
		'dsh-remote-mobile: browser rpc route',
	);

	// ---- 浏览器流式订阅（全局一条） ----
	ctx.effect(
		() =>
			ctx.webServer.register({
				kind: 'exact',
				path: ROUTE_STREAM,
				handler: (req, res) => {
					// ⚠️ 同上：`GET /api/dsh-remote/stream` 会把**全部远程会话的
					// 流式 AI 正文**推给任何一个订阅者（外部审计 D5），必须有门。
					const rejected = bridgeRejection(ctx, req);
					if (rejected !== undefined) {
						res.writeHead(rejected);
						res.end();
						return;
					}
					res.writeHead(200, {
						'content-type': 'text/event-stream; charset=utf-8',
						'cache-control': 'no-store, no-transform',
						connection: 'keep-alive',
						'x-accel-buffering': 'no',
					});
					res.write(': connected\n\n');
					watchers.add(res);
					const heartbeat = setInterval(() => {
						try {
							res.write(': ping\n\n');
						} catch {
							clearInterval(heartbeat);
						}
					}, 15000);
					const cleanup = () => {
						clearInterval(heartbeat);
						watchers.delete(res);
					};
					req.on('close', cleanup);
					req.on('error', cleanup);
				},
			}),
		'dsh-remote-mobile: browser stream route',
	);

	// ---- 启动时恢复：把持久化的远程会话重新接上电脑 ----
	ctx.effect(() => {
		for (const sessionId of remoteSessions) follow(sessionId);
		if (remoteSessions.size > 0) {
			ctx.logger?.info?.(`[dsh-remote-mobile] 已恢复 ${remoteSessions.size} 个远程会话的事件流`);
		}
		// 已配对的话，启动时也把电脑的模型/预设静默拉一次 —— 否则重启 App 后
		// 用户直接打开模型菜单会看到空列表（缓存是内存态，重启即丢）。
		// ⚠️ 不 await：这是后台动作，别拖慢引擎启动。`.catch` 兜住最后一道，
		// 免得一个后台失败变成 unhandled rejection 打脏引擎日志。
		prefetchDesktop('启动').catch(() => {});
		return () => {
			for (const controller of follows.values()) controller.abort();
			follows.clear();
			for (const res of watchers) {
				try {
					res.end();
				} catch {
					/* 已断开 */
				}
			}
			watchers.clear();
		};
	}, 'dsh-remote-mobile: restore + teardown');

	/**
	 * 会话被删除 / 释放时，把本插件挂在它身上的**一切**收干净。
	 *
	 * ## 为什么必须有这段（真 bug，不是洁癖）
	 *
	 * `follow()` 里那个 `run()` 是**永续重连循环**：
	 * ```js
	 * while (!controller.signal.aborted) { … await once() … wait = min(wait*2, 15000) }
	 * ```
	 * 它唯一的退出条件是 `controller.signal.aborted`。而在这之前，
	 * 整个插件里**只有两处**会 abort：`ctx.effect` 的 teardown（插件卸载）
	 * 和 `unpair`。也就是说 ——
	 *
	 * 用户在侧边栏删掉一个远程会话之后，那条 SSE 连接**不会被任何代码停掉**，
	 * 它会以 15 秒一次的节奏**永远**去连电脑上一个已经不存在的会话：
	 * · 每次都是一个完整的 HTTPS 请求（走公网隧道，耗流量、耗电）；
	 * · 电脑端每次都要回一个 404 / 空流，日志被刷；
	 * · 删得越多，这类僵尸连接越多，而且**永远不回收**。
	 *
	 * 官方在会话释放时会 `emit('api-session/removed', session.id)`
	 *（`dsh-api-session-controller/lib/index.js:2753`，由 `session/disposed` 触发），
	 * 监听它就能拿到**准确的**删除时机 —— 比轮询会话列表可靠得多。
	 *
	 * 清理项与 `unpair` 保持同一套（缺一个都会留下不一致）：
	 * ① 连接 —— abort 掉跟随流；
	 * ② 归属 —— 从 `remoteSessions` / `syncedSessions` 里摘掉；
	 * ③ 落盘 —— 否则重启后 `loadRemoteSessions` 又把它们读回来，
	 *    启动时 `for (const id of remoteSessions) follow(id)` 会**重新**建出一堆
	 *    指向已删会话的连接，等于白修。
	 */
	ctx.on('api-session/removed', (sessionId) => {
		if (typeof sessionId !== 'string' || sessionId === '') return;
		const wasRemote = remoteSessions.has(sessionId) || syncedSessions.has(sessionId);
		if (!wasRemote) return;
		const follower = follows.get(sessionId);
		if (follower !== undefined) {
			follows.delete(sessionId);
			try {
				follower.abort();
			} catch {
				/* 已断开 */
			}
		}
		remoteSessions.delete(sessionId);
		syncedSessions.delete(sessionId);
		delete remotePaths[sessionId];
		delete desktopTitles[sessionId];
		reconcileStates.delete(sessionId);
		mirrorStates.delete(sessionId);
		saveRemoteSessions(remoteSessions, ctx.logger, syncedSessions, remotePaths, desktopTitles, turnOffsets, remoteModels);
		ctx.logger?.info?.(`[dsh-remote-mobile] 会话 ${sessionId} 已删除，跟随流与归属记录一并清理`);
	});
}

/**
 * 把电脑路径压成一个**文件名安全**的稳定 slug。
 *
 * 只用于镜像目录的命名（用户看不到），所以要求是「同一个电脑路径永远得到
 * 同一个名字，不同路径几乎不可能撞」。`createHash` 那 8 位十六进制就是防撞的：
 * 纯字符替换会把 `C:\a-b` 与 `C:\a\b` 压成同一个名字。
 *
 * @param desktopPath - 电脑上的绝对路径（win32 语义）。
 * @returns 形如 `c-users-<user>-1a2b3c4d` 的 slug。
 */
function desktopSlug(desktopPath) {
	const flat = desktopPath.replace(/[^a-zA-Z0-9]+/g, '-').replace(/^-+|-+$/g, '').toLowerCase();
	const digest = createHash('sha256').update(desktopPath).digest('hex').slice(0, 8);
	return `${flat === '' ? 'root' : flat}-${digest}`;
}

/**
 * 取电脑路径的末段当工作区标题。
 *
 * ⚠️ **不能直接用 `node:path` 的 `basename`**：手机是 posix 语义，
 * `basename('C:\\Users\\<user>')` 会把整串原样返回（反斜杠不是分隔符），
 * 侧边栏就会显示一长条 `C:\Users\<user>`。这里两种分隔符都切。
 *
 * @param desktopPath - 电脑上的绝对路径。
 * @returns 末段；没有末段（如 `C:\`）时退回去掉尾部斜杠的原串。
 */
function desktopTitle(desktopPath) {
	const trimmed = desktopPath.replace(/[/\\]+$/, '');
	const cut = Math.max(trimmed.lastIndexOf('/'), trimmed.lastIndexOf('\\'));
	const name = cut < 0 ? trimmed : trimmed.slice(cut + 1);
	return name === '' ? desktopPath : name;
}

/**
 * 为电脑上的一个目录，在手机本地建（或复用）一个**同名镜像工作区**。
 *
 * ## 为什么要建工作区，而不是塞进「默认工作区」
 *
 * 前端把会话归到哪个工作区，是**按 cwd 路径精确匹配**的
 * （`dsh-workspace/lib/index.js:102` 的 `sessionIds` getter）。
 * 早先的实现把远程会话的 cwd 设成手机默认工作区的路径，于是**所有**远程会话
 * 都堆在「默认工作区」底下 —— 用户在电脑页签里选了 `C:\Users\<user>`，
 * 侧边栏却显示「默认工作区」，看起来就像「建完又回到默认工作去了」。
 *
 * 正解是给每个电脑目录配一个**独立的手机本地镜像目录**，用它注册一个独立
 * 工作区，标题取电脑目录的末段（`<user>`）。镜像目录是空的、永远不会被写入
 * —— 真正干活的是电脑那台机器，这里只是给前端一个可以挂载的 cwd 锚点。
 *
 * ⚠️ cwd 依然**不能**是电脑路径：Android 是 posix 语义，
 * `path.posix.isAbsolute('C:/Users/...') === false`，而
 * `createOrAdopt()` 里有 `await mkdir(cwd, {recursive:true})` —— 传电脑路径
 * 会真在手机上建出一个叫 `C:\Users\...` 的怪目录。
 *
 * @param ctx - 宿主 Cordis 上下文。
 * @param desktopPath - 电脑上的绝对路径（仅用于取名字，不会成为 cwd）。
 * @returns 工作区实体（含 `.id` / `.path` / `.title`）。
 */
async function ensureDesktopWorkspace(ctx, desktopPath, workspaceMap) {
	const registry = ctx.get?.('workspaceRegistry');
	if (registry === undefined || typeof registry.create !== 'function') {
		throw new Error('workspaceRegistry 不可用，无法为电脑目录建工作区');
	}
	const slug = desktopSlug(desktopPath);
	const dir = join(STATE_DIR, 'workspaces', slug);
	mkdirSync(dir, { recursive: true });
	// 登记 `slug → 电脑路径`，供「备选区直接选镜像工作区」那条路反查（见 WORKSPACES_PATH）。
	if (workspaceMap !== undefined && workspaceMap[slug] !== desktopPath) {
		workspaceMap[slug] = desktopPath;
		saveWorkspaceMap(workspaceMap, ctx.logger);
	}
	return await registry.create(dir, desktopTitle(desktopPath));
}

/**
 * 把一个刚建出来的空会话标记成「非空壳」，让侧边栏肯显示它。
 *
 * 官方 `dsh-client-ui-workspace/lib/client.js:396` 的可见性判据是
 *
 *     session.origin !== "subagent" && !archived.has(session.id) &&
 *     (!session.blank || session.id === current)
 *
 * 也就是**非当前选中的 `blank` 会话一律不渲染**。而 `blank` 只由
 * `sessionListMetadata` 投影算出，投影初值是 `{ blank: true }`
 *（`dsh-api-session-controller/lib/index.js:1782`），只有 `turn/start`
 * 事件能把它翻成 false（同文件 `:1749`：`state.blank && event.type !== "turn/start"`）。
 *
 * 同步来的会话在电脑上可能一条消息都没有（用户只是把它列出来），本地建出来
 * 就是纯空壳 —— 于是 `blank` 恒为 true、侧边栏里永远看不见它。
 *
 * 所以这里补一对**成对的** `turn/start` + `turn/end`：
 *
 * - 只写 `turn/start` 会在日志里留下一个**未闭合的轮次**，
 *   `interruptedTurnClosers()`（`dsh-session/lib/index.js:666`）会把它当成
 *   「被打断的轮次」，前端可能画出一个转圈的占位。
 * - 所以紧跟着补 `turn/end`，`reason.kind` 用 `aborted` —— 语义上诚实
 *   （这不是一次真实对话，没有内容产生），且是 `turnEnds` 的合法取值
 *   （`dsh-agent-loop/lib/index.js:979`）。
 *
 * ⚠️ **必须在 `create()` 之后立刻调用**：会话一旦冷掉，
 * `ctx.sessions.get()` 返回 `undefined`，`append()` 会抛 `session not live`，
 * 而把冷会话重新唤醒的入口（`ApiSessionAgentController`）是 API 层的私有成员，
 * 插件拿不到。新建出来的会话是活的，这是唯一稳妥的时机。
 *
 * @param ctx - Cordis 上下文。
 * @param sessionId - 刚建出来的会话 id。
 * @param title - 电脑上那个对话的标题（可选）。
 *   传了就把标题也写成一条 `session/title` 事件，这样侧边栏显示的是**真实名字**
 *   而不是「未命名」。见下面的注释。
 * @param wake - 可选的唤醒参数 `{ workspaceId?, cwd? }`。
 *   会话**冷掉**时（App 重启后，`ctx.sessions.get()` 返回 `undefined`），
 *   光拿不到会话对象就写不进去。传了这个参数就先走一次
 *   `sessionController.create({sessionId, …})` 把它重新载入内存再写 ——
 *   官方那条链路对已存在的会话是**幂等**的（`ensureSession` 看到活会话直接
 *   返回、看到已持久化的会话会 adopt 并重放日志，见
 *   `dsh-api-session-controller/lib/index.js:697` → `agents.ensureSession`）。
 *
 *   ⚠️ **不传 `wake` 时行为与改动前完全一致**（冷会话直接返回 false），
 *   所以老调用点不受影响。
 * @param liveSession - 可选的**已查到的**会话对象。
 *   调用方如果刚刚 `ctx.sessions.get(id)` 拿到了非 undefined 的值，就应该把它
 *   传进来。理由：`ctx.sessions.get()` 在两次调用之间**可能变成 undefined**
 *   （会话被卸载/切走），而调用方那次判断是「已存在」、走到这里却查不到 ——
 *   于是白跑一趟 create（对已持久化的会话还会撞 corrupt，见真机日志），
 *   标题一条都没写进去。直接把对象传下来就没有这个竞态。
 * @param rememberTurnOffset - 可选回调 `(sessionId, offset) => void`。
 *   写入占位轮次后调用，让调用方把「这个会话的电脑端轮次要整体 +N」
 *   落盘（见 `loadTurnOffsets`）。不传则只写日志不记账 —— 那样后续
 *   电脑端真实事件会撞上占位轮次，所以正式路径必须传。
 * @returns 是否真的写进去了（失败不影响同步本身，只影响可见性）。
 */
async function markSessionNonBlank(ctx, sessionId, title, wake, liveSession, rememberTurnOffset) {
	let session = liveSession !== undefined && liveSession !== null ? liveSession : ctx.sessions?.get?.(sessionId);
	const diag = {
		at: Date.now(),
		sid: typeof sessionId === 'string' ? sessionId.slice(8, 16) : '',
		title: typeof title === 'string' ? title.slice(0, 20) : '',
		cold: session === undefined,
		woke: false,
		hasAppend: false,
		wrote: false,
		error: '',
	};
	// ⚠️ **冷会话先唤醒**（见上面 `wake` 的注释）。
	//
	// 这是「重新同步也补不上标题」的根因：用户重新同步那批老会话时，
	// 它们在内存里已经不在了（`ctx.sessions.get()` 返回 undefined），
	// 于是这里直接 return false —— 日志一条都没写，文件时间戳纹丝不动，
	// 侧边栏继续显示「未命名」。
	if (session === undefined && wake !== undefined && typeof ctx.sessionController?.create === 'function') {
		try {
			const request = { sessionId };
			if (typeof wake.workspaceId === 'string' && wake.workspaceId !== '') request.workspaceId = wake.workspaceId;
			else if (typeof wake.cwd === 'string' && wake.cwd !== '') request.cwd = wake.cwd;
			await ctx.sessionController.create(request);
			session = ctx.sessions?.get?.(sessionId);
			diag.woke = session !== undefined;
		} catch (error) {
			diag.error = `wake: ${String(error?.message ?? error)}`.slice(0, 160);
			ctx.logger?.warn?.(
				`[dsh-remote-mobile] 唤醒会话 ${sessionId} 失败（不影响同步）: ${String(error?.message ?? error)}`,
			);
			session = undefined;
		}
	}
	diag.hasAppend = session !== undefined && typeof session.append === 'function';
	if (session === undefined || typeof session.append !== 'function') {
		markDiag.push(diag);
		if (markDiag.length > 30) markDiag.shift();
		return false;
	}
	try {
		// ## 顺手把**标题**写上（用户要求：同步过来的对话不要显示「未命名」）
		//
		// 标题投影是**纯日志重放**出来的：
		//   `dsh-session-title/lib/index.js:177`
		//   `apply: (state, event) => event.type === "session/title" ? event.data.title : state`
		// 也就是「日志里最后一条 `session/title` 事件的 `data.title`」。
		//
		// 而同步会话在手机本地是**空壳**（历史按需从电脑拉，见
		// `syncConversations` 的注释），日志里一条 `session/title` 都没有
		// ⇒ 投影是 `init: () => null` ⇒ 前端 `sessionTitle()` 返回空串
		// ⇒ 侧边栏回退成 i18n 的「未命名」。
		//
		// 所以补一条。形状照抄官方 `rename()`（同文件 `:302`）：
		//   `{ title, messageSeqs: [], source: { kind: "user" } }`
		// - `messageSeqs: []`：这条标题不是从某条消息生成的，没有可引用的消息；
		// - `source.kind: "user"`：**只能是 user**。自动标题那条路
		//   （`source.kind: "provider"` / `"fallback"`）会带 `messageSeqs`
		//   并参与「首条消息触发一次标题生成」的判定，写错了会让电脑端
		//   后续的自动标题行为被误判。
		//
		// ⚠️ `title` 必须是**非空字符串**：`titleViewSchema` 是
		// `z.string().min(1).nullable()`，空串会让投影校验失败。
		// 电脑上没标题的对话（`（未命名对话）`）就跳过，让它继续显示「未命名」。
		//
		// ## ⚠️⚠️ 幂等：已经写好过就**不要再写一遍**
		//
		// 用户重复同步同一批对话是常态（「我勾上这几个」→ 隔天再勾一次）。
		// 而这个函数每调一次就 append **三条**事件。不设闸的话，日志会随
		// 同步次数线性增长，每个会话堆出一串没人读的空轮次，投影也要多跑几轮。
		//
		// 判据看本地日志现状，两条都满足才跳过：
		//   1. 最后一条 `session/title` 的标题**已经等于**这次要写的；
		//   2. 日志里已经有 `turn/start`（= 早就不是空壳了，`blank` 已是 false）。
		// 只要有一条不满足就照常补 —— 那说明确实还需要写。
		const log = Array.isArray(session.log) ? session.log : [];
		let lastTitle = '';
		/** 日志里是否已经有**成对闭合**的占位轮次（`turn/start {1}` + `turn/end {1}`）。 */
		let hasPlaceholder = false;
		/** 老版本留下的非法标记（`turn/start {999999}`），有就得修。 */
		let hasLegacyMarker = false;
		/** 日志里出现过任何 `turn/start`（= `blank` 已经是 false）。 */
		let hasTurn = false;
		for (let at = 0; at < log.length; at += 1) {
			const entry = log[at];
			if (entry === null || entry === undefined) continue;
			if (entry.type === 'turn/start') {
				hasTurn = true;
				if (entry.data?.turn === LEGACY_MARKER_TURN) hasLegacyMarker = true;
				if (entry.data?.turn === PLACEHOLDER_TURN) {
					// 有 start 还得有配对的 end 才算数（否则是半个轮次）。
					for (let at2 = at + 1; at2 < log.length; at2 += 1) {
						const later = log[at2];
						if (later?.type === 'turn/end' && later.data?.turn === PLACEHOLDER_TURN) {
							hasPlaceholder = true;
							break;
						}
					}
				}
			}
		}
		// 标题取**最后一条** `session/title`。
		for (let at = log.length - 1; at >= 0; at -= 1) {
			const entry = log[at];
			if (entry?.type === 'session/title') {
				lastTitle = typeof entry.data?.title === 'string' ? entry.data.title : '';
				break;
			}
		}
		const wantedTitle = typeof title === 'string' ? title.trim() : '';
		// ⚠️ 老版本写进去的 `turn/start {999999}` 是**非法事件**，会让这个会话
		// 永远打不开（见 `LEGACY_MARKER_TURN` 的注释）。
		//
		// ## 为什么这里只标记、不修
		//
		// 修它必须**整份重写会话日志**（删掉那条非法事件并重排 seq）。
		// 而日志是 `session.v4.jsonl.zstd`，官方要求**多帧结构**：
		// 第 1 帧恰好是那一行 header，后续帧才是事件
		//（校验点 `dsh-session-persistence-jsonl/lib/index.js:2298`
		//  `assertZstdHeaderFrame`）。用 `zstd -f -o` 整体重压成单帧会得到：
		//
		//     corrupt Zstandard session log: first frame is not exactly one header line
		//
		// 在插件里重做这套多帧写入、还要和官方正在持有的会话对象保持一致，
		// 风险远大于收益 —— 写坏了就是把用户的历史彻底毁掉，而这里**无法回滚**。
		//
		// 所以：检测到就只记一笔，让上层把「这个会话需要修复」报给用户
		//（见 `syncConversations` 的 `failures`）。真正的修法是由用户/作者
		// 在停机状态下用官方工具处理，而不是在运行中的插件里盲改。
		if (hasLegacyMarker) {
			diag.error = 'legacy-marker';
			ctx.logger?.warn?.(
				`[dsh-remote-mobile] 会话 ${sessionId} 含老版本写入的非法占位轮次（turn: ${LEGACY_MARKER_TURN}），` +
					`官方校验会判定 corrupt，需要修复日志后才能打开`,
			);
		}
		// ⚠️ 幂等判据：标题已经是想要的 + 已经有**合法的**成对占位轮次。
		if (wantedTitle !== '' && lastTitle === wantedTitle && hasPlaceholder) {
			diag.error = diag.error === '' ? 'already' : diag.error;
			diag.wrote = false;
			markDiag.push(diag);
			if (markDiag.length > 30) markDiag.shift();
			return false;
		}
		if (wantedTitle !== '' && lastTitle !== wantedTitle) {
			session.append('session/title', {
				title: wantedTitle,
				messageSeqs: [],
				source: { kind: 'user' },
			});
		}
		// ## ⚠️⚠️ 占位轮次：`turn: 1`，而且**必须成对闭合**
		//
		// 官方校验器要求 turn 编号从 1 开始严格连续（见 `PLACEHOLDER_TURN`
		// 的注释，含三代方案的踩坑史）。所以这里写的是：
		//
		//     turn/start { turn: 1 }
		//     turn/end   { turn: 1 }
		//
		// 完全合法，校验器一句话都挑不出来。
		//
		// 代价是**占用了本地的 turn 1**，而电脑端给这个会话派的第一个真实轮次
		// 也是 1 ⇒ 必须让这个会话的电脑端轮次整体 +1（`turnOffsets`，
		// 见 `loadTurnOffsets` 与 `mirrorEvent`）。
		//
		// ⚠️ 只有当本地**还没有任何** `turn/start` 时才写 —— 已经有过真实轮次的
		// 会话（用户在手机上聊过）不该再塞一个假轮次进去，那会直接撞车。
		if (!hasPlaceholder && !hasTurn) {
			session.append('turn/start', { turn: PLACEHOLDER_TURN });
			session.append('turn/end', { turn: PLACEHOLDER_TURN });
			// 记下偏移：这个会话的电脑端轮次从此刻起整体 +1。
			rememberTurnOffset?.(sessionId, 1);
		}
		// ⚠️ **写完主动推一次 `api-session/added`，让前端立刻拿到新标题。**
		//
		// 标题投影是「日志里最后一条 `session/title`」，写进去之后**服务端**是对的
		//（`session/list` 立刻能读到新标题，投影缓存也正确），但**前端侧边栏
		// 不会自己变** —— 它手里那份摘要（`api-session/added` 推过去的
		// `summaryFor(session)`）是旧的。
		//
		// 真机表现：重新同步之后 `session/list` 明明返回「防用户插件崩溃方案」，
		// 侧边栏却还写着「未命名」，要等下一次刷新/重启才恢复。
		//
		// 官方自己的做法就是这个：`dsh-api-session-controller/lib/index.js:2874`
		// 在 `session/created` 时 `ctx.emit("api-session/added", summaryFor(session))`。
		// 我们改了摘要内容，就得重发一次同样的消息。
		//
		// ⚠️ 包在 try 里：拿不到摘要服务（版本差异）不该让同步失败 ——
		// 最坏只是前端晚一步更新，标题本身已经落盘了。
		try {
			const summary = ctx.sessionController?.listState?.summaryFor?.(session);
			if (summary !== undefined && summary !== null) ctx.emit('api-session/added', summary);
		} catch (emitError) {
			ctx.logger?.warn?.(
				`[dsh-remote-mobile] 推送会话 ${sessionId} 的新摘要失败（前端会晚一步更新）: ${String(
					emitError?.message ?? emitError,
				)}`,
			);
		}
		diag.wrote = true;
		markDiag.push(diag);
		if (markDiag.length > 30) markDiag.shift();
		return true;
	} catch (error) {
		diag.error = String(error?.message ?? error).slice(0, 160);
		markDiag.push(diag);
		if (markDiag.length > 30) markDiag.shift();
		ctx.logger?.warn?.(
			`[dsh-remote-mobile] 标记会话 ${sessionId} 为非空失败（不影响同步）: ${String(error?.message ?? error)}`,
		);
		return false;
	}
}

/**
 * 从镜像目录反查电脑路径。
 *
 * @param workspaceMap - `slug → 电脑路径`。
 * @param dir - 会话的 cwd（应当落在 `STATE_DIR/workspaces/<slug>` 下）。
 * @returns 电脑路径；不是镜像目录时 `undefined`。
 */
function desktopPathOfMirror(workspaceMap, dir) {
	if (typeof dir !== 'string' || dir === '') return undefined;
	// ⚠️ 用 `/workspaces/` 定位而不是拿 `join(STATE_DIR,'workspaces')` 做前缀比对：
	// `workspace.path` 是 **realpath**（官方 `createOrAdopt` 会 realpath 解析），
	// 在真机上可能多出/少掉一层符号链接（`/data/user/0` vs `/data/data`），
	// 前缀一比就永远不命中 —— 症状是「修了但没生效」，极难查。
	//
	// 分隔符两种都认：真机是 posix（`join` 给 `/`），但把本插件装到 Windows 上
	// 调试时 `join` 给的是 `\`，只认一种就会在那边静默失效。
	const at = dir.search(/[\\/]workspaces[\\/]/);
	if (at < 0) return undefined;
	const slug = dir.slice(at).split(/[\\/]/)[2];
	if (slug === undefined || slug === '') return undefined;
	const path = workspaceMap[slug];
	return typeof path === 'string' && path !== '' ? path : undefined;
}

/**
 * 处理浏览器桥来的一条 RPC。
 *
 * @param ctx - Cordis 上下文。
 * @param config - 远程配置。
 * @param io - `{ remoteSessions, follow, follows, mirrorStates, persist, workspaceMap, body }`。
 */
async function handleBridge(ctx, config, io) {
	const params = io.body?.params ?? {};
	switch (io.body?.method) {
		/** 连接自检：手机 → 电脑。 */
		case 'ping':
			return callRemote(config, 'ping', {});

		/**
		 * 列电脑上的目录。
		 *
		 * ⚠️ **命中缓存就直接回，不打隧道**。两级缓存：
		 *
		 * ① 根层（`desktopCache.directory`）—— 用户点开「电脑」页签时看的正是
		 *    这一层，每一轮往返都要穿过 Cloudflare 隧道，那就是用户抱怨的
		 *    「还是有一个加载过程」（原话：「目录树都提前存档手机本地了，
		 *    为什么还是还加载呢，不能预加载吗」）。
		 * ② 子层（`desktopCache.dirs`）—— 用户报障原文「你现在好像只缓存了
		 *    顶级目录，打开某个文件夹还是要加载」。
		 *
		 * 两级都只在**拉成功之后**写回，并且**每次返回前都顺手预取「下一层」**
		 * （见 `prefetchDirs`）：用户看列表、决定点哪个，这一两秒足够把下一层
		 * 拉回来，点下去时就已经在本地了。
		 */
		case 'listDesktop': {
			const wantRoot = params.path === undefined || params.path === null || params.path === '';
			const path = wantRoot ? '' : params.path;
			// ⚠️ 读缓存也要过 `isSafeCacheKey`（外部审计 5.2e）：`dirs['__proto__']`
			// 在无原型对象上是 `undefined`，但在**普通对象**上会返回 `Object.prototype`
			// ⇒ 缓存命中一个不是列表的东西。这里两个条件都要求。
			const cached = wantRoot
				? io.desktopCache?.directory
				: isSafeCacheKey(path)
					? io.desktopCache?.dirs?.[path]
					: undefined;
			if (cached != null) {
				// 命中也要预取下一层：用户还没离开这一屏。
				io.prefetchDirs?.(cached);
				return cached;
			}
			const listing = await callRemote(config, 'listDirectory', { path: params.path });
			if (io.desktopCache !== undefined) {
				if (wantRoot) io.desktopCache.directory = listing;
				else if (typeof path === 'string' && path !== '') io.rememberDir?.(path, listing);
			}
			io.prefetchDirs?.(listing);
			return listing;
		}

		/**
		 * 电脑上的模型目录 —— **只读本地缓存，不联网**。
		 *
		 * 用户要的是「选了电脑目录之后，输入框右下方的模型列表就刷新为电脑的」。
		 * 手机本地那份模型列表来自手机自己的 LLM 配置，跟电脑上能跑什么没关系，
		 * 拿它去发消息会选到一个电脑上不存在的模型。
		 *
		 * ⚠️⚠️ **这里从前是 `return callRemote(config, 'modelCatalog', {})`，
		 * 每次前端读一次就过一趟公网隧道。** 用户原话：「我选择了电脑工作区之后，
		 * 虽然说模型列表已经显示的是电脑的了，但是我如果展开列表，然后切换一个
		 * 模型的话，此时会转圈圈的。如果是本地读取的话，完全没必要转圈圈」。
		 *
		 * 现在改为：**有缓存直接给，没有才拉一次并落盘**。电脑的模型配置极少变，
		 * 要更新由用户自己点「刷新」（走 `refreshAll`，那条路仍然联网）。
		 */
		case 'modelCatalog': {
			// ⚠️ `handleBridge` 是**模块级**函数，看不到 `apply()` 闭包里的
			// `desktopCache` / `desktopCatalog` / `persistDesktopCache`。
			// 全都要经 `io` 传进来 —— 直接引用会抛
			// `desktopCache is not defined`，而且这条分支是**模型选择列表**的
			// 数据源，一挂就等于手机上模型列表刷不出来。
			//
			//（同一个坑在本文件里踩过三次：`pendingPlans`、`desktopCache`、
			// `syncedSessions` —— 新增闭包变量时**必须**同步加进 `io` 对象。）
			const cache = io.desktopCache;
			if (cache === undefined) return null;
			if (cache.catalog !== null) return cache.catalog;
			const fetched = await io.desktopCatalog?.();
			if (fetched !== null && fetched !== undefined) io.persistDesktopCache?.();
			return fetched ?? null;
		}

		/**
		 * 电脑上的预设列表 —— **只读本地缓存，不联网**（理由同 `modelCatalog`）。
		 *
		 * 没有缓存的第一次仍会拉一次，之后一直用本地的。电脑下线后也不会再
		 * 反复打失败的请求；用户切回手机工作区，读到的仍是这份本地快照，
		 * 由前端按工作区筛选（见 `pickWorkspacePresets`）。
		 */
		case 'presetList': {
			const cache = io.desktopCache;
			if (cache === undefined) return null;
			if (cache.presets !== null) return cache.presets;
			try {
				const fetched = await callRemote(config, 'presetList', {});
				if (fetched !== null && fetched !== undefined) {
					cache.presets = fetched;
					cache.at = Date.now();
					io.persistDesktopCache?.();
				}
				return fetched;
			} catch (error) {
				ctx.logger?.warn?.(
					`[dsh-remote-mobile] 取电脑预设列表失败: ${String(error?.message ?? error)}`,
				);
				return null;
			}
		}

		/**
		 * 一键刷新：把「模型 / 预设 / 目录树」三样一起重新拉一遍。
		 *
		 * ⚠️ 三样**必须并发**且**互不牵连**：目录树拉不到（比如电脑上那个目录被删了）
		 * 不该让模型列表也刷不出来。所以逐个 catch，谁失败谁自己带 `error`。
		 *
		 * @returns `{ models, presets, directory }`，每项是 `{ok, value|error}`。
		 */
		case 'refreshAll': {
			const wantDirectory = typeof params.path === 'string' && params.path !== '';
			const jobs = [
				['models', () => callRemote(config, 'modelCatalog', {})],
				['presets', () => callRemote(config, 'presetList', {})],
				...(wantDirectory ? [['directory', () => callRemote(config, 'listDirectory', { path: params.path })]] : []),
			];
			const settled = await Promise.all(
				jobs.map(async ([key, run]) => {
					try {
						return [key, { ok: true, value: await run() }];
					} catch (error) {
						return [key, { ok: false, error: String(error?.message ?? error) }];
					}
				}),
			);
			const result = Object.fromEntries(settled);
			// ⚠️ 刷新到的模型/预设要**写回缓存**，否则「刷新按钮」只更新了它自己
			// 那份响应，前端模型菜单读的是宿主拦截器里的缓存 —— 用户会看到
			// 「点了刷新，模型列表没变」。
			//
			// 根目录那一层同理：刷完不回写的话，用户接下来点开「电脑」页签
			// 命中的还是**刷新前**那份预拉缓存，看起来就是「刷新没生效」。
			//
			// ⚠️ **判据是「拉回来的这一层就是缓存的那一层」**（同 path），
			// 不是「有没有带 path」：前端在根层点刷新时传的是当前显示的绝对路径
			// （`loc.path`，也就是 home），照样是带 path 的一次调用 —— 只看
			// `params.path` 空不空会把根层刷新漏掉。
			if (io.desktopCache !== undefined) {
				if (result.models?.ok === true) io.desktopCache.catalog = result.models.value;
				if (result.presets?.ok === true) io.desktopCache.presets = result.presets.value;
				// ⚠️ **刷新不再把子层缓存整棵作废**（原来是 `emptyDirCache()`）。
				//
				// 作废的代价是用户点完刷新之后，**每一次点文件夹都要重新过隧道**
				// （实测 700～1200ms），而重新预取 100 多个子目录要好几秒 ——
				// 用户实测原话：「我先是点了一下刷新，刷新成功之后，我再点击某个
				// 文件夹，它里面的内容还是要加载」。这正是那条 `emptyDirCache()`
				// 造成的：刷新的瞬间把用户已经攒下的全部缓存清零了。
				//
				// 现在改成**保留旧子层、由下面那次 `prefetchDirs` 覆盖更新**：
				// 点进去先命中上一次的内容（秒开），新的那份在后台拉回来替换。
				// 用户要的是「不转圈」，而不是「每个子目录都必须是这一毫秒的」。
				if (
					result.directory?.ok === true &&
					io.desktopCache.directory?.path === result.directory.value?.path
				) {
					io.desktopCache.directory = result.directory.value;
				}
				io.desktopCache.at = Date.now();
				// ⚠️ 手动刷新是**唯一**会把新数据写进本地快照的路径 —— 平时读的都是
				// 这份快照（见 `modelCatalog` / `presetList` 两个 case）。不落盘的话，
				// 刷新只对本次运行有效，重开 App 又回到旧数据。
				io.persistDesktopCache?.();
				// 刷新完顺手把这一层的下一层预取回来，用户点进去就不用等了。
				if (result.directory?.ok === true) io.prefetchDirs?.(result.directory.value);
			}
			return result;
		}

		/**
		 * 在电脑的某个目录下开一个新会话。
		 *
		 * 两端用**同一个 sessionId**：手机本地建会话（用手机 cwd，见文件头），
		 * 电脑端在它自己的 cwd 下建同名会话。之后事件就能直接互搬。
		 */
		case 'createSession': {
			if (typeof params.path !== 'string' || params.path === '') {
				throw new Error('请先在电脑页签里选一个目录');
			}
			if (config.token === '') {
				throw new Error('还没配对：请先在「电脑」页签里填配对码');
			}
			const sessionId = `session-${randomUUID()}`;
			// ⚠️ 手机端 cwd 必须是**手机本地**的绝对路径，绝不把电脑路径传进来
			// （Android 是 posix 语义，`path.posix.isAbsolute('C:/...') === false`，
			// 且官方 createOrAdopt 会 `mkdir(cwd, {recursive:true})` 真去建目录）。
			//
			// 这里给它一个**电脑目录专属的镜像工作区**（标题 = 电脑目录末段），
			// 而不是手机的「默认工作区」—— 否则所有远程会话都堆在「默认工作区」
			// 底下，看起来就像「建完又回到默认工作去了」（见 `ensureDesktopWorkspace`）。
			const workspace = await ensureDesktopWorkspace(ctx, params.path, io.workspaceMap);
			const cwd = workspace.path;
			// ⚠️ **必须走 `sessionController.create`，不能走 `ctx.sessions.create`。**
			//
			// `ctx.sessions.create()` 只把会话塞进内存 store：持久化层既没
			// `registerCreated()` 也没 `adopt()` 一个写句柄，于是日志**永远不落盘**，
			// 而前端会话列表读的是 `sessionQuery.listSessions()`（磁盘 + pending），
			// 因此列表里看不到这个会话、`openSession()` 也就跳不过去 —— 表现是
			// 「电脑端建了会话，手机界面却停在『选择一个工作区开始』」。
			//
			// 官方链路 `sessionController.create` → `agents.ensureSession` →
			// `agents.create` → `sessionPersistence.create` 才会真正建写句柄并落盘。
			//
			// ⚠️ 这**不会**让手机跑本地模型：远程会话的 `prompt` 被我们拦下来转发电脑
			// （见上面的 prompt/cancel 拦截），这里的 agent 只是个空壳。
			if (typeof ctx.sessionController?.create !== 'function') {
				throw new Error('sessionController.create 不可用，无法在手机本地建会话');
			}
			await ctx.sessionController.create({ sessionId, cwd });
			// ⚠️ 建完必须**挂到工作区**上，否则前端看不到这个会话。
			//
			// 归属判据是 `dsh-workspace` 的 `sessionIds` getter：
			// `record.sessionIds.filter(id => host.sessionPath(id) === record.path)`。
			// 而 `sessionPaths` 索引只在注册表启动时建一次（`indexHeader`），
			// 新建的会话不在索引里 ⇒ `sessionPath(id)` 是 `undefined` ≠ `record.path`
			// ⇒ 被 getter 过滤掉：侧边栏没有它、composer 判成「没选工作区」而禁用。
			//
			// `attachSession()` 会 `rememberSessionPath()` 把这条补进索引，并写进
			// `record.sessionIds` —— 正是官方「添加工作区」那条路的落点。
			// 它要求 `header.cwd` 的 realpath **等于**工作区 path，所以上面的 cwd
			// 必须取自同一个工作区实体，不能是 `process.cwd()`。
			//
			// ⚠️ 失败不致命（最坏是回到「会话在盘上、界面看不见」的旧症状），
			// 所以只记日志，不让整个创建失败。
			if (workspace !== undefined && typeof workspace.attachSession === 'function') {
				try {
					await workspace.attachSession(sessionId);
				} catch (error) {
					ctx.logger?.warn?.(
						`[dsh-remote-mobile] 会话 ${sessionId} 挂到工作区失败（界面可能看不到它）: ${String(error?.message ?? error)}`,
					);
				}
			}
			// ⚠️ **这里不再联网建会话**（用户三轮反馈后的定稿）。
			//
			// 原来这里 `await callRemote(config, 'createSession', ...)` —— 那是
			// 「选目录」和「建对话」被绑在一起留下的痕迹。官方在电脑上同样是
			// **发了消息才 create**，选工作区只是面板上的一个状态。
			//
			// 现在只登记一份「待发送计划」，等用户在 `prompt` 里真的发消息时，
			// 把 `desktopPath` / `presetId` / `model` 打包进**同一个** prompt 请求
			// 发过去（见 `prompt` 拦截器与 `pendingPlans` 的注释）。
			//
			// 好处是双重的：
			//
			// 1. 选目录这一步**零延迟**（纯内存登记，不等隧道）；原来那趟隧道
			//    往返实测 1～3 秒，而选择器早就收起了，用户只能干等；
			// 2. 从「选目录 → 发第一条消息」全程只跟电脑沟通**一次**。
			//
			// 而且失败面变好了：原来电脑离线要等到选目录之后才知道，界面上
			// 已经收了选择器、没地方提示；现在电脑离线是在**发送那一刻**报错，
			// 用户正看着输入框，能正常看到「发不出去」。
			//
			// `presetId` 取自请求时官方 create 并不会带（`createSession` 桥只收
			// `path`），所以这里留空，真正发消息时由 `presetIdOf()` 读手机本地
			// 会话投影上的**最新**值（见 `prompt` 拦截器）。
			//
			// ⚠️ **镜像工作区必须在这里就建好**（上面 `ensureDesktopWorkspace`
			// 已经做了）：它负责把 `slug → 电脑路径` 登记进 `workspaceMap`，
			// 而 `desktopPathOfMirror()` 反过来靠这份登记认人。少了它，
			// `prompt` 时反查不出电脑路径，整套远程逻辑就静默失效。
			// ⚠️ 用 `io.pendingPlans`：这个函数是**模块级**的，看不见 `apply()` 闭包里的
			// `pendingPlans`（1.0.64 因此报 `pendingPlans is not defined`，整条
			// 「选目录」链路静默失败）。与 `io.workspaceMap` 是同一个坑。
			io.pendingPlans.set(sessionId, { desktopPath: params.path });
			io.remoteSessions.add(sessionId);
			// ⚠️ **落盘电脑路径**（见 `loadRemotePaths`）：`pendingPlans` 是内存的，
			// App 重启后清空，那时发消息只能靠这份映射反解。
			io.rememberPath?.(sessionId, params.path);
			io.persist();
			// ⚠️ **事件流先不开**：会话在电脑上还不存在，此时连 SSE 只是挂一条
			// 空连接，白白占着隧道的并发。`prompt` 里发完消息才 `follow()`。
			ctx.logger?.info?.(
				`[dsh-remote-mobile] 已在手机本地建好会话 ${sessionId}（cwd 取镜像工作区），` +
					`电脑上的会话等第一条消息时再建：${params.path}`,
			);
			return { sessionId, cwd, desktopPath: params.path };
		}

		/** 手机本地会话（供前端判断某个会话是不是远程会话）。 */
		/**
		 * 诊断：`prompt` 拦截器的命中记录。
		 *
		 * 只读，不改状态、不联网。
		 */
		case 'debugPrompt':
			return { items: io.promptDiag.slice() };

		/**
		 * 诊断：运行态派发记录（`deriveRunning` 每次 emit 前记一条）。
		 *
		 * 排查「同步来的会话没有『深度求索中 / 停止生成』」专用：
		 * 有记录 ⇒ 派发走到了，问题在转发到浏览器；
		 * 没记录 ⇒ 根本没走到，问题在事件流（`followSynced` 没被调用）。
		 *
		 * 只读，不改状态、不联网。
		 */
		case 'debugRunning':
			return { items: runningDiag.slice() };

		/**
		 * 诊断：`markSessionNonBlank` 的写入记录。
		 *
		 * 排查「重新同步也补不上标题 / 侧边栏仍显示未命名」专用。
		 * 判读见 `markDiag` 的注释。
		 *
		 * 只读，不改状态、不联网。
		 */
		case 'debugMark':
			return { items: markDiag.slice() };

		/**
		 * 诊断：`controller.page` 拦截器的分页参数记录。
		 *
		 * 排查「历史加载失败：page did not end at its requested cursor」专用。
		 * 每条含前端要的 `throughSeq`（`want`）与电脑实际返回的 `tail`，
		 * 两者不等就是分页边界错位。
		 *
		 * 只读，不改状态、不联网。
		 */
		case 'debugPage':
			return { items: io.pageDiag.slice() };

		case 'isRemote':
			return { remote: io.remoteSessions.has(params.sessionId) };

		/**
		 * 诊断：某会话在**手机本地**到底存不存在，以及它的待发送计划。
		 *
		 * 排查「发消息报 `session not found`」时用。`remoteSessions` 里有它、但
		 * `ctx.sessions.get()` 里没有，就说明本地会话没建成功 —— 那是
		 * `createSession` 桥或 `controller.create` 拦截器的问题，不是转发的问题。
		 *
		 * ⚠️ 只读，不改任何状态；不联网。
		 */
		case 'debugSession': {
			const sid = params.sessionId;
			const local = ctx.sessions?.get?.(sid);
			const plan = io.pendingPlans.get(sid);
			return {
				remote: io.remoteSessions.has(sid),
				synced: io.syncedSessions.has(sid),
				following: io.follows.has(sid),
				followsTotal: io.follows.size,
				remoteTotal: io.remoteSessions.size,
				syncedTotal: io.syncedSessions.size,
				localExists: local !== undefined,
				localCwd: local?.header?.cwd ?? null,
				planDesktopPath: plan?.desktopPath ?? null,
				planPresetId: plan?.presetId ?? null,
				planModel: plan?.model ?? null,
				workspaceCount: io.workspaceMap instanceof Map ? io.workspaceMap.size : null,
			};
		}

		/**
		 * 全量远程会话清单（一次性给前端，前端拿它在**本地**做同步判定）。
		 *
		 * ⚠️ **为什么需要一个「全量」的桥**：前端要把模型菜单按「当前会话是不是
		 * 电脑镜像工作区」隔离（用户需求原文：「用户如果当前选择的是 app 本地
		 * 文件夹，那么就不要出现电脑的模型，以这个文件夹为隔离条件，互不干预」）。
		 * 而模型菜单每渲染一帧都会读一次目录快照，**不可能每帧打一次桥** ——
		 * 那是同步 API。所以前端在 `apply` 时拉一次全量，之后建会话/解绑时
		 * 自己增量维护，`getSnapshot` 就纯查内存。
		 *
		 * 重启 App 后已有会话也能正确识别，正是靠这一条：宿主从
		 * `sessions.json` 恢复出 `remoteSessions`，前端一次性拿走。
		 */
		case 'remoteSessions':
			return { ids: [...io.remoteSessions] };

		/**
		 * 电脑镜像工作区的 slug 集合（`workspaceMap` 的 keys）。
		 *
		 * ⚠️⚠️ **这是「模型列表按工作区隔离」的第二代判据，用户 1.0.19 报障后的正解。**
		 *
		 * 上一代用 `remoteSessions`（按 **sessionId**）当判据，漏掉了这条真实路径：
		 * 用户在侧边栏点「新建会话」时，官方前端直接
		 * `sessionController.create({workspaceId})` —— **不经过本插件的桥**。
		 * 宿主的 create 拦截器认得出镜像目录（所以消息发得对），但**前端不知道**，
		 * 于是模型菜单仍按「本地」筛，显示手机模型。
		 *
		 * 用户原话：「只有我手动添加工作区的时候，才会触发模型和预设的刷新」——
		 * 一针见血：判据挂在了「走了哪条路」上，而不是「会话的工作区在哪」。
		 *
		 * 正解是**按 cwd 判定**，与宿主 `desktopPathOfMirror` 同一个轴：
		 * 前端从 `sessions.list` 的会话行里**同步**读到 cwd，看它是不是
		 * `<STATE_DIR>/workspaces/<slug>` 且 `slug` 在这里面。这条路
		 * **与「谁建的会话」完全无关**，所以任何入口进来都一致。
		 *
		 * 只回 slug 列表而不是整张 `workspaceMap`：前端不需要电脑路径，
		 * 少传一份数据也少一处可能不一致的地方。
		 */
		case 'mirrorSlugs':
			return { slugs: Object.keys(io.workspaceMap ?? {}) };

		/**
		 * 镜像工作区清单：`[{ slug, path, desktopPath, title }]`。
		 *
		 * ## 为什么前端需要它（用户需求一）
		 *
		 * 「同步对话」这个菜单项挂在**工作区行**上，点了之后要先知道
		 * 「这个工作区对应电脑上的哪个目录」，才能去问电脑要历史对话列表。
		 *
		 * 前端手里只有**手机侧**的工作区路径（`<STATE_DIR>/workspaces/<slug>`），
		 * 电脑路径在宿主的 `workspaceMap` 里。所以把两者一起下发，
		 * 前端按 `path` 匹配就能拿到 `desktopPath`。
		 *
		 * 只下发镜像工作区（用户选过的电脑目录），本地工作区不在其中。
		 */
		case 'mirrorWorkspaces': {
			const map = io.workspaceMap ?? {};
			const items = [];
			const registry = ctx.get?.('workspaceRegistry');
			for (const slug of Object.keys(map)) {
				const path = join(STATE_DIR, 'workspaces', slug);
				// ⚠️ 带上 `workspaceId`：前端靠它把「菜单点的是哪一行」精确对上。
				//
				// 只用标题匹配不可靠 —— `desktopTitle()` 取电脑路径的末段，
				// `C:\a\proj` 与 `D:\b\proj` 都叫 `proj`，用户点第二行却可能
				// 同步到第一个目录的对话。而官方前端**不在 DOM 上暴露**
				// workspaceId（`ProjectRowItem` 只有 `role="treeitem"` + 文本），
				// 所以前端拿不到更细的判据。
				//
				// 解决方式：把 `workspaceId` 也下发，前端在轮询时给每一行打上
				// `data-dsh-workspace-id`，pointerdown 时直接读那个属性 ——
				// 精确、与标题无关。
				let workspaceId = null;
				try {
					const entity = await registry?.resolveByPath?.(path);
					if (entity !== undefined && typeof entity.id === 'string') workspaceId = entity.id;
				} catch {
					// 工作区还没建（用户没点过这个目录）就当没有 id，前端退回标题匹配。
				}
				items.push({
					slug,
					desktopPath: map[slug],
					path,
					title: desktopTitle(map[slug]),
					workspaceId,
				});
			}
			return { workspaces: items };
		}

		/**
		 * 同步来的对话 id 清单（供前端区分「历史在电脑上」与「历史在本地」）。*/
		case 'syncedSessions':
			return { ids: [...io.syncedSessions] };

		/**
		 * 列出电脑上某个目录下的历史对话（**只要名称**）。
		 *
		 * 用户需求一原文：「点开弹出列表，显示电脑上该文件夹的历史对话，
		 * 只要名称」+「电脑要**提前抓取**对话名称，手机需要时直接下发」。
		 *
		 * 过滤在电脑侧做（见 desktop 的 `listWorkspaceSessions`）：本机 578 个
		 * 会话全量下发有几 MB，而免费隧道带宽是硬约束。
		 *
		 * @returns `{ sessions: [{sessionId, title, updatedAt}] }`。
		 */
		case 'listDesktopConversations': {
			if (config.token === '') throw new Error('还没配对：请先在「电脑」页签里填配对码');
			if (typeof params.path !== 'string' || params.path === '') throw new Error('path required');
			return await callRemote(config, 'listWorkspaceSessions', { cwd: params.path });
		}

		/**
		 * 把用户勾选的电脑对话「同步」到手机侧边栏。
		 *
		 * ## 关键设计：不在手机落一份完整历史
		 *
		 * 手机本地 session 的 seq 是**连续从 0 开始**的
		 *（`dsh-session/lib/index.js:1195` `get seq() { return this.log.length }`，
		 * `append()` 里 `seq: SessionSeq(this.log.length)` 写死），
		 * 所以「只存最近 N 条」物理上不可能 —— 中间会留一个巨大的 seq 空洞，
		 * 之后任何 append 都会撞 `事件流有缺口`。
		 *
		 * 所以同步只做三件事：
		 *   1. 在手机本地建一个**空壳会话**（工作区 = 该电脑目录的镜像），
		 *      侧边栏里就出现了这一行，样式与原生会话完全一样（它就是原生会话）；
		 *   2. 把 `sessionId` 记进 `remoteSessions` 和 `syncedSessions`，并登记
		 *      `slug → 电脑路径`（`pendingPlans` 里放同步标记，prompt 时才建电脑侧）；
		 *   3. 历史**不搬**：前端打开时走 `follow`/`page` 的拦截器，
		 *      按需从电脑拉最近 N 条（见 `controllerPage` 拦截器）。
		 *
		 * 实测带宽依据：最大的会话全量明文 155 MB / 29638 事件，
		 * 而「最近 10 条」只要 131 KB —— 差 1200 倍。
		 *
		 * @returns `{ synced: [sessionId...] }`。
		 */
		case 'syncConversations': {
			if (config.token === '') throw new Error('还没配对：请先在「电脑」页签里填配对码');
			if (typeof params.path !== 'string' || params.path === '') throw new Error('path required');
			if (!Array.isArray(params.items)) throw new Error('items required');
			// ⚠️ **先按 sessionId 去重**（保留第一次出现的那条）。
			//
			// 前端一般不会传重复的，但这一步的代价极低、收益很实在：
			// 同一个 id 出现两次时，第二次会走「已存在」分支再调一次
			// `markSessionNonBlank` —— 那个函数**每次都会 append
			// `session/title` + `turn/start` + `turn/end` 三个事件**。
			// 重复调用就在本地日志里堆出多份空轮次，虽然不影响正确性，
			// 但会让会话的 seq 无谓变大、投影多跑几轮。
			const seen = new Set();
			/** @type {Array<{sessionId?: string, title?: string}>} */
			const wanted = [];
			for (const item of params.items) {
				const id = typeof item?.sessionId === 'string' ? item.sessionId : '';
				if (id === '' || seen.has(id)) continue;
				seen.add(id);
				wanted.push(item);
			}
			if (typeof ctx.sessionController?.create !== 'function') {
				throw new Error('sessionController.create 不可用，无法在手机本地建会话');
			}
			const workspace = await ensureDesktopWorkspace(ctx, params.path, io.workspaceMap);
			// ⚠️⚠️ **必须走 `workspaceId`，不能传 `cwd` 再手动 `attachSession`。**
			//
			// 这是「同步来的第二个对话不出现在侧边栏、而是掉进未分组」的真根因
			// （真机复现：`session-a02697b4` 在电脑上有 21 条历史、`syncedSessions`
			// 里也有它，但侧边栏 Desktop 下只见第一个会话，第二个在「未分组」里）。
			//
			// 原因在官方 `Workspace.attachSession()`（`dsh-workspace/lib/index.js:122`）：
			//
			//     if (cwd !== this.record.path)
			//       throw new Error(`cannot attach session ... : its cwd resolves to '${cwd}'`)
			//
			// `record.path` 是**建工作区时 realpath 过的**，而这里传给 `create` 的
			// `cwd` 是 `workspace.path`。手机上的 `$DSH_HOME` 常走
			// `/data/user/0/...`，realpath 会解成 `/data/data/...` —— 两边字符串
			// 不等，`attachSession` 就抛错。而 `syncConversations` 里那句
			// `await workspace.attachSession(sessionId)` 是包在 try/catch 里的
			// （失败只打 warn），于是**错误被吞、会话留在未分组**，界面看不出
			// 任何异常。这就是「同步成功了但侧边栏没有它」的完整链条。
			//
			// 官方 `sessionController.create({ workspaceId })` 的分支
			//（`dsh-api-session-controller/lib/index.js:575-593`）**自己**会用
			// `workspace.path` 当 cwd 并在成功后调 `attachSession` —— 它拿到的
			// 是同一个 `record.path`，不存在上面的字符串错位，而且这是官方路径。
			//
			// 所以这里改成只传 `workspaceId`（与电脑端 `ensureWorkspaceId` 的
			// 写法一致）。⚠️ 官方 `:572` 明确禁止**同时**传 `workspaceId` 和
			// `cwd`，两个都给会直接抛 `session.create accepts workspaceId or
			// cwd, not both`。
			const workspaceId = typeof workspace?.id === 'string' && workspace.id !== '' ? workspace.id : undefined;
			const synced = [];
			/**
			 * 逐条失败的原因（`[{sessionId, error}]`）。
			 *
			 * ⚠️ 必须带回给前端：真机上插件的 `ctx.logger` **进不了 app.log**，
			 * 只打日志等于把失败彻底吞掉（见下面 catch 里的注释）。
			 */
			const failures = [];
			// 诊断：记这次调用的入参规模，排查「同步返回空数组」用。
			markDiag.push({
				at: Date.now(),
				sid: 'ENTER',
				title: `items=${Array.isArray(params.items) ? params.items.length : 'not-array'}`,
				cold: false,
				woke: false,
				hasAppend: workspaceId !== undefined,
				wrote: false,
				error: '',
			});
			if (markDiag.length > 30) markDiag.shift();
			// ⚠️ 遍历**去重后的** `wanted`，不是原始 `params.items`
			//（理由见上面 `seen` 的注释：重复项会重复 append 空轮次）。
			for (const item of wanted) {
				// ⚠️ **sessionId 直接沿用电脑的**：两端的会话 id 一致，事件才能直接
				// 互搬（这是整套架构的前提，见文件头）。自己另生成一个就没法对上了。
				const sessionId = typeof item?.sessionId === 'string' ? item.sessionId : '';
				if (sessionId === '') continue;
				// 电脑上那个对话的标题（`listWorkspaceSessions` 已经取好）。
				// 电脑上没标题时它是「（未命名对话）」—— 那种就原样传下去，
				// `markSessionNonBlank` 会把它写进日志，总比「未命名」有信息量。
				const desktopTitle = typeof item?.title === 'string' ? item.title : '';
				// ⚠️ 记下电脑上的标题：`follow` 首帧要用它填 `projections.values`
				//（同步会话不落盘、本地没有 title 事件，见 `desktopTitles` 的注释）。
				// 空格串就跳过 —— 不要用空值覆盖掉已有的好标题。
				if (desktopTitle !== '') io.rememberTitle?.(sessionId, desktopTitle);
				// 已经在本地的跳过（用户重复同步同一批是常态）。
				//
				// ⚠️ **把查到的对象留着**（`liveSession`）：下面的
				// `markSessionNonBlank` 要直接用它，不能再查一次 ——
				// 两次查询之间会话可能已被卸载，那时它会白跑一次 create，
				// 而对已持久化的会话 create 会撞 `corrupt`（真机日志确认过），
				// 标题就一条都写不进去。
				const liveSession = ctx.sessions?.get?.(sessionId);
				// ⚠️⚠️ **冷会话也要认出来，否则它会掉进下面的 `create` 分支。**
				//
				// ## 症状（用户 2026-10-21 报障）
				//
				// 「底部的 token 信息以及上下文信息，都没有显示了」。
				//
				// ## 根因
				//
				// 判断「本地已存在」原来只靠 `ctx.sessions.get()`，而它**只返回
				// 内存里活着的会话** —— App 重启后，磁盘上的会话日志还在，内存里
				// 却没有。于是：
				//
				//   ① 老会话被当成「新会话」→ 走到下面的 `create`；
				//   ② 而 `create` 对**已持久化**的会话会撞 `corrupt`
				//      （见下面 `liveSession` 的注释，真机日志确认过）→ 抛错；
				//   ③ 抛错后 `syncedSessions.add` 根本没执行 ⇒ 这个会话
				//      **永远不进 `syncedSessions`**；
				//   ④ 而 `follow` 拦截器判 `syncedSessions.has(...)` 才走远程路径
				//      （见 `controller.follow` 里的 `fromRemote`）⇒ 它走官方本地
				//      路径 ⇒ 读本地那份 475 字节的空壳日志 ⇒
				//      `tokenUsage` / `contextPressure` 投影算不出来 ⇒
				//      底部 token 与上下文信息整块消失。
				//
				// 所以这里**先尝试唤醒**：唤醒成功就说明本地确实有这个会话
				// （只是冷着），应当走「已存在」分支把 `synced` 标上。
				if (liveSession === undefined) {
					try {
						await ctx.sessionController.create({
							sessionId,
							...(workspaceId !== undefined ? { workspaceId } : {}),
						});
					} catch {
						// 唤醒失败（比如磁盘上确实没有）→ 交给下面的 `create` 分支
						// 正常新建。这里不报错，那条路自己会处理。
					}
				}
				const liveSessionAfterWake = ctx.sessions?.get?.(sessionId) ?? liveSession;
				if (liveSessionAfterWake !== undefined) {
					// ⚠️ **但归属要补一次**：旧版本（1.0.75 及以前）同步出来的会话
					// 因为上面那段 `attachSession` 字符串错位，全都掉进了「未分组」。
					// 光跳过的话，用户升级后重新同步这批对话**仍然停在未分组**，
					// 只能手动删掉再同步才恢复 —— 那不是修好了，是要用户自己绕。
					//
					// `attachSession` 是幂等的（`dsh-workspace/lib/index.js:112`
					// 已经包含就只做一次去重 mutate），失败也无所谓（会话在别处
					// 可能本来就属于另一个工作区），所以这里只补、不报错。
					if (workspaceId !== undefined && typeof workspace.attachSession === 'function') {
						try {
							await workspace.attachSession(sessionId);
						} catch {
							// 已经属于别的工作区，无所谓 —— 这里只补、不报错。
						}
					}
					// ⚠️ 老版本同步出来的会话可能也是空壳（`blank: true`），
					// 侧边栏看不见。这里补一次标记，让升级后的用户重新同步就能恢复。
					//
					// 标题也一并补：老版本同步出来的会话在本地日志里**没有**
					// `session/title` 事件，所以显示「未命名」。重新同步一次
					// 就能把电脑上的真名补上 —— 不用让用户删了重来。
					//
					// ⚠️ 传 `wake`：这些老会话此刻**多半是冷的**（App 重启过），
					// 不唤醒就写不进去，重新同步也白搭（真机踩过）。
					//
					// 同时把刚查到的 `liveSessionAfterWake` 传下去，避免二次查找的竞态。
					await markSessionNonBlank(ctx, sessionId, desktopTitle, {
						workspaceId,
						cwd: workspace?.path,
					}, liveSessionAfterWake, io.rememberTurnOffset);
					io.remoteSessions.add(sessionId);
					io.syncedSessions.add(sessionId);
					// ⚠️ 落盘电脑路径：老版本同步出来的会话**没有**这条记录，
					// 重新同步一次就补上了 —— 顺便把「重启后发不了消息」一起治好。
					io.rememberPath?.(sessionId, params.path);
					io.persist();
					synced.push(sessionId);
					continue;
				}
				try {
					// ⚠️ 有 `workspaceId` 就只传它（官方会自己 attach，见上）；
					// 拿不到 id 的老环境才退回 `cwd` + 手动 attach。
					if (workspaceId !== undefined) {
						await ctx.sessionController.create({ sessionId, workspaceId });
					} else {
						await ctx.sessionController.create({ sessionId, cwd: workspace.path });
						if (typeof workspace.attachSession === 'function') {
							await workspace.attachSession(sessionId);
						}
					}
					// ⚠️ **必须紧跟 `create` 调用**（会话此时才活着，见函数注释）：
					// 电脑上没消息的会话在本地是纯空壳，`blank: true` 会让
					// 侧边栏**完全不渲染**它 —— 用户看到的现象就是「同步成功了
					// 但列表里没有第二个对话」。
					//
					// 同时把电脑上的标题写进本地日志，否则侧边栏显示的是
					// 「未命名」而不是真实对话名（见 `markSessionNonBlank` 的注释）。
					await markSessionNonBlank(ctx, sessionId, desktopTitle, {
						workspaceId,
						cwd: workspace?.path,
					}, undefined, io.rememberTurnOffset);
					// 标记成「同步来的」：它的历史不在本地，读的时候要转发电脑。
					io.syncedSessions.add(sessionId);
					io.remoteSessions.add(sessionId);
					// 登记电脑路径：第一条消息要据此在电脑上建出会话（见 prompt 拦截器）。
					io.pendingPlans.set(sessionId, { desktopPath: params.path });
					// ⚠️ 同时**落盘**一份：`pendingPlans` 活不过 App 重启，
					// 而落盘的这份要用来在重启后反解路径（见 `loadRemotePaths`）。
					io.rememberPath?.(sessionId, params.path);
					synced.push(sessionId);
				} catch (error) {
					// ## ⚠️⚠️ 失败原因**必须带回给前端**（用户 2026-10-22 报障）
					//
					// 原来这里只打 `ctx.logger.warn` —— 而**引擎的 warn 在真机上
					// 进不了 `app.log`**（查过，一条都没有）。于是用户看到的是：
					//
					//     点了同步 → 没反应、没报错、列表还是空的
					//
					// 完全无法定位，这次排查里花掉了最多时间。
					//
					// 所以除了日志，还要把原因累加进返回值，让前端能弹出来。
					const reason = String(error?.message ?? error);
					markDiag.push({
						at: Date.now(),
						sid: typeof sessionId === 'string' ? sessionId.slice(8, 16) : '',
						title: '',
						cold: true,
						woke: false,
						hasAppend: false,
						wrote: false,
						error: reason.slice(0, 200),
					});
					if (markDiag.length > 30) markDiag.shift();
					failures.push({ sessionId, error: reason });
					ctx.logger?.warn?.(`[dsh-remote-mobile] 同步会话 ${sessionId} 失败（跳过）: ${reason}`);
				}
			}
			io.persist();
			ctx.logger?.info?.(
				`[dsh-remote-mobile] 已同步 ${synced.length} 个对话到 ${params.path}（历史按需加载，不落本地）`,
			);
			// ⚠️ 带上 `failures`：全部成功时它是空数组，前端据此判断要不要弹提示。
			return { synced, failures };
		}

		/**
		 * 读一段**电脑上**的历史（供同步会话按需加载）。
		 *
		 * @returns `{ records, hasMore }`，结构与官方 `controller.page()` 一致。
		 */
		case 'readDesktopHistory': {
			if (config.token === '') throw new Error('还没配对');
			if (typeof params.sessionId !== 'string' || params.sessionId === '') {
				throw new Error('sessionId required');
			}
			const body = { sessionId: params.sessionId };
			if (typeof params.beforeSeq === 'number') body.beforeSeq = params.beforeSeq;
			// ⚠️ `throughSeq` 必须透传（与 `controller.page` 拦截器同一套语义）：
			// 电脑端会优先用它当精确上界，缺了就只能自己猜「末尾在哪」，
			// 一旦猜偏，前端 `assertPageThrough` 就会抛
			// `page did not end at its requested cursor`。
			if (
				typeof params.throughSeq === 'number' &&
				Number.isSafeInteger(params.throughSeq) &&
				params.throughSeq >= -1
			) {
				body.throughSeq = params.throughSeq;
			}
			if (typeof params.maxMessages === 'number') body.maxMessages = params.maxMessages;
			return await callRemote(config, 'readHistory', body);
		}

		/**
		 * 把用户在手机卡片上的选择回传给电脑。
		 *
		 * 对应桌面端 `askPhone()` 推过来的那条 `ask` 事件（带 `requestId`）。
		 * 电脑端收到后 resolve 挂起的 waterfall，那一轮继续往下跑。
		 *
		 * ⚠️ 电脑端返回 `ok: false` 是**正常情况**，不是错误：
		 *   · `expired` —— 等超时了（5 分钟）或这一轮已被中断；
		 *   · `no-relay` —— 电脑端还是老版本，没这条通道。
		 * 所以这里**原样返回**，不抛错 —— 前端据此静默收起卡片即可，
		 * 弹红条只会让用户以为出故障了。
		 */
		case 'answerAsk': {
			if (typeof params.requestId !== 'string' || params.requestId === '') {
				return { ok: false, reason: 'bad-request' };
			}
			try {
				return await callRemote(config, 'answerAsk', {
					requestId: params.requestId,
					answer: params.answer,
				});
			} catch (error) {
				// 隧道断了 / 电脑关掉了。同样不抛：卡片照常收起，用户
				// 可以在电脑上处理，或者下一轮重新弹。
				return { ok: false, reason: 'unreachable', error: String(error?.message ?? error) };
			}
		}

		/**
		 * 配对：用电脑上生成的 6 位码换回这台电脑的隧道地址与令牌。
		 *
		 * ⚠️ **必须就地改写 `config`**（`Object.assign` 而不是重新赋值）：
		 * `config` 是 `apply()` 里 `loadConfig()` 的产物，被闭包里的 `prompt`
		 * 拦截、`follow`、以及本函数共享。如果这里只写盘不更新对象，
		 * 配对的**当下**仍然拿着旧的默认地址与令牌 —— 表现是「配对提示成功、
		 * 一发消息就 401」，而重启 App 后又好了（重启才会重新 `loadConfig`）。
		 */
		case 'pair': {
			if (typeof params.code !== 'string' || params.code.trim() === '') {
				throw new Error('请输入电脑上显示的 6 位配对码');
			}
			const saved = await claimPairing(params.code.trim());
			Object.assign(config, saved);
			// 配对成功后**静默**预拉电脑的模型与预设（用户需求原文：
			// 「用户配对成功之后 就可以静默的拉取一些数据了，比如模型列表和预设列表」）。
			// ⚠️ 不 await：这是后台动作，配对请求要立刻回给前端，别让用户等一轮往返。
			io.prefetch?.('配对成功')?.catch?.(() => {});
			return { endpoint: saved.endpoint, tokenSet: saved.token !== '' };
		}

		/** 当前连的是哪台电脑（前端用来显示状态）。 */
		case 'pairStatus':
			// 「已配对」的判据是**有令牌**。早期拿「令牌不等于内置常量」当判据，
			// 那个常量已删（它是公开凭据，见 EMPTY_TOKEN 的注释）。
			return { endpoint: config.endpoint, paired: config.token !== EMPTY_TOKEN };

		/**
		 * 解除配对：清掉本地凭据，退回未配对状态，让用户能重新填配对码。
		 *
		 * ⚠️ **这个方法是必需的回退路径，不是便利功能**。电脑端一旦「解绑」，
		 * 它会换掉令牌并停掉隧道；手机这边 `mobile.json` 里还留着旧令牌，
		 * `pairStatus` 就一直报「已配对」⇒ 前端那条「已配对」横幅永远挂着、
		 * **输入框根本不渲染**（见 client.js 里 `paired === true` 的分支）。
		 * 结果就是：电脑解绑后手机永久卡在 502，界面上没有任何重新配对的入口，
		 * 只能手工删 `mobile.json` 或清 App 数据。
		 *
		 * 清理分三部分，缺一不可：
		 * ① 凭据 —— `config` 就地清空 + 写盘，否则重启后旧令牌又回来了；
		 * ② 连接 —— 停掉所有跟随旧电脑的事件流，否则它们会拿着旧令牌
		 *    无限重连（退避到 15s 一次）一直打一个已经不属于自己的域名；
		 * ③ 会话归属 —— `remoteSessions` 里的会话原本靠「转发电脑」工作，
		 *    解绑后电脑那边可能已经不存在了，继续留着会让这些会话的 prompt
		 *    走 `callRemote` 报错。一并清掉，它们就退回普通本地会话。
		 *
		 * @returns 清空后的 `{ endpoint, paired: false }`。
		 */
		case 'unpair': {
			Object.assign(config, saveConfig(DEFAULT_ENDPOINT, EMPTY_TOKEN));
			for (const controller of io.follows.values()) {
				try {
					controller.abort();
				} catch {
					/* 已经结束了 */
				}
			}
			io.follows.clear();
			io.mirrorStates.clear();
			io.remoteSessions.clear();
			io.syncedSessions.clear();
			// ⚠️ **运行态表也要清**：它是模块级的，换电脑配对后旧会话的
			// `running` 还留着。不清的话，新电脑上同名会话会被「去重」判成
			// 状态未变而不派发 ⇒ 按钮状态是上一台电脑的。
			reconcileStates.clear();
			// ⚠️ **子层目录缓存必须一起清**：`catalog` / `presets` / 根层目录在下次
			// 配对成功时会被 `prefetchDesktop` **覆盖**，不会串味；而 `dirs` 是
			// **累加**的（用户每浏览一层写一格），不清的话换一台电脑配对后，
			// 旧电脑那些子目录还会命中缓存 —— 用户点进去看到的是上一台电脑的内容。
			if (io.desktopCache !== undefined) {
				io.desktopCache.dirs = emptyDirCache();
				io.desktopCache.directory = null;
			}
			io.persist();
			ctx.logger?.info?.('[dsh-remote-mobile] 已解除配对，本地凭据与远程会话清单已清空');
			return { endpoint: config.endpoint, paired: false };
		}

		default:
			throw new Error(`unknown method "${String(io.body?.method)}"`);
	}
}

export { callRemote, mirrorEvent, loadConfig, DEFAULT_ENDPOINT, CONFIG_PATH, PAIR_ENDPOINT, claimPairing, handleBridge };
