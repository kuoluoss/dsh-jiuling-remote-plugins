# dsh-jiuling-remote-plugins

把一台电脑上的 [DeepSeek](https://www.npmjs.com/package/@deepseek-ai/dsh) 会话搬到手机上：
手机进入**电脑的目录**、在电脑上发起对话，并把电脑的实时消息流同步回手机界面。

公网只传文本，不做屏幕映射。

仓库里是**两个互相独立的 npm 包**：

| 目录 | 包名 | 装在哪 | 作用 |
|---|---|---|---|
| `dsh-remote-desktop` | `dsh-jiuling-remote-desktop` | 电脑上的 DSH | 转发本机会话事件、接收手机请求、配对 |
| `dsh-remote-mobile` | `dsh-jiuling-remote-mobile` | 手机上的 DSH | 在选文件夹里加「电脑」页签、镜像电脑消息流 |

两边通过一条 Cloudflare 隧道通信，靠一个**配对码**建立信任。

---

## 它到底做了什么

**电脑端**（`dsh-remote-desktop`）

- 在本机起一个 HTTP 服务，交给 Cloudflare 隧道暴露到公网。
- 收到手机的请求后，代为调用本机 DSH 的 RPC（列目录、读模型列表、新建会话、发消息……）。
- 订阅本机会话的事件流，转成精简的文本事件推给手机。

**手机端**（`dsh-remote-mobile`）

- 在「选择工作区目录」对话框里多出一个 **「电脑」页签**，可以浏览电脑上的文件夹。
- 选中一个电脑目录 → 在电脑上开一个会话 → 手机跳到那个会话。
- 电脑那边的模型列表、预设列表会覆盖手机本地的对应列表（因为真正跑的是电脑）。
  切回手机工作区时自动恢复成手机的那份。
- 目录树、模型列表、预设列表都在手机本地落盘缓存，只在你点「刷新」时重新拉取。

---

## ⚠️ 开箱不能直接用

这个仓库**依赖一个配对服务端**（签发/校验配对码、分发 cloudflared），
**那个服务端不在本仓库里**。两个插件里与之相关的地址都是占位域名：

```js
// dsh-remote-mobile/lib/index.js
const DEFAULT_ENDPOINT = 'https://dsh-remote.example.com';  // 隧道地址
const CF_ZONE_NAME     = 'example.com';                     // 凭据白名单区
const PAIR_ENDPOINT    = 'https://pair.example.com';        // 配对服务

// dsh-remote-desktop/lib/pair.js
export const PAIR_ENDPOINT = 'https://pair.example.com';    // 可用环境变量覆盖
```

要自己跑起来，你得：

1. 准备一个域名 + 一次 Cloudflare 隧道（指向电脑端的服务端口）。
2. 自己实现一个配对服务，至少提供这几个端点：

   | 端点 | 作用 |
   |---|---|
   | `POST /pair/claim` | 用激活码 + 配对码换 `accessToken` |
   | `POST /pair/register` | 用激活码注册这台机器，返回隧道令牌 |
   | `POST /pair/release` | 解绑时清除服务端记录 |
   | `GET /pair/cloudflared/:file` | 分发 cloudflared 二进制 |

3. 把上面三个常量改成你自己的，或：
   - 电脑端设环境变量 `DSH_REMOTE_PAIR_ENDPOINT`
   - 手机端改 `mobile.json` 里的 `endpoint` / `zone` / `pairEndpoint`

   （改配置比改源码好，升级时不会被覆盖）

⚠️ `CF_ZONE_NAME` 是**凭据白名单**：插件只会把 token 发往这个域名区。
改错会出现「配对成功但连不上」，因为它拒绝向非白名单主机发送凭据。

⚠️ **包里不含任何凭据。** token 一律本机生成、本机落盘（`token.txt`），
配对成功后由服务端下发的 `accessToken` 取代。

---

## 安装

```bash
# 电脑上
pnpm add dsh-jiuling-remote-desktop

# 手机上（或在 DSH App 的「插件 → 添加插件」里输入包名）
pnpm add dsh-jiuling-remote-mobile
```

⚠️ 手机 App 内的插件市场有安装源选项。若装最新版时报
**「没有匹配的版本」**，通常是中国大陆镜像还没同步 —— 把安装源切到
**npm 官方源（registry.npmjs.org）** 再装。

装完要**重启 App**才会加载。

---

## 配对流程

配对分成**两步**，顺序是刻意的。

### 第 1 步：准备环境（先下依赖）

在电脑端 DSH 的设置页点 **「下载隧道组件」**。

- 需要下载约 55 MB 的 `cloudflared`，有百分比进度条。
- **这一步可以随便重试**，失败了重下就行，不影响任何东西。

> 为什么先做这步？早期版本把下载塞在「绑定」按钮里，于是网络一慢就变成：
> 激活码已经交给服务端（**已被消费**）→ 下载超时 → 整个绑定失败 →
> 用户以为激活码没用上、重填，结果撞上服务端的「设备不符」，激活码永久废掉。
> 现在两件事彻底拆开，**下载失败永远牵连不到激活码**。

### 第 2 步：填激活码

环境就绪后，激活码输入框才会出现。填码 → 点「绑定」，几秒完成。

### 第 3 步：手机连上来

电脑端显示一个**六位配对码**（还有二维码），在手机上输入即可。

配对成功后手机拿到 `accessToken`，之后所有请求都带它。

换电脑、解绑之后旧凭据作废，缓存里的目录也会一起清掉。

---

## 兼容性（重要）

`lib/client.js` 跑在**用户设备的 WebView** 里，可能是很旧的安卓机。
所以这个文件里禁止使用新语法，否则**整个文件解析失败**，插件在界面上直接消失。

禁用清单、替代写法、以及 CSS 兜底的正确姿势，全部写在
[CONTRIBUTING.md](CONTRIBUTING.md) 里。**改 `client.js` 前请务必读一遍。**

已经踩过的两个坑（都表现为「界面上的东西凭空消失」）：

1. `height:min(560px,100dvh - 32px)` —— 旧内核不认识 `dvh`，**整条声明失效**，
   弹窗高度退化成 `auto`，被几十个文件夹撑满整屏，标题栏和底部按钮全被顶出视口。
2. `padding:clamp(...)` 单独使用 —— 不认识 `clamp()` 的内核会丢掉整条声明，
   padding 变 0。

两者的解法都是**先写固定值兜底，再写新特性覆盖**。

---

## 开发

两个包都是纯 ESM、**无构建步骤**，`lib/` 里就是最终产物。

```bash
git clone <this-repo>
cd dsh-jiuling-remote-plugins
# 没有 npm install 这一步，直接改 lib/ 即可
```

改完发布（npm 的规矩，不是本仓库的）：

```bash
cd dsh-remote-mobile        # 或 dsh-remote-desktop
# 先改 package.json 的 version，npm 不允许覆盖已发布版本
npm publish --registry=https://registry.npmjs.org/
```

⚠️ 发布后 registry 的 `latest` 标签可能延迟数分钟才更新。
急着在手机上装新版的话，直接用 `包名@版本号` 指定，不要依赖 `latest`。

---

## 目录结构

```
dsh-remote-desktop/
  package.json
  cordis.patch.yml        # DSH 的 bundle patch，声明插件在引擎里的挂载方式
  lib/
    index.js              # 服务端：隧道服务、RPC 转发、事件镜像、路径沙箱
    client.js             # 前端：电脑端的配对 / 环境准备 UI
    pair.js               # 配对流程、cloudflared 下载与校验
    qr.js                 # 配对二维码

dsh-remote-mobile/
  package.json
  cordis.patch.yml
  lib/
    index.js              # 服务端：反向代理到电脑、本地缓存、桥接路由
    client.js             # 前端：「电脑」页签、目录树、模型/预设切换
```

---

## 安全说明

- 公网只传文本事件，不映射桌面、不传文件内容。
- 电脑端对文件系统访问做了**路径沙箱**（比对到路径分隔符，不用字符串前缀，
  避免 `ws-evil` 被误判成 `ws` 的子目录）。
- 配对接口挂在本机 Web 端口上，但只接受**同源 + 带令牌**的请求。
- 解绑会通知服务端清除记录，否则会出现「本机已解绑但服务端仍占着，
  这台机器再也注册不上」的死局。

发现安全问题请**不要开公开 issue**，先私下联系维护者。

---

## License

[MIT](LICENSE)
