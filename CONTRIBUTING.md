# 贡献指南

先说结论：这个仓库里**最有价值的贡献是「让它在更多设备上不崩」**，其次才是新功能。

## 最重要的规矩：前端代码要能在老 WebView 上跑

`lib/client.js` 由 DSH 前端在**用户的设备上**执行 —— 可能是五年前的安卓机、可能是某个系统自带的旧 WebView。它一旦用了新语法，**整个文件解析失败**，插件在界面上完全消失，而日志里往往什么也看不到。

所以 `lib/client.js` 里**禁止**出现：

| 禁用 | 原因 | 替代写法 |
|---|---|---|
| `?.` 可选链 | Chrome 80+ | `a && a.b && a.b.c` |
| `??` 空值合并 | Chrome 80+ | `x == null ? d : x` |
| `\|\|=` / `&&=` | Chrome 85+ | 显式 if |
| `Object.fromEntries` | Chrome 73+ | for 循环手动塞 |
| `Array.prototype.flat` / `flatMap` | Chrome 69+ | 手写递归 / 循环 |
| `String.replaceAll` | Chrome 85+ | `split().join()` 或带 `g` 的正则 |
| `Array.prototype.at` | Chrome 92+ | `a[a.length - 1]` |
| `structuredClone` | Chrome 98+ | `JSON.parse(JSON.stringify(x))` |
| 类字段 `class A { x = 1 }` | Chrome 72+ | 在构造函数里赋值 |
| 私有字段 `#x` | Chrome 74+ | 用 `_x` 约定 |

CSS 同理，关键属性必须**先写固定值兜底，再写新特性覆盖**：

```js
// 对：老内核用固定值，新内核用 clamp
'padding:16px 24px;padding:clamp(8px,2vh,16px) 24px;'

// 错：老内核不认识 clamp，整条声明被丢弃 ⇒ padding 变 0
'padding:clamp(8px,2vh,16px) 24px;'
```

已经踩过的坑：`height:min(560px,100dvh - 32px)` 在不支持 `dvh` 的内核上**整条失效**，弹窗高度退化成 `auto`，被几十个文件夹撑满整屏，标题栏和底部按钮全被顶出视口。所以现在写成三档：

```js
'max-height:calc(100vh - 48px);' +        // 都认
'max-height:calc(100dvh - 48px);' +       // 新内核覆盖
'height:min(720px,calc(100vh - 48px));' +
'height:min(720px,calc(100dvh - 48px));' +
'height:min(720px,100%);' +
```

> `lib/index.js` / `lib/pair.js` 跑在 Node 侧（Node 20+），**不受上述限制**，
> 放心用现代语法。

### 改完怎么自查

```bash
# 语法（必须全绿）
node --check dsh-remote-mobile/lib/client.js
node --check dsh-remote-desktop/lib/client.js

# 禁用语法（应当没有任何命中）
grep -nE '\?\.[a-zA-Z_$]|\?\?|Object\.fromEntries|\.flat\(|\.replaceAll\(|\.at\(' \
  dsh-remote-mobile/lib/client.js dsh-remote-desktop/lib/client.js
```

## 改完必须真机验证

**「构建成功」不等于「改动生效」。** 这两个包没有构建步骤，`lib/` 就是最终产物，
所以验证只有一条路：装到真实设备上看。

最低限度要确认三件事：

1. 插件在 DSH 里**加载出来了**（界面有对应入口），不是静默失败。
2. 你改的那个交互**按预期工作**。
3. 控制台/日志里**没有红色报错**。

尤其注意：如果你只在自己那台新手机/新 WebView 上测，很多兼容问题**根本测不出来**。
这正是上面那堆禁令存在的原因 —— 它们是别人踩坑换来的。

## 提交

- 一个 PR 只做一件事。修 bug 别顺手重构。
- 改了行为就在 `CHANGELOG.md` 加一条。
- 提交信息说清「改了什么、为什么」，不要写「fix bug」。
- **不要把任何真实凭据、域名、隧道地址提交进来**。默认值一律用 `example.com` 占位。

## 安全问题

发现安全问题（尤其是隧道鉴权、路径穿越、凭据泄露这类）请**不要开公开 issue**，
先私下联系维护者。

## 开发环境

两个包都是纯 ESM、无构建步骤：

```bash
git clone <this-repo>
cd dsh-jiuling-remote-plugins
# 直接改 lib/ 里的文件即可，没有 npm install 这一步
```

Node 20 或更高。
