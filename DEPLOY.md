# Phase 2 手机实测部署指南（全程手机浏览器可操作）

目标：把语音 Demo 部署到 Fly.io，手机打开 `https://luz-voice-spike.fly.dev`
就能按住说话、实时听到 Luz 回复。

> 费用说明：Fly.io 注册需要绑一张信用卡（扣费依据）。这台测试机是
> shared-cpu-1x / 256MB，约 **$2/月**，按秒计费。测完可在 dashboard 里删掉 app。

## 一、准备代码仓库（约 3 分钟）

1. 打开 github.com，注册/登录。
2. 右上角 `+` → **New repository**：
   - Repository name: `luz-voice-spike`
   - 选 **Public**，不要勾 "Add a README" → **Create repository**。
3. 创建 Personal Access Token：
   - 头像 → **Settings** → 最下方 **Developer settings** →
     **Personal access tokens** → **Tokens (classic)** →
     **Generate new token (classic)**。
   - Note 填 `luz-deploy`，勾选 **`repo`** 和 **`workflow`** 两个权限 →
     **Generate token**，复制这串 token（只显示一次）。
4. 把这串 token 填进 Muse 给你的安全卡片（只用一次，用来把代码推到你的仓库）。

完成后告诉 Muse 你的 **GitHub 用户名** 和仓库名，Muse 会把全部代码
（server.py、页面、Dockerfile、fly.toml、自动部署 workflow）推到你的仓库。

## 二、Fly.io 建应用 + 配密钥（约 5 分钟）

1. 打开 fly.io 注册/登录（**需要绑信用卡**）。
2. Dashboard 里创建 app，名字必须填 **`luz-voice-spike`**
   （要和 fly.toml 里的 app 名一致）。
3. 进这个 app → **Secrets** 标签页，逐个添加：
   - `DEEPGRAM_API_KEY` = 你的 Deepgram key（**裸 key，不要加 `Token ` 前缀**，
     服务器代码里会自动加）
   - `CARTESIA_API_KEY` = 你的 Cartesia key（去 play.cartesia.ai → API Keys 复制）
   - `CARTESIA_VOICE_ID` = `c68a8bd0-f99e-4e7f-915d-a097da6d024c`（Luz）
4. Dashboard 右上角头像 → **Account** → **Access Tokens** →
   创建一个 token 并复制。

## 三、连通 GitHub → Fly.io（约 2 分钟）

1. 回到你的 GitHub 仓库 → **Settings** →
   **Secrets and variables** → **Actions** →
   **New repository secret**：
   - Name: `FLY_API_TOKEN`
   - Value: 上一步复制的 Fly token → **Add secret**。
2. 仓库顶部 **Actions** 标签页 → 左侧选 **Deploy to Fly.io** →
   **Run workflow** → 等 3～5 分钟变绿 ✅。

## 四、手机实测

1. 手机浏览器打开 `https://luz-voice-spike.fly.dev`
   （看到 "Luz · prueba de voz" 页面）。
2. 点 **允许** 麦克风权限。
3. **按住红色按钮说话**（西语），松开 → 等 Luz 回复。
4. Luz 说话时**再按住按钮** → 打断，测试 barge-in。
5. 页面下方会显示延迟数字：`primer audio`（首音）和 `TTS` 时间。

### 记录这些数据发回

- 5 次对话里，转写对了几次？（页面上会显示转写文字）
- `primer audio` 一般是多少 ms？（目标 <1500ms）
- 打断是否灵敏？有没有杂音/卡顿？

## 备注

- 每次代码更新：Muse 推到 GitHub → 自动重新部署（约 3 分钟）。
- 香港节点（hkg）离你最近所以选它；正式给墨西哥/巴西用户用时会换圣保罗+美国节点。
- 测完长期不用：在 Fly.io dashboard 删掉 app 即停费。
