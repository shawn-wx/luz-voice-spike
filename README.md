# spike-voice — 西语语音链路 Spike

Phase 2：`server.py` + `web/index.html` —— 真人 mic 实时对话 Demo
（浏览器按住说话 → Deepgram 流式转写 → Cartesia WS 合成 → 播放，支持打断）。

单端口服务：HTTP 页面 + `/healthz` + WebSocket `/ws` 都在 `$PORT`
（默认 8080）上，可直接部署到 Fly.io。

## 本地运行

```bash
pip install -r requirements.txt
export DEEPGRAM_API_KEY="..."      # 裸 key，不要加 Token 前缀
export CARTESIA_API_KEY="..."
export CARTESIA_VOICE_ID="c68a8bd0-f99e-4e7f-915d-a097da6d024c"  # Luz 的嗓音
python3 server.py
# 浏览器打开 http://localhost:8080/ ，按住大按钮说西语
```

## 部署到 Fly.io（手机浏览器可操作）

见 [DEPLOY.md](DEPLOY.md)。要点：

- `fly.toml` 已配好（hkg 节点，shared-cpu-1x/256MB）。
- Secrets 在 Fly.io dashboard 里配，不进仓库：
  `DEEPGRAM_API_KEY`（裸 key）、`CARTESIA_API_KEY`（裸 key）、`CARTESIA_VOICE_ID`。
- `.github/workflows/deploy.yml`：push 到 main 自动部署（需在仓库 Secrets 里配 `FLY_API_TOKEN`）。

## 注意

- Deepgram 鉴权是 `Authorization: Token <key>`（不是 Bearer），server.py 里自动加前缀。
- Cartesia TTS WebSocket 只认 query 参数 `api_key`（header 会被忽略），
  `Cartesia-Version: 2026-03-01`，model `sonic-3`，语速 0.9x。
