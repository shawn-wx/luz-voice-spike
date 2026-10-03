#!/usr/bin/env python3
"""Phase 2: live mic demo server — voice conversation loop (single port).

Pipeline profiles (PIPELINE_PROFILE):
    es (default): mic -> Deepgram Nova-3 streaming STT
                  -> LLM (groq | deepseek | stub) with Luz persona (Spanish)
                  -> Cartesia Sonic-3 WS TTS
    zh:           mic -> DashScope qwen3-asr-flash-realtime (Singapore)
                  -> LLM (dashscope qwen3.8-flash) with Luz persona (Chinese)
                  -> DashScope qwen3-tts-flash-realtime
    -> browser playback, with barge-in and latency instrumentation.

One process serves everything on $PORT (default 8080):
    GET /          -> test page (web/index.html)
    GET /healthz   -> 200 ok (platform health checks)
    GET /debug/llm      -> connectivity + inference probe for the
                           currently configured LLM provider (LLM_PROVIDER)
    GET /debug/deepseek -> DeepSeek connectivity probe (back-compat alias)
    GET /debug/dashscope -> DashScope (intl/Singapore) connectivity probe
    WS  /ws        -> voice session

Run (production, e.g. Fly.io secrets) — es profile:
    DEEPGRAM_API_KEY=<raw key> CARTESIA_API_KEY=<raw key> \\
        CARTESIA_VOICE_ID=<luz id> GROQ_API_KEY=<key> \\
        PORT=8080 python3 server.py

Run — zh profile (China demo):
    PIPELINE_PROFILE=zh DASHSCOPE_API_KEY=<intl key> \\
        PORT=8080 python3 server.py

LLM selection: LLM_PROVIDER=groq (default) | deepseek | dashscope | stub.
In the zh profile the LLM is forced to dashscope unless LLM_PROVIDER is set.
Without the provider's API key, falls back to stub automatically.
"""
import asyncio
import base64
import collections
import json
import os
import sys
import time
import urllib.parse
import uuid

import websockets
from websockets.asyncio.server import serve
from websockets.datastructures import Headers
from websockets.http11 import Request, Response

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

DG_KEY = os.environ.get("DEEPGRAM_API_KEY", "")
CART_KEY = os.environ.get("CARTESIA_API_KEY", "")
VOICE_ID = os.environ.get("CARTESIA_VOICE_ID", "")
DS_KEY = os.environ.get("DASHSCOPE_API_KEY", "")
PORT = int(os.environ.get("PORT", "8080"))
CARTESIA_VERSION = "2026-03-01"
# Overridable for local tests with mock providers (this dev VM has no
# outbound WebSocket, so real provider WS cannot be exercised here).
DEEPGRAM_WS_BASE = os.environ.get("DEEPGRAM_WS_BASE",
                                  "wss://api.deepgram.com/v1/listen")
CARTESIA_WS_BASE = os.environ.get("CARTESIA_WS_BASE",
                                  "wss://api.cartesia.ai/tts/websocket")


# 最近 N 次完整语音轮次的延迟记录（内存环形缓冲，供 /api/latency 查询）。
# 2026-10-03 加：手机端 logcat 拿不到，服务端必须自己记，否则延迟问题无法复盘。
LATENCY_LOG = collections.deque(maxlen=100)


def record_latency(entry: dict):
    entry = dict(entry)
    entry["ts"] = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime())
    LATENCY_LOG.append(entry)
    # Fly 日志里也能看到单行摘要
    print("[latency] profile={profile} llm={llm_provider} "
          "llm_ttft={llm_ttft_ms}ms llm_total={llm_total_ms}ms "
          "tts_ttfa={tts_ttfa_ms}ms first_audio={first_audio_ms}ms "
          "q={user_text!r}".format(**{k: entry.get(k) for k in
              ("profile", "llm_provider", "llm_ttft_ms", "llm_total_ms",
               "tts_ttfa_ms", "first_audio_ms", "user_text")}),
          flush=True)


def get_profile() -> str:
    """'zh' for the DashScope China-demo pipeline, 'es' otherwise."""
    p = os.environ.get("PIPELINE_PROFILE", "es").lower()
    return "zh" if p in ("zh", "zh-cn", "dashscope", "cn") else "es"


def conn_profile(ws) -> str:
    """Per-connection pipeline profile.

    `?profile=zh|es` on the /ws URL overrides the server default
    (PIPELINE_PROFILE env). Unknown/missing values fall back to default.
    This is what lets the app switch voice pipelines without rebuilds:
    the backend config (/api/voice-config) decides the default.
    """
    try:
        qs = getattr(getattr(ws, "request", None), "query_string", "") or ""
        q = urllib.parse.parse_qs(qs).get("profile", [""])[0].lower()
        if q in ("zh", "zh-cn", "dashscope", "cn"):
            return "zh"
        if q in ("es", "es-mx", "spanish"):
            return "es"
    except Exception:
        pass
    return get_profile()


def missing_keys_for(profile: str) -> list:
    """Env keys required for a profile; empty means ready."""
    if profile == "zh":
        required = [("DASHSCOPE_API_KEY", DS_KEY)]
    else:
        required = [("DEEPGRAM_API_KEY", DG_KEY),
                    ("CARTESIA_API_KEY", CART_KEY),
                    ("CARTESIA_VOICE_ID", VOICE_ID)]
    return [k for k, v in required if not v]


def profile_llm_provider() -> str:
    """LLM provider for the active profile (explicit LLM_PROVIDER wins)."""
    explicit = os.environ.get("LLM_PROVIDER", "")
    if explicit:
        return explicit
    return "dashscope" if get_profile() == "zh" else "groq"


def _vault_surrogate(connector: str) -> str:
    sys.path.insert(0, "/opt/hatch/skills/skill-creator/bin")
    from dynamic_credentials import dynamic_credential_entry
    return str(dynamic_credential_entry(connector)["surrogate"]).strip()


def deepgram_auth_headers() -> dict:
    """Authorization header for Deepgram listen (REST + WS)."""
    if DG_KEY.startswith("vault:"):
        # Local dev: the stored credential already includes the "Token "
        # prefix, so the bare surrogate goes out as the whole header value.
        return {"Authorization": _vault_surrogate(DG_KEY.split(":", 1)[1])}
    return {"Authorization": f"Token {DG_KEY}"}


def cartesia_ws_target() -> tuple:
    """(url, extra_headers) for Cartesia TTS websocket."""
    if CART_KEY.startswith("vault:"):
        # Local dev: surrogate must travel as a header value (the registered
        # placement); the query-param path never gets replaced. NOTE: the
        # real Cartesia TTS WS only accepts api_key as a query param, so
        # vault-dev cannot reach the real endpoint — use a mock via
        # CARTESIA_WS_BASE for local tests; production uses a raw key.
        return CARTESIA_WS_BASE, {
            "X-API-Key": _vault_surrogate(CART_KEY.split(":", 1)[1])}
    qs = urllib.parse.urlencode(
        {"api_key": CART_KEY, "cartesia_version": CARTESIA_VERSION})
    return f"{CARTESIA_WS_BASE}?{qs}", {}


# ---------------------------------------------------------------- HTTP layer

with open(os.path.join(BASE_DIR, "web", "index.html"), "rb") as f:
    INDEX_HTML = f.read()


async def process_request(connection, request: Request):
    # 注意：某些 websockets 版本的 request.path 会带查询串，先剥掉
    req_path = request.path.split("?", 1)[0]
    if req_path == "/healthz":
        return Response(200, "OK",
                        Headers([("Content-Type", "text/plain")]), b"ok")
    if req_path == "/debug/llm":
        body = json.dumps(await debug_llm(), ensure_ascii=False).encode("utf-8")
        return Response(200, "OK",
                        Headers([("Content-Type",
                                   "application/json; charset=utf-8")]),
                        body)
    if req_path == "/debug/deepseek":
        body = json.dumps(await debug_llm("deepseek"),
                          ensure_ascii=False).encode("utf-8")
        return Response(200, "OK",
                        Headers([("Content-Type",
                                   "application/json; charset=utf-8")]),
                        body)
    if req_path == "/debug/dashscope":
        # ?model=qwen-turbo 覆盖默认模型（多模型测速）
        qs2 = {}
        try:
            target2 = getattr(request, "target", request.path)
            qs2 = urllib.parse.parse_qs(urllib.parse.urlparse(target2).query)
        except Exception:
            pass
        model_ov = (qs2.get("model", [None])[0] or "").strip() or None
        body = json.dumps(await debug_llm("dashscope", model=model_ov),
                          ensure_ascii=False).encode("utf-8")
        return Response(200, "OK",
                        Headers([("Content-Type",
                                   "application/json; charset=utf-8")]),
                        body)
    if req_path == "/api/voice-config":
        # 后台可配置的语音链路：app 启动时拉取，按 profile 建连 /ws?profile=。
        # 改 PIPELINE_PROFILE secret 并重启即切换，无需重新打包 app。
        body = json.dumps({
            "profile": get_profile(),
            "available": ["es", "zh"],
            "ws_path": "/ws",
        }).encode("utf-8")
        return Response(200, "OK",
                        Headers([("Content-Type",
                                   "application/json; charset=utf-8")]),
                        body)
    if req_path == "/api/latency":
        # 最近语音轮次的延迟记录（排查"回复慢"用）。
        # ?n=20 取最近 20 条；?format=text 取纯文本单行摘要。
        qs = {}
        try:
            target = getattr(request, "target", request.path)
            qs = urllib.parse.parse_qs(urllib.parse.urlparse(target).query)
        except Exception:
            pass
        try:
            n = max(1, min(100, int(qs.get("n", ["20"])[0])))
        except (ValueError, TypeError):
            n = 20
        items = list(LATENCY_LOG)[-n:]
        if qs.get("format", ["json"])[0] == "text":
            lines = ["ts | profile | llm_ttft | llm_total | tts_ttfa | "
                     "first_audio | user_text"]
            for e in items:
                lines.append(
                    f"{e.get('ts')} | {e.get('profile')} | "
                    f"{e.get('llm_ttft_ms')}ms | {e.get('llm_total_ms')}ms | "
                    f"{e.get('tts_ttfa_ms')}ms | {e.get('first_audio_ms')}ms | "
                    f"{(e.get('user_text') or '')[:60]}")
            body = "\n".join(lines).encode("utf-8")
            ctype = "text/plain; charset=utf-8"
        else:
            body = json.dumps({"count": len(items), "items": items},
                              ensure_ascii=False).encode("utf-8")
            ctype = "application/json; charset=utf-8"
        return Response(200, "OK", Headers([("Content-Type", ctype)]), body)
    if req_path in ("/", "/index.html"):
        return Response(200, "OK",
                        Headers([("Content-Type",
                                   "text/html; charset=utf-8")]),
                        INDEX_HTML)
    return None  # anything else -> WebSocket handshake


async def debug_llm(name: str = None, model: str = None) -> dict:
    """Connectivity + inference probe for an LLM provider.

    No API key needed for the network-level checks: an HTTP 401/400 proves
    the full path works. If the provider's API key is set, also runs a
    minimal chat request and reports TTFT.

    model: optional override (for dashscope speed comparison).
    """
    import socket
    import ssl as ssl_mod

    name = (name or os.environ.get("LLM_PROVIDER", "groq")).lower()
    conf = {
        "deepseek": {
            "host": "api.deepseek.com",
            "url": "https://api.deepseek.com/chat/completions",
            "model": "deepseek-chat",
            "key_env": "DEEPSEEK_API_KEY",
        },
        "groq": {
            "host": "api.groq.com",
            "url": "https://api.groq.com/openai/v1/chat/completions",
            "model": os.environ.get("GROQ_MODEL", "openai/gpt-oss-20b"),
            "key_env": "GROQ_API_KEY",
            "probe": "Hola, ¿cómo estás?",
        },
        "dashscope": {
            "host": "dashscope-intl.aliyuncs.com",
            "url": ("https://dashscope-intl.aliyuncs.com/compatible-mode/v1"
                    "/chat/completions"),
            "model": os.environ.get("DASHSCOPE_MODEL", "qwen3.8-flash"),
            "key_env": "DASHSCOPE_API_KEY",
            "probe": "你好，你好吗？",
        },
    }.get(name)
    if conf is None:
        return {"error": f"unknown provider for probe: {name}"}
    # 允许 ?model= 覆盖（多模型测速用）
    if model and name == "dashscope":
        conf = dict(conf)
        conf["model"] = model

    host = conf["host"]
    out = {"provider": name, "host": host, "port": 443}
    # DNS
    try:
        t0 = time.perf_counter()
        ip = socket.gethostbyname(host)
        out["dns_ms"] = round((time.perf_counter() - t0) * 1000, 1)
        out["resolved_ip"] = ip
    except Exception as e:
        out["dns_error"] = str(e)
        return out
    # TCP + TLS
    try:
        t0 = time.perf_counter()
        ctx = ssl_mod.create_default_context()
        raw = socket.create_connection((host, 443), timeout=10)
        tls = ctx.wrap_socket(raw, server_hostname=host)
        out["tls_handshake_ms"] = round(
            (time.perf_counter() - t0) * 1000, 1)
        out["tls_version"] = tls.version()
        tls.close()
    except Exception as e:
        out["tls_error"] = str(e)
        return out
    # HTTPS (no key -> 401/400 still proves connectivity)
    try:
        import httpx
        t0 = time.perf_counter()
        async with httpx.AsyncClient(timeout=15.0) as client:
            r = await client.post(
                conf["url"],
                json={"model": conf["model"],
                      "messages": [{"role": "user", "content": "hi"}]},
                headers={"Content-Type": "application/json",
                         "Authorization": "Bearer probe-no-key"})
        out["https_status"] = r.status_code
        out["https_ms"] = round((time.perf_counter() - t0) * 1000, 1)
        out["https_body_head"] = r.text[:120]
    except Exception as e:
        out["https_error"] = str(e)
        return out
    # Real inference probe (only if key configured)
    api_key = os.environ.get(conf["key_env"], "")
    if api_key:
        try:
            from llm import get_provider
            from llm.persona import SYSTEM_PROMPT, SYSTEM_PROMPT_ZH
            if model and name == "dashscope":
                from llm.dashscope import DashScopeProvider
                provider = DashScopeProvider(model=model)
            else:
                provider = get_provider(name)
            t0 = time.perf_counter()
            ttft_ms = None
            chars = 0
            persona = (SYSTEM_PROMPT_ZH if name == "dashscope"
                       else SYSTEM_PROMPT)
            async for delta in provider.chat_stream(
                    [{"role": "user",
                      "content": conf.get("probe", "Hola, ¿cómo estás?")}],
                    persona, max_tokens=60):
                if ttft_ms is None:
                    ttft_ms = round(
                        (time.perf_counter() - t0) * 1000, 1)
                chars += len(delta)
            total_ms = round((time.perf_counter() - t0) * 1000, 1)
            out["llm_ok"] = True
            out["llm_ttft_ms"] = ttft_ms
            out["llm_total_ms"] = total_ms
            out["llm_chars"] = chars
        except Exception as e:
            out["llm_ok"] = False
            out["llm_error"] = str(e)[:200]
    else:
        out["llm_skipped"] = f"{conf['key_env']} not set"
    return out


# Back-compat alias
async def debug_deepseek() -> dict:
    return await debug_llm("deepseek")


# ------------------------------------------------------- provider sessions

async def deepgram_listen(mic_to_dg, on_final, on_dg_status, stop_event):
    """Stream mic audio to Deepgram, forward transcripts, call on_final(text).

    Keeps the Deepgram session alive with KeepAlive when idle (avoids
    NET0001 1011 timeout) and reconnects automatically if the connection
    drops, until stop_event is set.
    """
    qs = urllib.parse.urlencode({
        "model": "nova-3",
        "language": "es",
        "smart_format": "true",
        "interim_results": "true",
        "endpointing": "300",
        # Browser sends raw PCM16 16kHz mono; Deepgram must be told.
        "encoding": "linear16",
        "sample_rate": "16000",
    })
    url = f"{DEEPGRAM_WS_BASE}?{qs}"

    async def run_session():
        async with websockets.connect(
            url, additional_headers=deepgram_auth_headers(),
            max_size=16 * 1024 * 1024,
        ) as dg:
            await on_dg_status("connected", None)

            async def sender():
                while not stop_event.is_set():
                    try:
                        msg = await asyncio.wait_for(mic_to_dg.get(),
                                                     timeout=5.0)
                    except asyncio.TimeoutError:
                        # idle: keepalive so Deepgram doesn't 1011 us
                        try:
                            await dg.send(json.dumps({"type": "KeepAlive"}))
                        except Exception:
                            break
                        continue
                    if msg is None:
                        break
                    if msg == "FINALIZE":
                        try:
                            await dg.send(json.dumps({"type": "Finalize"}))
                        except Exception:
                            break
                        continue
                    try:
                        await dg.send(msg)
                    except Exception:
                        break

            async def receiver():
                try:
                    async for raw in dg:
                        try:
                            d = json.loads(raw)
                        except ValueError:
                            continue
                        if d.get("type") != "Results":
                            continue
                        try:
                            alt = d["channel"]["alternatives"][0]
                        except (KeyError, IndexError):
                            continue
                        text = alt.get("transcript", "")
                        if not text:
                            continue
                        is_final = d.get("is_final", False)
                        await on_final(text, is_final)
                except websockets.exceptions.ConnectionClosed:
                    pass

            sender_task = asyncio.create_task(sender())
            try:
                await receiver()
            finally:
                sender_task.cancel()
                try:
                    await sender_task
                except asyncio.CancelledError:
                    pass

    while not stop_event.is_set():
        try:
            await run_session()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            print(f"[dg] error: {e}")
            await on_dg_status("error", str(e))
        if not stop_event.is_set():
            await on_dg_status("reconnecting", None)
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=2.0)
            except asyncio.TimeoutError:
                pass


async def cartesia_speak(text, audio_out, cancel_event, context_id):
    """Stream TTS audio chunks into audio_out queue.

    Returns (ttfa_ms, word_timestamps) where word_timestamps is a list of
    {"word": str, "start_ms": float, "end_ms": float}.

    Does NOT put an end marker; the caller manages queue termination
    (needed for multi-sentence replies where several speak calls feed
    the same queue sequentially).
    """
    url, extra_headers = cartesia_ws_target()
    t_send = None
    ttfa_ms = None
    word_timestamps = []
    try:
        async with websockets.connect(
            url, additional_headers=extra_headers or None,
            max_size=16 * 1024 * 1024,
        ) as ws:
            t_send = time.perf_counter()
            await ws.send(json.dumps({
                "model_id": "sonic-3",
                "transcript": text,
                "voice": {"mode": "id", "id": VOICE_ID},
                "context_id": context_id,
                "output_format": {"container": "raw", "encoding": "pcm_s16le",
                                  "sample_rate": 24000},
                "language": "es",
                "generation_config": {"speed": 0.9},
                "add_timestamps": True,
            }))
            async for raw in ws:
                if cancel_event.is_set():
                    try:
                        await ws.send(json.dumps(
                            {"context_id": context_id, "cancel": True}))
                    except Exception:
                        pass
                    break
                try:
                    d = json.loads(raw)
                except ValueError:
                    continue
                mtype = d.get("type")
                if mtype == "chunk":
                    if ttfa_ms is None:
                        ttfa_ms = (time.perf_counter() - t_send) * 1000.0
                    await audio_out.put(base64.b64decode(d["data"]))
                elif mtype == "timestamps":
                    # {"word_timestamps": {"words": [...], "start": [...], "end": [...]}}
                    wt = d.get("word_timestamps", {})
                    words = wt.get("words", [])
                    starts = wt.get("start", [])
                    ends = wt.get("end", [])
                    for w, s, e in zip(words, starts, ends):
                        word_timestamps.append({
                            "word": w,
                            "start_ms": round(s * 1000, 1),
                            "end_ms": round(e * 1000, 1),
                        })
                elif mtype == "done":
                    break
                elif mtype == "error":
                    print(f"[tts] error: {d}")
                    break
    except Exception as e:
        print(f"[tts] error: {e}")
    return ttfa_ms, word_timestamps


# ------------------------------------------------------- LLM -> speech

async def llm_speak(user_text, audio_out, cancel_event, context_id, state,
                  tts_fn=None, system_prompt=None, llm_provider=None):
    """Stream LLM reply sentence-by-sentence into TTS.

    tts_fn: async (text, audio_out, cancel_event, context_id)
            -> (ttfa_ms, word_timestamps). Defaults to cartesia_speak.
    system_prompt / llm_provider: default to the es-profile Spanish
            persona and the profile's LLM provider.

    Returns dict with provider name and timing measurements.
    Falls back to the stub reply if the LLM call fails.
    """
    import re
    from contextlib import aclosing
    from llm import get_provider
    from llm.persona import SYSTEM_PROMPT

    tts_fn = tts_fn or cartesia_speak
    system_prompt = system_prompt or SYSTEM_PROMPT
    provider = get_provider(llm_provider or profile_llm_provider())
    llm_start = time.perf_counter()
    llm_ttft_ms = None
    tts_ttfa_ms = None
    full_reply = ""

    # In-session conversation history, capped at 6 turns.
    history = state.setdefault("history", [])
    messages = history + [{"role": "user", "content": user_text}]

    async def speak_sentence(sentence):
        nonlocal tts_ttfa_ms
        ttfa, words = await tts_fn(sentence, audio_out,
                                   cancel_event, context_id)
        if tts_ttfa_ms is None and ttfa:
            tts_ttfa_ms = ttfa
        return words

    sentence_buf = ""
    all_words = []  # [{"word","start_ms","end_ms"}] 相对整段回复音频
    audio_offset_ms = 0.0
    try:
        async with aclosing(provider.chat_stream(
                messages, system_prompt, max_tokens=120)) as stream:
            async for delta in stream:
                if cancel_event.is_set():
                    break
                if llm_ttft_ms is None:
                    llm_ttft_ms = (time.perf_counter() - llm_start) * 1000.0
                full_reply += delta
                sentence_buf += delta
                while True:
                    m = re.search(r"[.!?]\s+", sentence_buf)
                    if not m:
                        break
                    sent = sentence_buf[:m.end()].strip()
                    sentence_buf = sentence_buf[m.end():]
                    if sent and not cancel_event.is_set():
                        words = await speak_sentence(sent)
                        for w in words:
                            w["start_ms"] += audio_offset_ms
                            w["end_ms"] += audio_offset_ms
                        all_words.extend(words)
                        if words:
                            audio_offset_ms = max(w["end_ms"] for w in words)
        if sentence_buf.strip() and not cancel_event.is_set():
            words = await speak_sentence(sentence_buf.strip())
            for w in words:
                w["start_ms"] += audio_offset_ms
                w["end_ms"] += audio_offset_ms
            all_words.extend(words)
    except asyncio.CancelledError:
        raise
    except Exception as e:
        print(f"[llm] error: {e}")
        if not full_reply and not cancel_event.is_set():
            from llm.stub import STUB_REPLY
            await speak_sentence(STUB_REPLY)
            full_reply = STUB_REPLY
    finally:
        await audio_out.put(None)  # end marker for pump_audio

    llm_total_ms = (time.perf_counter() - llm_start) * 1000.0
    if full_reply:
        history.append({"role": "user", "content": user_text})
        history.append({"role": "assistant", "content": full_reply})
        if len(history) > 12:
            del history[:-12]

    return {
        "provider": provider.name,
        "llm_ttft_ms": round(llm_ttft_ms, 1) if llm_ttft_ms else None,
        "llm_total_ms": round(llm_total_ms, 1),
        "tts_ttfa_ms": round(tts_ttfa_ms, 1) if tts_ttfa_ms else None,
        "reply": full_reply,
        "words": all_words,
    }


# ------------------------------------------------------- browser session

async def handle_browser(ws):
    """One browser session: mic in, conversation out."""
    print("[session] browser connected")
    mic_queue = asyncio.Queue()
    stop_event = asyncio.Event()
    tts_cancel = asyncio.Event()
    state = {"speaking": False, "final_at": None}

    # Pipeline profile: ?profile=zh|es overrides the server default.
    profile = conn_profile(ws)
    missing = missing_keys_for(profile)
    if missing:
        err = (f"voice profile '{profile}' not configured on server "
               f"(missing: {', '.join(missing)})")
        print(f"[session] {err}")
        try:
            await ws.send(json.dumps({"type": "error", "error": err}))
            await ws.close(1011, err[:120])
        except Exception:
            pass
        return

    if profile == "zh":
        from dashscope_rt import dashscope_listen as asr_listen
        from dashscope_rt import dashscope_speak as _tts_speak
        from llm.persona import SYSTEM_PROMPT_ZH as persona

        async def tts_speak(text, audio_out, cancel_event, context_id):
            # forward TTS server errors to the test page
            return await _tts_speak(text, audio_out, cancel_event,
                                    context_id, on_status=on_dg_status)

        print("[session] profile=zh (DashScope)")
    else:
        asr_listen = deepgram_listen
        tts_speak = cartesia_speak
        from llm.persona import SYSTEM_PROMPT as persona
        print("[session] profile=es (Deepgram/Cartesia)")

    async def on_final(text, is_final):
        await ws.send(json.dumps(
            {"type": "transcript", "text": text, "is_final": is_final}))
        if is_final and not state["speaking"]:
            state["speaking"] = True
            state["final_at"] = time.perf_counter()
            tts_cancel.clear()
            audio_out = asyncio.Queue()
            context_id = uuid.uuid4().hex
            await ws.send(json.dumps({"type": "tts_start"}))
            speak_task = asyncio.create_task(
                llm_speak(text, audio_out, tts_cancel, context_id, state,
                          tts_fn=tts_speak, system_prompt=persona))

            async def pump_audio():
                first_sent_at = None
                while True:
                    chunk = await audio_out.get()
                    if chunk is None:
                        break
                    if tts_cancel.is_set():
                        break
                    try:
                        await ws.send(chunk)  # binary PCM16 24kHz
                        if first_sent_at is None:
                            first_sent_at = time.perf_counter()
                    except Exception:
                        break
                await ws.send(json.dumps({"type": "tts_end"}))
                return first_sent_at

            pump_task = asyncio.create_task(pump_audio())
            result = await speak_task
            first_sent_at = await pump_task
            # first_audio_ms: final transcript -> first audio byte to browser
            first_audio_ms = (
                (first_sent_at - state["final_at"]) * 1000.0
                if first_sent_at and state["final_at"] else None)
            if result.get("reply"):
                try:
                    await ws.send(json.dumps(
                        {"type": "reply", "text": result["reply"]}))
                except Exception:
                    pass
            # 词级时间戳（口型同步用）：tts_start 后下发
            if result.get("words"):
                try:
                    await ws.send(json.dumps(
                        {"type": "words", "words": result["words"]}))
                except Exception:
                    pass
            latency_msg = {
                "type": "latency",
                "llm_provider": result.get("provider"),
                "llm_ttft_ms": result.get("llm_ttft_ms"),
                "llm_total_ms": result.get("llm_total_ms"),
                "tts_ttfa_ms": result.get("tts_ttfa_ms"),
                "first_audio_ms": round(first_audio_ms, 1)
                if first_audio_ms else None,
                "note": f"Phase 3: {result.get('provider')}",
            }
            await ws.send(json.dumps(latency_msg))
            # 服务端也记一份（/api/latency 可查），手机 logcat 拿不到
            record_latency({
                "profile": conn_profile(ws),
                "llm_provider": result.get("provider"),
                "llm_ttft_ms": result.get("llm_ttft_ms"),
                "llm_total_ms": result.get("llm_total_ms"),
                "tts_ttfa_ms": result.get("tts_ttfa_ms"),
                "first_audio_ms": round(first_audio_ms, 1)
                if first_audio_ms else None,
                "user_text": text,
                "reply_chars": len(result.get("reply") or ""),
            })
            state["speaking"] = False

    async def on_dg_status(status, error):
        try:
            await ws.send(json.dumps(
                {"type": "dg_status", "status": status, "error": error}))
        except Exception:
            pass

    dg_task = asyncio.create_task(
        asr_listen(mic_queue, on_final, on_dg_status, stop_event))

    try:
        async for msg in ws:
            if isinstance(msg, bytes):
                # mic PCM16 16kHz mono from browser
                if not state.get("got_audio"):
                    state["got_audio"] = True
                    try:
                        await ws.send(json.dumps(
                            {"type": "debug", "msg": "audio flowing ✓"}))
                    except Exception:
                        pass
                if not state["speaking"]:
                    await mic_queue.put(msg)
                # else: drop mic audio while TTS is playing (half-duplex v1)
            else:
                try:
                    d = json.loads(msg)
                except ValueError:
                    continue
                if d.get("type") == "barge_in" and state["speaking"]:
                    print("[session] barge-in: cancelling TTS")
                    tts_cancel.set()
                    state["speaking"] = False
                elif d.get("type") == "utterance_end":
                    # user released the button: force Deepgram to finalize
                    await mic_queue.put("FINALIZE")
                elif d.get("type") == "stop":
                    break
    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        stop_event.set()
        await mic_queue.put(None)
        dg_task.cancel()
        print("[session] browser disconnected")


async def main():
    profile = get_profile()
    missing = missing_keys_for(profile)
    if missing:
        print(f"missing env for profile={profile}: {', '.join(missing)}")
        raise SystemExit(1)
    # 另一个 profile 没配 key 只是告警：该 profile 的连接会被明确拒绝，
    # 不影响默认 profile 服务。
    other = "es" if profile == "zh" else "zh"
    other_missing = missing_keys_for(other)
    if other_missing:
        print(f"[warn] profile={other} unavailable "
              f"(missing: {', '.join(other_missing)})")
    print(f"serving on http://0.0.0.0:{PORT}/  (ws: /ws)  profile={profile}")
    async with serve(handle_browser, "0.0.0.0", PORT,
                     process_request=process_request,
                     max_size=16 * 1024 * 1024):
        await asyncio.get_running_loop().create_future()


if __name__ == "__main__":
    asyncio.run(main())
