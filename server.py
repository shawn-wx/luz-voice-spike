#!/usr/bin/env python3
"""Phase 2: live mic demo server — voice conversation loop (single port).

Browser mic (16kHz PCM16 mono) -> Deepgram Nova-3 streaming STT -> stub reply
-> Cartesia Sonic-3 WS TTS -> browser playback, with barge-in and latency
instrumentation.

One process serves everything on $PORT (default 8080):
    GET /          -> test page (web/index.html)
    GET /healthz   -> 200 ok (platform health checks)
    WS  /ws        -> voice session

Run (production, e.g. Fly.io secrets):
    DEEPGRAM_API_KEY=<raw key> CARTESIA_API_KEY=<raw key> \\
        CARTESIA_VOICE_ID=<luz id> PORT=8080 python3 server.py

Run (local dev on the builder VM, uses the credential vault via surrogates):
    DEEPGRAM_API_KEY=vault:custom.deepgram-stt CARTESIA_API_KEY=vault:custom.cartesia \\
        CARTESIA_VOICE_ID=<luz id> python3 server.py
    # NOTE: the deepgram-stt connector stores the full header value
    # "Token <key>", so the vault path sends the bare surrogate as the whole
    # Authorization value (the only pattern the egress proxy replaces).

Phase 2 note: the "LLM" is a stub (fixed warm reply). Phase 3 plugs in the
real LLM with the persona prompt.
"""
import asyncio
import base64
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
PORT = int(os.environ.get("PORT", "8080"))
CARTESIA_VERSION = "2026-03-01"
# Overridable for local tests with mock providers (this dev VM has no
# outbound WebSocket, so real provider WS cannot be exercised here).
DEEPGRAM_WS_BASE = os.environ.get("DEEPGRAM_WS_BASE",
                                  "wss://api.deepgram.com/v1/listen")
CARTESIA_WS_BASE = os.environ.get("CARTESIA_WS_BASE",
                                  "wss://api.cartesia.ai/tts/websocket")

# Fixed warm reply used until Phase 3 plugs in the LLM.
STUB_REPLY = "¡Hola! Qué gusto escucharte. Cuéntame, ¿cómo va tu día?"


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
    if request.path == "/healthz":
        return Response(200, "OK",
                        Headers([("Content-Type", "text/plain")]), b"ok")
    if request.path == "/debug/deepseek":
        body = json.dumps(await debug_deepseek(),
                          ensure_ascii=False).encode("utf-8")
        return Response(200, "OK",
                        Headers([("Content-Type",
                                   "application/json; charset=utf-8")]),
                        body)
    if request.path in ("/", "/index.html"):
        return Response(200, "OK",
                        Headers([("Content-Type",
                                   "text/html; charset=utf-8")]),
                        INDEX_HTML)
    return None  # anything else -> WebSocket handshake


async def debug_deepseek() -> dict:
    """Connectivity probe from this machine to api.deepseek.com.

    No API key needed for the network-level checks: an HTTP 401/400 proves
    the full path works. If DEEPSEEK_API_KEY is set, also runs a minimal
    chat request and reports TTFT.
    """
    import socket
    import ssl as ssl_mod
    out = {"host": "api.deepseek.com", "port": 443}
    # DNS
    try:
        t0 = time.perf_counter()
        ip = socket.gethostbyname("api.deepseek.com")
        out["dns_ms"] = round((time.perf_counter() - t0) * 1000, 1)
        out["resolved_ip"] = ip
    except Exception as e:
        out["dns_error"] = str(e)
        return out
    # TCP + TLS
    try:
        t0 = time.perf_counter()
        ctx = ssl_mod.create_default_context()
        raw = socket.create_connection(("api.deepseek.com", 443), timeout=10)
        tls = ctx.wrap_socket(raw, server_hostname="api.deepseek.com")
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
                "https://api.deepseek.com/chat/completions",
                json={"model": "deepseek-chat",
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
    ds_key = os.environ.get("DEEPSEEK_API_KEY", "")
    if ds_key and not ds_key.startswith("vault:"):
        try:
            from llm import get_provider
            from llm.persona import SYSTEM_PROMPT
            provider = get_provider("deepseek")
            t0 = time.perf_counter()
            ttft_ms = None
            chars = 0
            async for delta in provider.chat_stream(
                    [{"role": "user", "content": "Hola, ¿cómo estás?"}],
                    SYSTEM_PROMPT, max_tokens=60):
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
        out["llm_skipped"] = "DEEPSEEK_API_KEY not set"
    return out


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
    """Stream TTS audio chunks into audio_out queue. Returns ttfa_ms."""
    url, extra_headers = cartesia_ws_target()
    t_send = None
    ttfa_ms = None
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
                elif mtype == "done":
                    break
                elif mtype == "error":
                    print(f"[tts] error: {d}")
                    break
    except Exception as e:
        print(f"[tts] error: {e}")
    finally:
        await audio_out.put(None)  # end marker
    return ttfa_ms


# ------------------------------------------------------- browser session

async def handle_browser(ws):
    """One browser session: mic in, conversation out."""
    print("[session] browser connected")
    mic_queue = asyncio.Queue()
    stop_event = asyncio.Event()
    tts_cancel = asyncio.Event()
    state = {"speaking": False, "final_at": None}

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
                cartesia_speak(STUB_REPLY, audio_out, tts_cancel, context_id))

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
            ttfa_ms = await speak_task
            first_sent_at = await pump_task
            # first_audio_ms: final transcript -> first audio byte to browser
            first_audio_ms = (
                (first_sent_at - state["final_at"]) * 1000.0
                if first_sent_at and state["final_at"] else None)
            await ws.send(json.dumps({
                "type": "latency",
                "tts_ttfa_ms": round(ttfa_ms, 1) if ttfa_ms else None,
                "first_audio_ms": round(first_audio_ms, 1)
                if first_audio_ms else None,
                "note": "Phase 2: excludes LLM; stub reply",
            }))
            state["speaking"] = False

    async def on_dg_status(status, error):
        try:
            await ws.send(json.dumps(
                {"type": "dg_status", "status": status, "error": error}))
        except Exception:
            pass

    dg_task = asyncio.create_task(
        deepgram_listen(mic_queue, on_final, on_dg_status, stop_event))

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
    missing = [k for k, v in
               [("DEEPGRAM_API_KEY", DG_KEY), ("CARTESIA_API_KEY", CART_KEY),
                ("CARTESIA_VOICE_ID", VOICE_ID)] if not v]
    if missing:
        print(f"missing env: {', '.join(missing)}")
        raise SystemExit(1)
    print(f"serving on http://0.0.0.0:{PORT}/  (ws: /ws)")
    async with serve(handle_browser, "0.0.0.0", PORT,
                     process_request=process_request,
                     max_size=16 * 1024 * 1024):
        await asyncio.get_running_loop().create_future()


if __name__ == "__main__":
    asyncio.run(main())
