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
    if request.path in ("/", "/index.html"):
        return Response(200, "OK",
                        Headers([("Content-Type",
                                   "text/html; charset=utf-8")]),
                        INDEX_HTML)
    return None  # anything else -> WebSocket handshake


# ------------------------------------------------------- provider sessions

async def deepgram_listen(send_to_browser, on_final, stop_event):
    """Stream mic audio to Deepgram, forward transcripts, call on_final(text)."""
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
    try:
        async with websockets.connect(
            url, additional_headers=deepgram_auth_headers(),
            max_size=16 * 1024 * 1024,
        ) as dg:
            async def sender():
                while True:
                    msg = await send_to_browser.get()  # PCM16 bytes
                    if msg is None or stop_event.is_set():
                        break
                    await dg.send(msg)
                await stop_event.wait()  # keep-alive until session ends

            async def receiver():
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

            await asyncio.gather(sender(), receiver())
    except Exception as e:
        print(f"[dg] error: {e}")


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
                while True:
                    chunk = await audio_out.get()
                    if chunk is None:
                        break
                    if tts_cancel.is_set():
                        break
                    try:
                        await ws.send(chunk)  # binary PCM16 24kHz
                    except Exception:
                        break
                await ws.send(json.dumps({"type": "tts_end"}))

            pump_task = asyncio.create_task(pump_audio())
            ttfa_ms = await speak_task
            await pump_task
            first_audio_ms = (
                (time.perf_counter() - state["final_at"]) * 1000.0
                if state["final_at"] else None)
            await ws.send(json.dumps({
                "type": "latency",
                "tts_ttfa_ms": round(ttfa_ms, 1) if ttfa_ms else None,
                "first_audio_ms": round(first_audio_ms, 1)
                if first_audio_ms else None,
                "note": "Phase 2: excludes LLM; stub reply",
            }))
            state["speaking"] = False

    dg_task = asyncio.create_task(
        deepgram_listen(mic_queue, on_final, stop_event))

    try:
        async for msg in ws:
            if isinstance(msg, bytes):
                # mic PCM16 16kHz mono from browser
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
