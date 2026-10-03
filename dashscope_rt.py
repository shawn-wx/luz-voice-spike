"""DashScope realtime WebSocket: ASR + TTS for the zh-CN pipeline profile.

International endpoint (Singapore):
    wss://dashscope-intl.aliyuncs.com/api-ws/v1/realtime?model=<model>
Beijing endpoint:
    wss://dashscope.aliyuncs.com/api-ws/v1/realtime?model=<model>
Auth headers:  Authorization: Bearer <DASHSCOPE_API_KEY>
               OpenAI-Beta: realtime=v1

Models:
    ASR: qwen3-asr-flash-realtime  (mic PCM16 16kHz in, transcripts out)
    TTS: qwen3-tts-flash-realtime  (text in, PCM16 24kHz audio out)

ASR session.update schema (verified against the live server):
    {"type": "session.update", "session": {
        "modalities": ["text"],
        "input_audio_format": "pcm",   # NOT "pcm16": the server rejects
                                       # it with "Audio format is not valid
                                       # 'pcm16'!"
        "input_audio_transcription": {"sample_rate": 16000,
                                      "language": "zh"},
        "turn_detection": None}}          # null = manual mode; we commit
                                          # explicitly on FINALIZE
Server transcription events:
    conversation.item.input_audio_transcription.text      (partial)
    conversation.item.input_audio_transcription.completed (final)

DASHSCOPE_WS_BASE env overrides the endpoint for local mock tests
(same pattern as DEEPGRAM_WS_BASE / CARTESIA_WS_BASE).
"""
import asyncio
import base64
import json
import os
import time
import urllib.parse
import uuid

import websockets

DS_KEY = os.environ.get("DASHSCOPE_API_KEY", "")
DASHSCOPE_WS_BASE = os.environ.get(
    "DASHSCOPE_WS_BASE",
    "wss://dashscope-intl.aliyuncs.com/api-ws/v1/realtime")
ASR_MODEL = os.environ.get("DASHSCOPE_ASR_MODEL", "qwen3-asr-flash-realtime")
ASR_LANG = os.environ.get("DASHSCOPE_ASR_LANG", "zh")
TTS_MODEL = os.environ.get("DASHSCOPE_TTS_MODEL", "qwen3-tts-flash-realtime")
TTS_VOICE = os.environ.get("DASHSCOPE_VOICE", "Cherry")  # verify in console


def _headers() -> dict:
    return {
        "Authorization": f"Bearer {DS_KEY}",
        "OpenAI-Beta": "realtime=v1",
    }


def _event(type_: str, **fields) -> str:
    return json.dumps({"event_id": uuid.uuid4().hex[:12],
                       "type": type_, **fields})


def _extract_transcript(d: dict):
    """Return (text, is_final or None) from a transcription-ish event."""
    t = d.get("type", "")
    # DashScope official: .text = partial, .completed = final
    if t.endswith("input_audio_transcription.text"):
        return d.get("text", ""), False
    if t.endswith("input_audio_transcription.completed"):
        return d.get("transcript", ""), True
    # defensive variants seen in the wild
    if t.endswith("input_audio_transcription.delta"):
        return d.get("delta", ""), False
    if "transcript" in d and isinstance(d["transcript"], str):
        return d["transcript"], t.endswith((".done", ".completed"))
    return None, None


def _close_info(e: Exception) -> str:
    code = getattr(e, "code", "?")
    reason = getattr(e, "reason", "") or ""
    return f"code={code} reason={reason!r}"[:160]


async def dashscope_listen(mic_to_ds, on_final, on_ds_status, stop_event):
    """Stream mic audio to DashScope realtime ASR (manual-commit mode).

    Same contract as deepgram_listen(): forwards (text, is_final) to
    on_final, reconnects until stop_event. Browser sends PCM16 16kHz mono.
    Server error events and WS close reasons are forwarded via
    on_ds_status("error", ...) so the test page shows the real cause.
    """
    url = (f"{DASHSCOPE_WS_BASE}?"
           f"{urllib.parse.urlencode({'model': ASR_MODEL})}")

    async def run_session():
        try:
            ws = await websockets.connect(
                url, additional_headers=_headers(),
                max_size=16 * 1024 * 1024)
        except Exception as e:
            await on_ds_status("error", f"connect failed: {e}"[:200])
            return
        async with ws:
            await on_ds_status("connected", None)
            # 1. wait for session.created
            try:
                async for raw in ws:
                    try:
                        d = json.loads(raw)
                    except ValueError:
                        continue
                    if d.get("type") == "session.created":
                        break
                    if d.get("type") == "error":
                        await on_ds_status("error",
                                           f"handshake: {d}"[:200])
                        return
            except websockets.exceptions.ConnectionClosed as e:
                await on_ds_status("error",
                                   f"closed before session.created: "
                                   f"{_close_info(e)}")
                return
            # 2. configure session (manual mode), wait for session.updated
            await ws.send(_event("session.update", session={
                "modalities": ["text"],
                # NOTE: server rejects "pcm16" here ("Audio format is not
                # valid 'pcm16'!"); the accepted value is "pcm".
                "input_audio_format": "pcm",
                "input_audio_transcription": {"sample_rate": 16000,
                                              "language": ASR_LANG},
                "turn_detection": None,
            }))
            try:
                async for raw in ws:
                    try:
                        d = json.loads(raw)
                    except ValueError:
                        continue
                    if d.get("type") == "session.updated":
                        break
                    if d.get("type") == "error":
                        await on_ds_status(
                            "error", f"session.update rejected: {d}"[:300])
                        return
            except websockets.exceptions.ConnectionClosed as e:
                await on_ds_status("error",
                                   f"closed after session.update: "
                                   f"{_close_info(e)}")
                return

            async def sender():
                n_appends = 0
                append_bytes = 0
                try:
                    while not stop_event.is_set():
                        try:
                            msg = await asyncio.wait_for(mic_to_ds.get(),
                                                         timeout=15.0)
                        except asyncio.TimeoutError:
                            continue
                        if msg is None:
                            break
                        if msg == "FINALIZE":
                            await on_ds_status(
                                "debug",
                                f"commit: {n_appends} appends, "
                                f"{append_bytes} bytes buffered")
                            try:
                                await ws.send(_event(
                                    "input_audio_buffer.commit"))
                            except Exception:
                                break
                            continue
                        try:
                            await ws.send(_event(
                                "input_audio_buffer.append",
                                audio=base64.b64encode(
                                    msg).decode("ascii")))
                            n_appends += 1
                            append_bytes += len(msg)
                        except Exception:
                            break
                finally:
                    try:
                        await ws.close()
                    except Exception:
                        pass

            async def receiver():
                try:
                    async for raw in ws:
                        try:
                            d = json.loads(raw)
                        except ValueError:
                            continue
                        if d.get("type") == "error":
                            await on_ds_status(
                                "error", f"asr: {d}"[:300])
                            continue
                        text, is_final = _extract_transcript(d)
                        if text:
                            await on_final(text,
                                           True if is_final else False)
                except websockets.exceptions.ConnectionClosed as e:
                    await on_ds_status(
                        "error", f"asr closed: {_close_info(e)}")

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
            print(f"[ds-asr] error: {e}")
            await on_ds_status("error", str(e)[:200])
        if not stop_event.is_set():
            await on_ds_status("reconnecting", None)
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=2.0)
            except asyncio.TimeoutError:
                pass


async def dashscope_speak(text, audio_out, cancel_event, context_id):
    """Stream TTS audio chunks into audio_out queue.

    Same contract as cartesia_speak(): returns (ttfa_ms, word_timestamps).
    The realtime TTS API does not provide word timestamps, so the list is
    always empty (client falls back to volume-envelope lip sync).
    Does NOT put an end marker; the caller manages queue termination.
    """
    url = (f"{DASHSCOPE_WS_BASE}?"
           f"{urllib.parse.urlencode({'model': TTS_MODEL})}")
    t_send = None
    ttfa_ms = None
    try:
        async with websockets.connect(
            url, additional_headers=_headers(),
            max_size=16 * 1024 * 1024,
        ) as ws:
            # wait for session.created, then set voice
            try:
                async for raw in ws:
                    try:
                        d = json.loads(raw)
                    except ValueError:
                        continue
                    if d.get("type") == "session.created":
                        break
                    if d.get("type") == "error":
                        print(f"[ds-tts] session error: {d}")
                        return None, []
            except websockets.exceptions.ConnectionClosed:
                return None, []
            await ws.send(_event("session.update", session={
                "voice": TTS_VOICE,
                "output_audio_format": "pcm16",
            }))
            # one response turn per sentence
            await ws.send(_event("conversation.item.create", item={
                "type": "message", "role": "user", "content": [{
                    "type": "input_text", "text": text}]}))
            t_send = time.perf_counter()
            await ws.send(_event("response.create", response={
                "modalities": ["audio", "text"]}))
            async for raw in ws:
                if cancel_event.is_set():
                    try:
                        await ws.send(_event("response.cancel"))
                    except Exception:
                        pass
                    break
                try:
                    d = json.loads(raw)
                except ValueError:
                    continue
                mtype = d.get("type", "")
                if mtype == "response.audio.delta":
                    if ttfa_ms is None and t_send:
                        ttfa_ms = (time.perf_counter() - t_send) * 1000.0
                    b64 = d.get("delta", "")
                    if b64:
                        await audio_out.put(base64.b64decode(b64))
                elif mtype in ("response.done", "response.audio.done"):
                    if mtype == "response.done":
                        break
                elif mtype == "error":
                    print(f"[ds-tts] error: {d}")
                    break
    except Exception as e:
        print(f"[ds-tts] error: {e}")
    return ttfa_ms, []
