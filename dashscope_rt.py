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


async def dashscope_speak(text, audio_out, cancel_event, context_id,
                        on_status=None):
    """Stream TTS audio chunks into audio_out queue.

    DashScope-native TTS realtime flow (per provider docs):
        session.update {voice, response_format, sample_rate, mode}
        -> input_text_buffer.append {text}
        -> input_text_buffer.commit
        -> response.audio.delta (base64 pcm16 24kHz) ... -> response.done

    Same contract as cartesia_speak(): returns (ttfa_ms, word_timestamps).
    The realtime TTS API does not provide word timestamps, so the list is
    always empty (client falls back to volume-envelope lip sync).
    Does NOT put an end marker; the caller manages queue termination.
    on_status(status, error): optional callback; server errors are
    forwarded so the test page shows the real cause.
    """
    async def report(status, error=None):
        if on_status:
            try:
                await on_status(status, error)
            except Exception:
                pass
        else:
            print(f"[ds-tts] {status}: {error}")

    url = (f"{DASHSCOPE_WS_BASE}?"
           f"{urllib.parse.urlencode({'model': TTS_MODEL})}")
    t_send = None
    ttfa_ms = None
    try:
        async with websockets.connect(
            url, additional_headers=_headers(),
            max_size=16 * 1024 * 1024,
        ) as ws:
            # wait for session.created, then configure voice
            try:
                async for raw in ws:
                    try:
                        d = json.loads(raw)
                    except ValueError:
                        continue
                    if d.get("type") == "session.created":
                        break
                    if d.get("type") == "error":
                        await report("error", f"session: {d}"[:300])
                        return None, []
            except websockets.exceptions.ConnectionClosed as e:
                await report("error", f"closed before session.created: "
                                      f"{_close_info(e)}")
                return None, []
            await ws.send(_event("session.update", session={
                "voice": TTS_VOICE,
                "response_format": "pcm",  # pcm16 rejected; server allows
                "sample_rate": 24000,      # [mp3, wav, pcm, opus]
                "mode": "server_commit",
            }))
            # wait for session.updated (surface rejection immediately)
            try:
                async for raw in ws:
                    try:
                        d = json.loads(raw)
                    except ValueError:
                        continue
                    if d.get("type") == "session.updated":
                        break
                    if d.get("type") == "error":
                        await report("error",
                                     f"session.update rejected: {d}"[:300])
                        return None, []
            except websockets.exceptions.ConnectionClosed as e:
                await report("error", f"closed after session.update: "
                                      f"{_close_info(e)}")
                return None, []
            # one response turn per sentence
            t_send = time.perf_counter()
            await ws.send(_event("input_text_buffer.append", text=text))
            await ws.send(_event("input_text_buffer.commit"))
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
                    await report("error", f"tts: {d}"[:300])
                    break
    except Exception as e:
        await report("error", str(e)[:200])
    return ttfa_ms, []


# ---------------------------------------------------------------- HTTP TTS
# 非流式 HTTP 版 Qwen3-TTS，用作 WebSocket 版失败时的兜底。
# 无建连限流问题，可靠性高；整句一次请求，不做逐句流式。

DASHSCOPE_HTTP_BASE = os.environ.get(
    "DASHSCOPE_HTTP_BASE",
    "https://dashscope-intl.aliyuncs.com")
TTS_HTTP_MODEL = os.environ.get("DASHSCOPE_TTS_HTTP_MODEL", "qwen3-tts-flash")


def _http_post_json(url, payload, headers, timeout=30):
    """同步 POST JSON，返回解析后的 dict。跑在线程池里。"""
    import urllib.request
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers=headers, method="POST")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _http_get_bytes(url, timeout=30):
    import urllib.request
    req = urllib.request.Request(url, method="GET")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read()


def _wav_to_pcm16(wav_bytes):
    """解析 WAV，返回 24kHz 单声道 16bit PCM bytes。"""
    import struct
    import array
    if len(wav_bytes) < 44 or wav_bytes[0:4] != b"RIFF" or wav_bytes[8:12] != b"WAVE":
        raise ValueError("not a WAV file")
    pos = 12
    fmt = None
    data = None
    while pos + 8 <= len(wav_bytes):
        chunk_id = wav_bytes[pos:pos+4]
        chunk_size = struct.unpack("<I", wav_bytes[pos+4:pos+8])[0]
        chunk_data = wav_bytes[pos+8:pos+8+chunk_size]
        if chunk_id == b"fmt ":
            fmt = chunk_data
        elif chunk_id == b"data":
            data = chunk_data
            break
        pos += 8 + chunk_size + (chunk_size % 2)
    if fmt is None or data is None:
        raise ValueError("WAV missing fmt/data")
    audio_fmt, channels, sample_rate, _, _, bits = struct.unpack("<HHIIHH", fmt[:16])
    if audio_fmt != 1:
        raise ValueError(f"WAV not PCM (fmt={audio_fmt})")
    pcm = data
    le = struct.pack("=H", 1) == b"\x01\x00"
    if bits == 8:
        a = array.array("B", pcm)
        pcm = b"".join(struct.pack("<h", (s - 128) * 256) for s in a)
    elif bits == 32:
        a = array.array("i", pcm)
        if not le:
            a.byteswap()
        pcm = b"".join(struct.pack("<h", s >> 16) for s in a)
    elif bits != 16:
        raise ValueError(f"unsupported bits={bits}")
    if channels == 2:
        a = array.array("h", pcm)
        if not le:
            a.byteswap()
        mono = array.array("h", ((a[i] + a[i+1]) // 2 for i in range(0, len(a), 2)))
        pcm = mono.tobytes()
    elif channels != 1:
        raise ValueError(f"unsupported channels={channels}")
    if sample_rate != 24000:
        a = array.array("h", pcm)
        if not le:
            a.byteswap()
        ratio = 24000 / sample_rate
        new_len = int(len(a) * ratio)
        res = array.array("h", [0]) * new_len
        for i in range(new_len):
            src = i / ratio
            i0 = int(src)
            i1 = min(i0 + 1, len(a) - 1)
            frac = src - i0
            res[i] = int(a[i0] * (1 - frac) + a[i1] * frac)
        pcm = res.tobytes()
    return pcm


async def dashscope_tts_http(text, audio_out, cancel_event, context_id,
                             on_status=None):
    """HTTP 版 TTS（兜底）：整句一次请求，无 WebSocket 建连。
    返回 (ttfa_ms, [])，无词级时间戳。"""
    async def report(status, error=None):
        if on_status:
            try:
                await on_status(status, error)
            except Exception:
                pass
        else:
            print(f"[ds-tts-http] {status}: {error}")

    t0 = time.perf_counter()
    ttfa_ms = None
    try:
        url = (f"{DASHSCOPE_HTTP_BASE}/api/v1/services/aigc/"
               f"multimodal-generation/generation")
        headers = {
            "Authorization": f"Bearer {DS_KEY}",
            "Content-Type": "application/json",
        }
        payload = {
            "model": TTS_HTTP_MODEL,
            "input": {
                "text": text,
                "voice": TTS_VOICE,
                "language_type": "Auto",
            },
        }
        resp = await asyncio.to_thread(_http_post_json, url, payload, headers)
        audio_url = (resp.get("output", {}).get("audio", {}).get("url", ""))
        if not audio_url:
            await report("error", f"no audio url: {str(resp)[:300]}")
            return None, []
        wav_bytes = await asyncio.to_thread(_http_get_bytes, audio_url)
        pcm = await asyncio.to_thread(_wav_to_pcm16, wav_bytes)
        chunk_size = 24000 * 2 // 10  # 每块约 100ms
        for i in range(0, len(pcm), chunk_size):
            if cancel_event.is_set():
                break
            if ttfa_ms is None:
                ttfa_ms = (time.perf_counter() - t0) * 1000.0
            await audio_out.put(pcm[i:i+chunk_size])
        return ttfa_ms, []
    except Exception as e:
        await report("error", str(e)[:200])
        return None, []
