"""Локальный управляемый двойник видеосервиса."""
import hashlib
import threading
import time

from fastapi import FastAPI, Query, Request
from fastapi.responses import JSONResponse, Response


app = FastAPI(title="Mock Target")
RATE_LIMIT_PER_SECOND = 60
_lock = threading.Lock()
_recent_requests = []


def file_bytes(video_id: int) -> bytes:
    size = 32 * 1024 + (video_id % 5) * 64 * 1024
    pattern = f"FAKE_VIDEO_{video_id}_".encode()
    return (pattern * (size // len(pattern) + 1))[:size]


def classify(video_id: int):
    if video_id % 70 == 0:
        return "forbidden"
    if video_id % 91 == 0:
        return "server_error"
    if video_id % 77 == 0:
        return "slow"
    if video_id % 7 != 0:
        return "not_found"
    return "found"


def rate_limited():
    now = time.monotonic()
    with _lock:
        _recent_requests[:] = [stamp for stamp in _recent_requests if now - stamp < 1]
        _recent_requests.append(now)
        return len(_recent_requests) > RATE_LIMIT_PER_SECOND


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/download/{video_id}")
def download_file(video_id: int, request: Request, disconnect_once: bool = False):
    body = file_bytes(video_id)
    total = len(body)
    range_header = request.headers.get("range", "")
    start = 0
    if range_header.startswith("bytes="):
        try:
            start = int(range_header[6:].split("-", 1)[0])
        except ValueError:
            return Response(status_code=416)
    if start >= total:
        return Response(status_code=416)

    # Первый запрос для специальных ID обещает полный размер, но отдаёт половину.
    # HTTP-клиент увидит обрыв. Повтор с Range получит оставшуюся часть.
    if disconnect_once and start == 0:
        partial = body[: total // 2]
        return Response(
            content=partial,
            status_code=200,
            media_type="video/mp4",
            headers={"Content-Length": str(total), "Accept-Ranges": "bytes"},
        )

    chunk = body[start:]
    status = 206 if start else 200
    headers = {
        "Content-Length": str(len(chunk)),
        "Accept-Ranges": "bytes",
    }
    if start:
        headers["Content-Range"] = f"bytes {start}-{total - 1}/{total}"
    return Response(content=chunk, status_code=status, media_type="video/mp4", headers=headers)


@app.get("/{video_id}")
def check_video(video_id: int, force_status: int | None = Query(default=None)):
    if force_status in {401, 403, 404, 429, 500}:
        headers = {"Retry-After": "1"} if force_status == 429 else None
        return JSONResponse({"forced": force_status}, status_code=force_status, headers=headers)
    if rate_limited():
        return JSONResponse(
            {"error": "rate limited"}, status_code=429, headers={"Retry-After": "1"},
        )
    kind = classify(video_id)
    if kind == "not_found":
        return JSONResponse({"error": "not found"}, status_code=404)
    if kind == "forbidden":
        return JSONResponse({"error": "forbidden"}, status_code=403)
    if kind == "server_error":
        return JSONResponse({"error": "internal"}, status_code=500)
    if kind == "slow":
        time.sleep(2)

    body = file_bytes(video_id)
    checksum = hashlib.sha256(body).hexdigest()
    suffix = "?disconnect_once=true" if video_id % 119 == 0 else ""
    return {
        "id": video_id,
        "title": f"Учебный ролик {video_id}",
        "download_url": f"/download/{video_id}{suffix}",
        "mime": "video/mp4",
        "size": len(body),
        "checksum": checksum,
    }
