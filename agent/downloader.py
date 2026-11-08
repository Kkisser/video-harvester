"""Resumable files and public unencrypted VOD HLS. FFmpeg reads local files only."""

import fcntl
import hashlib
import json
import os
import re
import shutil
import subprocess
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor
import threading
from pathlib import Path
from urllib.parse import urlparse
import m3u8
from agent.http import SourceError, Cancelled
from common.paths import inside_storage

BUFFER_SIZE = 256 * 1024


def normalize_name(name, max_length=120):
    return (
        re.sub(
            r'[\\/:*?"<>|]', "_", "".join(c for c in (name or "") if c.isprintable())
        )
        .strip()
        .strip(".")[:max_length]
    )


def sha256_file(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def atomic_json(path, value):
    tmp = Path(str(path) + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(value, f, ensure_ascii=False, indent=2)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def _safe_folder(storage_path, video_id):
    if video_id < 0:
        raise ValueError("ID must be nonnegative")
    root = Path(storage_path).resolve()
    folder = (root / str(video_id)[:3] / str(video_id)).resolve()
    if not inside_storage(str(root), str(folder)):
        raise ValueError("Path outside storage")
    folder.mkdir(parents=True, exist_ok=True)
    return folder


def ensure_space(path, reserve, needed=BUFFER_SIZE):
    if shutil.disk_usage(path).free < reserve * 1e9 + needed:
        raise SourceError("disk_reserve", retryable=True)


@contextmanager
def file_lock(folder, http):
    with (folder / ".download.lock").open("a") as f:
        while True:
            http.check()
            try:
                fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                http.sleep(0.1)
        try:
            yield
        finally:
            fcntl.flock(f, fcntl.LOCK_UN)


def fetch_file(
    http,
    url,
    dest,
    expected_size=None,
    expected_checksum=None,
    reserve=0,
    byte_start=0,
    byte_length=None,
    progress=lambda done, total: None,
):
    """Every persisted partial has a digest checkpoint and an optional HTTP validator."""
    dest = Path(dest)
    part = Path(str(dest) + ".part")
    checkpoint = Path(str(part) + ".json")
    receipt = Path(str(dest) + ".receipt.json")
    identity = hashlib.sha256(f"{url}|{byte_start}|{byte_length}".encode()).hexdigest()
    expected_size = byte_length if byte_length is not None else expected_size
    if dest.exists() and receipt.exists():
        try:
            old = json.loads(receipt.read_text())
            if (
                old["identity"] == identity
                and old["size"] == dest.stat().st_size
                and old["checksum"] == sha256_file(dest)
                and (expected_size is None or old["size"] == expected_size)
                and (not expected_checksum or old["checksum"] == expected_checksum)
            ):
                return old
        except (ValueError, KeyError):
            pass
    for path in (dest, part, checkpoint, receipt):
        if path.is_symlink():
            raise SourceError("symlink_rejected")
    state = {}
    resume = 0
    if part.exists():
        try:
            state = json.loads(checkpoint.read_text())
        except (OSError, ValueError):
            state = {}
        if (
            state.get("identity") == identity
            and state.get("size") == part.stat().st_size
            and state.get("checksum") == sha256_file(part)
        ):
            resume = part.stat().st_size
        else:
            part.unlink()
            checkpoint.unlink(missing_ok=True)
    if expected_size is None:
        expected_size = state.get("expected_size")
    if expected_size is not None and resume > expected_size:
        part.unlink(missing_ok=True)
        resume = 0
    if expected_size is not None and resume == expected_size and part.exists():
        checksum = sha256_file(part)
        if not expected_checksum or checksum == expected_checksum:
            os.replace(part, dest)
            checkpoint.unlink(missing_ok=True)
            result = {"identity": identity, "size": expected_size, "checksum": checksum}
            atomic_json(receipt, result)
            return result
        part.unlink()
        resume = 0
    digest = hashlib.sha256()
    if resume:
        with part.open("rb") as f:
            for b in iter(lambda: f.read(1024 * 1024), b""):
                digest.update(b)
    size = resume
    etag = state.get("etag")
    while expected_size is None or size < expected_size:
        http.check()
        offset = byte_start + size
        headers = {}
        requested_end = None
        if size or byte_length is not None:
            # Bounded requests avoid very long CDN responses; the same durable
            # partial file is retained between slices and across process restarts.
            requested_end = (
                min(byte_start + byte_length - 1, offset + 8 * 1024 * 1024 - 1)
                if byte_length is not None
                else None
            )
            headers["Range"] = (
                f'bytes={offset}-{requested_end if requested_end is not None else ""}'
            )
            if etag:
                headers["If-Range"] = etag
        ensure_space(dest.parent, reserve)
        with http.stream(url, headers=headers) as response:
            if response.status_code == 416:
                part.unlink(missing_ok=True)
                checkpoint.unlink(missing_ok=True)
                raise SourceError("range_unsatisfiable", 416, retryable=True)
            response_end = None
            if response.status_code == 206:
                m = re.fullmatch(
                    r"bytes (\d+)-(\d+)/(\d+|\*)",
                    response.headers.get("content-range", ""),
                )
                if (
                    not m
                    or int(m[1]) != offset
                    or (requested_end is not None and int(m[2]) != requested_end)
                ):
                    raise SourceError("bad_content_range", retryable=True)
                response_end = int(m[2]) + 1 - byte_start
                if expected_size is None and m[3] != "*":
                    expected_size = int(m[3]) - byte_start
            elif byte_start or byte_length is not None:
                if byte_start or response.headers.get("content-length") != str(
                    byte_length
                ):
                    raise SourceError("range_not_supported")
                size = 0
                digest = hashlib.sha256()
            else:
                size = 0
                digest = hashlib.sha256()
            if expected_size is None and response.headers.get("content-length"):
                expected_size = size + int(response.headers["content-length"])
            etag = response.headers.get("etag") or response.headers.get("last-modified")
            last_saved = size

            def save(f):
                f.flush()
                os.fsync(f.fileno())
                atomic_json(
                    checkpoint,
                    dict(
                        identity=identity,
                        size=size,
                        checksum=digest.hexdigest(),
                        expected_size=expected_size,
                        etag=etag,
                    ),
                )

            with part.open("ab" if size else "wb") as f:
                try:
                    for block in response.iter_bytes(BUFFER_SIZE):
                        http.check()
                        ensure_space(dest.parent, reserve, len(block))
                        f.write(block)
                        http.metrics({"bytes": len(block)})
                        digest.update(block)
                        size += len(block)
                        progress(size, expected_size)
                        if expected_size is not None and size > expected_size:
                            raise SourceError("oversized_response")
                        if size - last_saved >= 4 * 1024 * 1024:
                            save(f)
                            last_saved = size
                finally:
                    save(f)
            if response_end is not None and size != response_end:
                raise SourceError("short_range", retryable=True)
        if byte_length is None:
            break
    if expected_size is not None and size != expected_size:
        raise SourceError("size_mismatch", retryable=True)
    checksum = digest.hexdigest()
    if expected_checksum and checksum != expected_checksum:
        part.unlink(missing_ok=True)
        checkpoint.unlink(missing_ok=True)
        raise SourceError("checksum_mismatch", retryable=True)
    http.check()
    os.replace(part, dest)
    checkpoint.unlink(missing_ok=True)
    result = {"identity": identity, "size": size, "checksum": checksum}
    atomic_json(receipt, result)
    return result


def read_playlist(http, url):
    body, resolved, _ = http.get(url, kind="download")
    if not body.lstrip().startswith(b"#EXTM3U"):
        raise SourceError("invalid_hls_manifest")
    p = m3u8.loads(body.decode("utf-8-sig"), uri=resolved)
    if any(key and key.method != "NONE" for key in p.keys):
        raise SourceError("encrypted_hls", 403)
    if p.session_keys:
        raise SourceError("encrypted_hls", 403)
    return p


def choose_tracks(http, url, max_height=720):
    master = read_playlist(http, url)
    if not master.is_variant:
        return [master]
    choices = sorted(
        master.playlists, key=lambda p: (p.stream_info.resolution or (0, 0))[1]
    )
    if not choices:
        raise SourceError("empty_hls_master")
    suitable = [
        p for p in choices if (p.stream_info.resolution or (0, 0))[1] <= max_height
    ]
    selected = (suitable or choices)[-1 if suitable else 0]
    tracks = [read_playlist(http, selected.absolute_uri)]
    audios = [
        m
        for m in master.media
        if m.type == "AUDIO" and m.group_id == selected.stream_info.audio and m.uri
    ]
    if audios:
        audio = next((m for m in audios if m.default == "YES"), audios[0])
        tracks.append(read_playlist(http, audio.absolute_uri))
    return tracks


def units(playlist):
    if not playlist.is_endlist:
        raise SourceError("live_stream_not_supported")
    out = []
    previous_end = {}
    last_init = None

    def add(uri, byterange):
        offset = 0
        length = None
        if byterange:
            parts = byterange.split("@")
            length = int(parts[0])
            offset = int(parts[1]) if len(parts) == 2 else previous_end.get(uri, 0)
            previous_end[uri] = offset + length
        out.append((uri, offset, length))

    for seg in playlist.segments:
        if seg.discontinuity:
            raise SourceError("hls_discontinuity_not_supported")
        init = seg.init_section
        key = (init.absolute_uri, init.byterange) if init else None
        if key and key != last_init:
            add(*key)
            last_init = key
        add(seg.absolute_uri, seg.byterange)
    if not out:
        raise SourceError("empty_hls_media")
    # Merge adjacent ranges of the same media object. A long stream can span
    # several objects; never request across an object boundary.
    merged = []

    def object_key(uri):
        p = urlparse(uri)
        # Kinescope's kcd parameter is a delivery hint; the object path identifies
        # bytes. Other query strings remain part of the identity.
        from urllib.parse import parse_qsl, urlencode

        query = urlencode([(k, v) for k, v in parse_qsl(p.query) if k != "kcd"])
        return p.scheme, p.netloc, p.path, query

    for uri, offset, length in out:
        if (
            merged
            and length is not None
            and merged[-1][2] is not None
            and object_key(uri) == object_key(merged[-1][0])
            and offset == merged[-1][1] + merged[-1][2]
        ):
            _, start, total = merged[-1]
            merged[-1] = (uri, start, total + length)
        else:
            merged.append((uri, offset, length))
    return merged


def run_process(args, http):
    with subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE) as p:
        while True:
            try:
                stdout, stderr = p.communicate(timeout=0.2)
                break
            except subprocess.TimeoutExpired:
                if http.cancelled.is_set():
                    p.terminate()
                    try:
                        p.communicate(timeout=3)
                    except subprocess.TimeoutExpired:
                        p.kill()
                        p.communicate()
                    raise Cancelled()
        if p.returncode:
            raise SourceError("media_tool_failed")
        return stdout


def validate_media(path, http, expected_duration=None):
    if not shutil.which("ffprobe"):
        raise SourceError("ffprobe_not_installed")
    data = json.loads(
        run_process(
            [
                "ffprobe",
                "-protocol_whitelist",
                "file,pipe",
                "-v",
                "error",
                "-show_streams",
                "-show_format",
                "-of",
                "json",
                str(path),
            ],
            http,
        )
    )
    if not any(s.get("codec_type") == "video" for s in data.get("streams", [])):
        raise SourceError("no_video_stream")
    duration = float(data.get("format", {}).get("duration", 0))
    if duration <= 0:
        raise SourceError("invalid_media_duration")
    if expected_duration and abs(duration - float(expected_duration)) > max(
        2, float(expected_duration) * 0.005
    ):
        raise SourceError("duration_mismatch", retryable=True)
    return data


def download_media(
    http,
    metadata,
    storage_path,
    video_id,
    reserve=0.1,
    max_height=720,
    progress=lambda done, total: None,
):
    folder = _safe_folder(storage_path, video_id)
    final = folder / "video.mp4"
    sidecar = folder / "metadata.json"
    with file_lock(folder, http):
        if final.exists() and sidecar.exists():
            try:
                old = json.loads(sidecar.read_text())
                if (
                    old.get("source_url") == metadata["source_url"]
                    and old.get("size") == final.stat().st_size
                    and old.get("checksum") == sha256_file(final)
                ):
                    return {
                        "path": str(final),
                        "size": old["size"],
                        "checksum": old["checksum"],
                        "mime": old.get("mime", "video/mp4"),
                        "skipped": True,
                    }
            except (ValueError, KeyError):
                pass
        if metadata["kind"] == "direct":
            result = fetch_file(
                http,
                metadata["download_url"],
                final,
                metadata.get("expected_size"),
                metadata.get("expected_checksum"),
                reserve,
                progress=progress,
            )
            if not http.test_source:
                validate_media(final, http, metadata.get("duration"))
        else:
            if not shutil.which("ffmpeg"):
                raise SourceError("ffmpeg_not_installed")
            tracks = choose_tracks(http, metadata["download_url"], max_height)
            local = []
            total_units = [units(t) for t in tracks]
            full_total = (
                sum(length for group in total_units for _, _, length in group)
                if all(
                    length is not None
                    for group in total_units
                    for _, _, length in group
                )
                else None
            )
            completed_tracks = 0
            for ti, playlist in enumerate(tracks):
                track = folder / f"track-{ti}.bin"
                pieces = units(playlist)
                received = {}
                progress_lock = threading.Lock()

                def fetch_piece(pair):
                    pi, (url, start, length) = pair
                    piece = folder / f"track-{ti}-piece-{pi}.bin"

                    def report(n, total):
                        with progress_lock:
                            received[pi] = n
                            progress(
                                completed_tracks + sum(received.values()), full_total
                            )

                    info = fetch_file(
                        http,
                        url,
                        piece,
                        reserve=reserve,
                        byte_start=start,
                        byte_length=length,
                        progress=report,
                    )
                    report(info["size"], info["size"])
                    return piece, info

                # Transfers can overlap, but the shared HTTP semaphore and coordinator
                # still cap connections and every individual request.
                with ThreadPoolExecutor(max_workers=min(4, len(pieces))) as pool:
                    downloaded = list(pool.map(fetch_piece, enumerate(pieces)))
                with Path(str(track) + ".part").open("wb") as target:
                    for piece, info in downloaded:
                        http.check()
                        ensure_space(folder, reserve, info["size"])
                        with piece.open("rb") as src:
                            while block := src.read(BUFFER_SIZE):
                                http.check()
                                ensure_space(folder, reserve, len(block))
                                target.write(block)
                    target.flush()
                    os.fsync(target.fileno())
                os.replace(str(track) + ".part", track)
                local.append(track)
                completed_tracks += sum(info["size"] for _, info in downloaded)
            ensure_space(folder, reserve, sum(t.stat().st_size for t in local))
            args = ["ffmpeg", "-nostdin", "-v", "error", "-y"]
            for track in local:
                args += ["-protocol_whitelist", "file,pipe", "-i", str(track)]
            args += [
                "-map",
                "0:v:0",
                "-map",
                "1:a:0" if len(local) > 1 else "0:a?",
                "-c",
                "copy",
                "-movflags",
                "+faststart",
                "-f",
                "mp4",
                str(folder / "video.mp4.part"),
            ]
            run_process(args, http)
            validate_media(folder / "video.mp4.part", http, metadata.get("duration"))
            http.check()
            os.replace(folder / "video.mp4.part", final)
            result = {"size": final.stat().st_size, "checksum": sha256_file(final)}
        http.check()
        atomic_json(
            sidecar,
            {
                **metadata,
                "title_normalized": normalize_name(metadata.get("title", "")),
                "id": video_id,
                "size": result["size"],
                "checksum": result["checksum"],
                "mime": "video/mp4",
                "downloaded_at": __import__("datetime")
                .datetime.now(__import__("datetime").timezone.utc)
                .isoformat(),
            },
        )
        # Only remove intermediates after both the verified file and metadata are durable.
        for path in folder.glob("track-*"):
            path.unlink(missing_ok=True)
        return {
            "path": str(final),
            "size": result["size"],
            "checksum": result["checksum"],
            "mime": "video/mp4",
        }


# Compatibility wrapper for legacy direct-file callers. New agents use download_media.
def download(client, base_url, download_url, storage_path, video_id, metadata):
    from urllib.parse import urljoin
    from agent.http import TargetHTTP

    http = TargetHTTP(client=client, test_source=True)
    try:
        f = download_media(
            http,
            {
                **metadata,
                "kind": "direct",
                "source_url": metadata.get("source_url", ""),
                "download_url": urljoin(base_url, download_url),
            },
            storage_path,
            video_id,
            reserve=0,
        )
        return {"status": "completed", **f}
    except SourceError as e:
        return {
            "status": "retry_wait" if e.retryable else "failed",
            "error_code": e.code,
            "http_status": e.status,
        }
