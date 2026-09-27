"""Безопасная загрузка файла с докачкой и проверкой целостности."""
import hashlib
import json
import os
import re
from pathlib import Path

import httpx


BUFFER_SIZE = 64 * 1024


def normalize_name(name: str, max_length: int = 120) -> str:
    name = "".join(char for char in (name or "") if char.isprintable())
    name = re.sub(r'[\\/:*?"<>|]', "_", name).strip().strip(".")
    return name[:max_length]


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(BUFFER_SIZE), b""):
            digest.update(block)
    return digest.hexdigest()


def _safe_folder(storage_path: str, video_id: int) -> Path:
    if video_id < 0:
        raise ValueError("идентификатор должен быть неотрицательным")
    root = Path(storage_path).resolve()
    folder = (root / str(video_id)[:3] / str(video_id)).resolve()
    if root != folder and root not in folder.parents:
        raise ValueError("путь вышел за пределы каталога хранения")
    folder.mkdir(parents=True, exist_ok=True)
    return folder


def download(client: httpx.Client, base_url: str, download_url: str,
             storage_path: str, video_id: int, metadata: dict):
    folder = _safe_folder(storage_path, video_id)
    final_path = folder / "video.mp4"
    part_path = folder / "video.mp4.part"
    metadata_path = folder / "metadata.json"
    expected_size = metadata.get("expected_size")
    expected_checksum = metadata.get("expected_checksum")

    if final_path.exists():
        size = final_path.stat().st_size
        checksum = sha256_file(final_path)
        size_ok = expected_size is None or size == expected_size
        hash_ok = not expected_checksum or checksum == expected_checksum
        if size_ok and hash_ok:
            return {"status": "completed", "path": str(final_path),
                    "size": size, "checksum": checksum, "skipped": True}
        final_path.unlink()

    resume_from = part_path.stat().st_size if part_path.exists() else 0
    if expected_size is not None and resume_from > expected_size:
        part_path.unlink()
        resume_from = 0
    headers = {"Range": f"bytes={resume_from}-"} if resume_from else {}
    url = download_url if download_url.startswith("http") else f"{base_url}{download_url}"

    try:
        with client.stream("GET", url, headers=headers, timeout=60) as response:
            if response.status_code not in (200, 206):
                return {"status": "failed", "http_status": response.status_code,
                        "error_code": "download_http_status"}
            append = resume_from > 0 and response.status_code == 206
            if append:
                content_range = response.headers.get("content-range", "")
                if not content_range.startswith(f"bytes {resume_from}-"):
                    return {"status": "retry_wait", "error_code": "bad_content_range"}
            mode = "ab" if append else "wb"
            with part_path.open(mode) as stream:
                for block in response.iter_bytes(BUFFER_SIZE):
                    stream.write(block)
    except (httpx.HTTPError, OSError) as error:
        return {"status": "retry_wait", "error_code": type(error).__name__}

    size = part_path.stat().st_size
    if expected_size is not None and size != expected_size:
        return {"status": "retry_wait", "error_code": "size_mismatch", "size": size}
    checksum = sha256_file(part_path)
    if expected_checksum and checksum != expected_checksum:
        part_path.unlink(missing_ok=True)
        return {"status": "retry_wait", "error_code": "checksum_mismatch"}

    os.replace(part_path, final_path)
    temporary_metadata = metadata_path.with_suffix(".json.part")
    with temporary_metadata.open("w", encoding="utf-8") as stream:
        json.dump({
            "id": video_id,
            "title": metadata.get("title"),
            "title_normalized": normalize_name(metadata.get("title", "")),
            "source_url": metadata.get("source_url"),
            "download_url": download_url,
            "mime": metadata.get("mime"),
            "size": size,
            "checksum": checksum,
        }, stream, ensure_ascii=False, indent=2)
    os.replace(temporary_metadata, metadata_path)
    return {"status": "completed", "path": str(final_path), "size": size,
            "checksum": checksum, "mime": metadata.get("mime", "")}
