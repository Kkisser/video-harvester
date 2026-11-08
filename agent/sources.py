"""Public Kinescope page adapter; test JSON is an explicit separate adapter."""

import json
import re
from urllib.parse import urljoin
from agent.http import SourceError


def discover(http, source, base_url, video_id):
    url = f'{base_url.rstrip("/")}/{video_id}'
    body, resolved, headers = http.get(url)
    if source == "mock":
        try:
            data = json.loads(body)
        except (ValueError, UnicodeError) as e:
            raise SourceError("invalid_test_json") from e
        if not isinstance(data, dict) or not isinstance(data.get("download_url"), str):
            raise SourceError("invalid_metadata")
        return dict(
            external_id=video_id,
            title=str(data.get("title", "")),
            source_url=url,
            download_url=urljoin(resolved, data["download_url"]),
            mime=data.get("mime", "application/octet-stream"),
            expected_size=data.get("size"),
            expected_checksum=data.get("checksum"),
            kind="direct",
        )
    if source != "kinescope":
        raise SourceError("unknown_source")
    html = body.decode("utf-8", errors="replace")
    match = re.search(r"\b(?:var|let|const)\s+playerOptions\s*=\s*", html)
    if not match:
        raise SourceError("player_metadata_missing")
    try:
        options, _ = json.JSONDecoder().raw_decode(html[match.end() :])
    except ValueError as e:
        raise SourceError("invalid_player_metadata") from e
    if not isinstance(options, dict):
        raise SourceError("invalid_player_metadata")
    playlist = options.get("playlist") or []
    if (
        not isinstance(playlist, list)
        or not playlist
        or not isinstance(playlist[0], dict)
    ):
        raise SourceError("empty_playlist")
    item = playlist[0]
    # Do not fetch keys, license servers, login/cookie gated media or DRM formats.
    if item.get("drm") or item.get("license") or item.get("drmProtected"):
        raise SourceError("protected_media", 403)
    sources = item.get("sources", {})
    if not isinstance(sources, dict):
        raise SourceError("invalid_media_sources")
    meta = item.get("meta") or {}
    if not isinstance(meta, dict):
        raise SourceError("invalid_media_metadata")
    hls = sources.get("hls") or sources.get("shakahls")
    direct = sources.get("mp4")
    selected = hls or direct
    if isinstance(selected, str):
        selected = {"src": selected}
    if not isinstance(selected, dict) or not isinstance(selected.get("src"), str):
        raise SourceError("no_public_media_source")
    if selected.get("drm"):
        raise SourceError("protected_media", 403)
    return dict(
        external_id=video_id,
        title=str(item.get("title") or ""),
        source_url=url,
        download_url=urljoin(resolved, selected["src"]),
        mime="video/mp4",
        expected_size=None,
        expected_checksum=None,
        kind="hls" if hls else "direct",
        duration=meta.get("duration"),
    )
