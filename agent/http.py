"""All target requests pass through one admission gate, including HLS segments."""

import ipaddress
import socket
import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from urllib.parse import urlparse, urljoin
import httpx


class Cancelled(Exception):
    pass


class SourceError(Exception):
    def __init__(self, code, status=0, retryable=False, retry_after=None):
        super().__init__(code)
        self.code = code
        self.status = status
        self.retryable = retryable
        self.retry_after = retry_after


def retry_after_seconds(headers):
    value = headers.get("retry-after") if headers else None
    if not value:
        return None
    try:
        return max(0, float(value))
    except ValueError:
        try:
            return max(
                0,
                (
                    parsedate_to_datetime(value) - datetime.now(timezone.utc)
                ).total_seconds(),
            )
        except (ValueError, TypeError, OverflowError):
            return None


def validate_url(url, test_source=False):
    p = urlparse(url)
    if p.username or p.password or p.fragment or p.scheme not in {"http", "https"}:
        raise SourceError("unsafe_url")
    host = (p.hostname or "").lower()
    if test_source:
        if host not in {"localhost", "127.0.0.1", "::1", "testserver"}:
            raise SourceError("test_source_must_be_local")
        return
    if p.scheme != "https" or p.port not in {None, 443}:
        raise SourceError("https_required")
    if not any(
        host == d or host.endswith("." + d)
        for d in ["kinescope.io", "kinescopecdn.net"]
    ):
        raise SourceError("unapproved_media_host")
    try:
        addresses = socket.getaddrinfo(host, 443, type=socket.SOCK_STREAM)
    except OSError as e:
        raise SourceError("dns_error", retryable=True) from e
    if any(not ipaddress.ip_address(a[4][0]).is_global for a in addresses):
        raise SourceError("private_address_rejected")


class TargetHTTP:
    def __init__(
        self,
        timeout=10,
        max_connections=8,
        test_source=False,
        admit=None,
        feedback=None,
        cancelled=None,
        client=None,
        metrics=None,
    ):
        self.timeout = timeout
        self.test_source = test_source
        self.admit = admit or (lambda kind: None)
        self.feedback = feedback or (lambda status: None)
        self.cancelled = cancelled or threading.Event()
        self.metrics = metrics or (lambda delta: None)
        self.semaphore = threading.BoundedSemaphore(max_connections)
        self.client = client or httpx.Client(
            timeout=httpx.Timeout(timeout, connect=min(timeout, 10)),
            follow_redirects=False,
            trust_env=False,
            limits=httpx.Limits(max_connections=max_connections),
        )

    def check(self):
        if self.cancelled.is_set():
            raise Cancelled()

    def sleep(self, delay):
        if self.cancelled.wait(max(0, delay)):
            raise Cancelled()

    @contextmanager
    def stream(self, url, kind="download", headers=None):
        while not self.semaphore.acquire(timeout=0.1):
            self.check()
        try:
            for _ in range(6):
                self.check()
                validate_url(url, self.test_source)
                self.admit(kind)
                self.check()
                self.metrics({"connections": 1})
                try:
                    with self.client.stream(
                        "GET",
                        url,
                        headers={"Accept-Encoding": "identity", **(headers or {})},
                        timeout=self.timeout,
                    ) as response:
                        self.feedback(response.status_code)
                        if response.status_code in (301, 302, 303, 307, 308):
                            location = response.headers.get("location")
                            if not location:
                                raise SourceError(
                                    "redirect_without_location", response.status_code
                                )
                            url = urljoin(url, location)
                            continue
                        if response.status_code not in (200, 206, 416):
                            raise SourceError(
                                f"http_{response.status_code}",
                                response.status_code,
                                response.status_code == 429
                                or response.status_code >= 500,
                                retry_after_seconds(response.headers),
                            )
                        yield response
                        return
                except httpx.HTTPError as e:
                    raise SourceError(type(e).__name__, retryable=True) from e
                finally:
                    self.metrics({"connections": -1})
            raise SourceError("too_many_redirects")
        finally:
            self.semaphore.release()

    def get(self, url, kind="check", max_bytes=4 * 1024 * 1024):
        with self.stream(url, kind) as response:
            parts = []
            size = 0
            for part in response.iter_bytes(64 * 1024):
                self.check()
                size += len(part)
                if size > max_bytes:
                    raise SourceError("response_too_large")
                parts.append(part)
            return b"".join(parts), str(response.url), response.headers

    def close(self):
        self.client.close()
