"""Protocol 2 agent: control traffic stays independent from media transfers."""

import argparse
import fcntl
import logging
import os
import random
import shutil
import signal
import threading
import time
from collections import Counter, deque
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from datetime import datetime, timezone
from pathlib import Path
import httpx
import psutil
import yaml
from dotenv import load_dotenv
from common import PROTOCOL_VERSION
from agent.http import TargetHTTP, SourceError, Cancelled
from agent.sources import discover
from agent.downloader import download_media

log = logging.getLogger("agent")


def backoff_delay(attempt, base=0.5, cap=30):
    return min(cap, base * 2**attempt) + random.uniform(0, base)


def now():
    return datetime.now(timezone.utc).replace(tzinfo=None).isoformat()


# Retained small utility for local callers; production admission is coordinated.
class RateLimiter:
    def __init__(self, rps):
        self.lock = threading.Lock()
        self.last = 0
        self.set_rps(rps)

    def set_rps(self, rps):
        self.rps = max(float(rps), 0.01)

    def wait(self):
        with self.lock:
            time.sleep(max(0, 1 / self.rps - (time.monotonic() - self.last)))
            self.last = time.monotonic()


def load_agent_config():
    path = Path(os.environ.get("CONFIG_PATH", "config.yaml"))
    if not path.exists():
        path = Path("config.example.yaml")
    return (
        (yaml.safe_load(path.read_text()) or {}).get("agent", {})
        if path.exists()
        else {}
    )


class Agent:
    def __init__(
        self,
        name,
        job_id,
        coordinator_url,
        token,
        storage_path="./data",
        threads=4,
        heartbeat_seconds=3,
        ca_cert=None,
        test_target_url="http://localhost:9000",
        max_height=720,
        once=False,
        **legacy,
    ):
        self.name = name
        self.job_id = job_id
        self.url = coordinator_url.rstrip("/")
        self.token = token
        self.storage_path = str(Path(storage_path).resolve())
        Path(self.storage_path).mkdir(parents=True, exist_ok=True)
        self.threads = threads
        self.heartbeat_seconds = heartbeat_seconds
        self.test_target_url = test_target_url
        self.max_height = max_height
        self.once = once
        headers = {"X-Agent-Token": token, "X-Agent-Name": name}
        verify = ca_cert or True
        self.client = httpx.Client(
            headers=headers, verify=verify, timeout=5, trust_env=False
        )
        self.heartbeat_client = httpx.Client(
            headers=headers, verify=verify, timeout=5, trust_env=False
        )
        self.shutdown = threading.Event()
        self.work_cancel = threading.Event()
        self.lock = threading.RLock()
        self.chunk = None
        self.paused = False
        self.lease_seconds = 30
        self.last_control = time.monotonic()
        self.current = {}
        self.processed = 0
        self.bytes_done = 0
        self.error_count = 0
        self.connections = 0
        self.retries = 0
        self.status_counts = Counter()
        self.recent = deque(maxlen=200)
        self.check_durations = deque(maxlen=200)
        self.download_durations = deque(maxlen=200)
        self.started = time.monotonic()
        self.last_metrics = time.monotonic()
        self.last_net = psutil.net_io_counters()
        self.last_disk = psutil.disk_io_counters()
        self.last_processed = 0
        self.last_bytes = 0

    def _request(self, path, payload, heartbeat=False, retry=True):
        client = self.heartbeat_client if heartbeat else self.client
        deadline = time.monotonic() + min(self.lease_seconds / 2, 15)
        while True:
            try:
                r = client.post(self.url + path, json=payload)
                if r.status_code in (401, 403, 409):
                    raise Cancelled(f"control_{r.status_code}")
                r.raise_for_status()
                return r.json()
            except httpx.HTTPError:
                if not retry or time.monotonic() >= deadline or self.shutdown.is_set():
                    raise
                self.shutdown.wait(0.25)

    def identity(self, chunk=None):
        c = chunk or self.chunk
        if not c:
            raise Cancelled("no_lease")
        return {key: c[key] for key in ("job_id", "chunk_id", "lease_token")}

    def _post(self, action, data, chunk=None):
        return self._request(
            f"/api/agent/{self.name}/{action}", {**self.identity(chunk), **data}
        )

    def register(self):
        r = self._request(
            "/api/agent/register",
            {
                "name": self.name,
                "version": PROTOCOL_VERSION,
                "threads": self.threads,
                "host": os.environ.get("AGENT_HOST", "local"),
            },
        )
        self.heartbeat_seconds = float(r["heartbeat_seconds"])
        self.lease_seconds = float(r["lease_seconds"])
        self.storage_path = str(Path(r["storage_path"]).resolve())
        Path(self.storage_path).mkdir(parents=True, exist_ok=True)
        if not r["enabled"]:
            log.warning("worker_disabled name=%s", self.name)
        return r

    def collect_metrics(self):
        t = time.monotonic()
        interval = max(t - self.last_metrics, 0.001)
        elapsed = max(t - self.started, 0.001)
        memory = psutil.virtual_memory()
        disk = psutil.disk_usage(self.storage_path)
        net = psutil.net_io_counters()
        io = psutil.disk_io_counters()
        proc = psutil.Process()
        with self.lock:
            current = [dict(v, external_id=k) for k, v in self.current.items()]
            result = dict(
                cpu=psutil.cpu_percent(),
                ram_used=memory.used / 1e9,
                ram_total=memory.total / 1e9,
                swap_used=psutil.swap_memory().used / 1e9,
                disk_free=disk.free / 1e9,
                disk_total=disk.total / 1e9,
                net_in=max(0, (net.bytes_recv - self.last_net.bytes_recv) / interval),
                net_out=max(0, (net.bytes_sent - self.last_net.bytes_sent) / interval),
                disk_read=max(0, (io.read_bytes - self.last_disk.read_bytes) / interval)
                if io and self.last_disk
                else 0,
                disk_write=max(
                    0, (io.write_bytes - self.last_disk.write_bytes) / interval
                )
                if io and self.last_disk
                else 0,
                load_avg=psutil.getloadavg()[0],
                open_files=len(proc.open_files()),
                active_connections=self.connections,
                uptime=elapsed,
                ids_per_second=(self.processed - self.last_processed) / interval,
                bytes_per_second=(self.bytes_done - self.last_bytes) / interval,
                active_checks=sum(v.get("phase") == "checking" for v in current),
                active_downloads=sum(v.get("phase") == "downloading" for v in current),
                current=current,
                error_count=self.error_count,
                error_ratio=sum(self.recent) / len(self.recent) if self.recent else 0,
                http_requests=sum(self.status_counts.values()),
                status_counts=dict(self.status_counts),
                retries=self.retries,
                mean_check_seconds=sum(self.check_durations) / len(self.check_durations)
                if self.check_durations
                else 0,
                mean_download_seconds=sum(self.download_durations)
                / len(self.download_durations)
                if self.download_durations
                else 0,
            )
            self.last_processed = self.processed
            self.last_bytes = self.bytes_done
            self.recent.clear()
        self.last_metrics = t
        self.last_net = net
        self.last_disk = io
        return result

    def heartbeat(self):
        with self.lock:
            c = dict(self.chunk) if self.chunk else None
        payload = self.collect_metrics()
        payload["job_id"] = self.job_id
        if c:
            payload.update(self.identity(c))
        r = self._request(
            f"/api/agent/{self.name}/heartbeat", payload, heartbeat=True, retry=False
        )
        self.last_control = time.monotonic()
        with self.lock:
            self.threads = max(1, int(r["desired_threads"]))
            self.paused = r["job_status"] == "paused"
            if c and self.chunk and c["lease_token"] == self.chunk["lease_token"]:
                if not r["enabled"] or not r["lease_renewed"]:
                    self.work_cancel.set()
        return r

    def heartbeat_loop(self):
        while not self.shutdown.is_set():
            try:
                self.heartbeat()
            except Cancelled:
                self.work_cancel.set()
            except Exception:
                log.warning("heartbeat_unavailable name=%s", self.name)
                if time.monotonic() - self.last_control >= self.lease_seconds / 2:
                    self.work_cancel.set()
            self.shutdown.wait(self.heartbeat_seconds)

    def admit(self, kind):
        while True:
            if self.work_cancel.is_set() or self.shutdown.is_set():
                raise Cancelled()
            try:
                r = self._post("permit", {"kind": kind})
            except (httpx.HTTPError, Cancelled):
                self.work_cancel.set()
                raise Cancelled()
            if r["granted"]:
                return
            if self.work_cancel.wait(r["wait_seconds"] + random.uniform(0, 0.01)):
                raise Cancelled()

    def feedback(self, code):
        with self.lock:
            self.status_counts[str(code)] += 1
        try:
            self._post("feedback", {"status_code": code})
        except (httpx.HTTPError, Cancelled):
            self.work_cancel.set()
            raise Cancelled()

    def connection_metric(self, delta):
        with self.lock:
            self.connections += delta.get("connections", 0)
            self.bytes_done += delta.get("bytes", 0)

    def state(self, video_id, status, title=None, attempt=None):
        with self.lock:
            self.current.setdefault(video_id, {})["phase"] = status
            if title:
                self.current[video_id]["title"] = title
        return self._post(
            "state",
            {
                "external_id": video_id,
                "status": status,
                "title": title,
                "attempt": attempt,
            },
        )

    def file_progress(self, video_id, done, total):
        with self.lock:
            v = self.current.setdefault(video_id, {})
            v.update(
                bytes=done,
                total=total,
                progress=round(done / total * 100, 1) if total else None,
            )

    def process_id(self, video_id, http, chunk):
        result = {"external_id": video_id, "status": "failed", "http_status": None}
        attempts = []
        metadata = None
        try:
            for attempt in range(chunk["max_retries"] + 1):
                started = now()
                downloading = False
                try:
                    self.state(video_id, "checking")
                    t = time.monotonic()
                    metadata = discover(
                        http,
                        chunk["source"],
                        "https://kinescope.io"
                        if chunk["source"] == "kinescope"
                        else self.test_target_url,
                        video_id,
                    )
                    with self.lock:
                        self.check_durations.append(time.monotonic() - t)
                    self.state(video_id, "found", metadata["title"])
                    self.state(video_id, "download_queued", metadata["title"])
                    downloading = True
                    started = now()
                    self.state(video_id, "downloading", metadata["title"], attempt + 1)
                    t = time.monotonic()
                    f = download_media(
                        http,
                        metadata,
                        self.storage_path,
                        video_id,
                        chunk["min_free_space_gb"],
                        self.max_height,
                        lambda n, total: self.file_progress(video_id, n, total),
                    )
                    with self.lock:
                        self.download_durations.append(time.monotonic() - t)
                    attempts.append(
                        dict(
                            attempt=attempt + 1,
                            status="completed",
                            started_at=started,
                            finished_at=now(),
                        )
                    )
                    result.update(metadata)
                    result.update(
                        status="completed", http_status=200, error_code=None, file=f
                    )
                    break
                except SourceError as e:
                    result.update(http_status=e.status or None, error_code=e.code)
                    if downloading:
                        attempts.append(
                            dict(
                                attempt=attempt + 1,
                                status="retry_wait"
                                if e.retryable and attempt < chunk["max_retries"]
                                else "failed",
                                started_at=started,
                                finished_at=now(),
                                error_code=e.code,
                            )
                        )
                    if e.status in (401, 403):
                        result["status"] = "forbidden"
                        break
                    if e.status == 404:
                        result["status"] = "not_found" if not metadata else "failed"
                        break
                    if not e.retryable or attempt == chunk["max_retries"]:
                        result["status"] = "failed"
                        break
                    self.state(video_id, "retry_wait")
                    with self.lock:
                        self.retries += 1
                    http.sleep(
                        e.retry_after
                        if e.retry_after is not None
                        else backoff_delay(attempt)
                    )
            if metadata:
                for k in (
                    "title",
                    "source_url",
                    "download_url",
                    "mime",
                    "expected_size",
                    "expected_checksum",
                ):
                    result[k] = metadata.get(k)
            result["attempts"] = attempts
            with self.lock:
                self.processed += 1
                bad = result["status"] == "failed"
                self.error_count += int(bad)
                self.recent.append(int(bad))
            return result
        except (httpx.HTTPError, Cancelled):
            self.work_cancel.set()
            raise Cancelled()
        except (OSError, ValueError, TypeError, KeyError) as e:
            # A persistent local/format error must not requeue the same block forever.
            result.update(
                status="failed", error_code=type(e).__name__, attempts=attempts
            )
            with self.lock:
                self.processed += 1
                self.error_count += 1
                self.recent.append(1)
            return result
        finally:
            with self.lock:
                self.current.pop(video_id, None)

    def process_chunk(self, chunk):
        with self.lock:
            self.chunk = chunk
            self.work_cancel = threading.Event()
            self.paused = False
            self.threads = chunk["threads"]
        self.storage_path = str(Path(chunk["storage_path"]).resolve())
        Path(self.storage_path).mkdir(parents=True, exist_ok=True)
        http = TargetHTTP(
            timeout=chunk["request_timeout"],
            max_connections=chunk["max_connections"],
            test_source=chunk["source"] == "mock",
            admit=self.admit,
            feedback=self.feedback,
            cancelled=self.work_cancel,
            metrics=self.connection_metric,
        )
        skip = set(chunk["skip_ids"])
        ids = iter(
            i for i in range(chunk["next_id"], chunk["range_end"] + 1) if i not in skip
        )
        exhausted = False
        futures = {}
        try:
            with ThreadPoolExecutor(max_workers=256) as pool:
                while futures or not exhausted:
                    if self.shutdown.is_set():
                        self.work_cancel.set()
                    while (
                        not exhausted
                        and not self.paused
                        and not self.work_cancel.is_set()
                        and len(futures) < self.threads
                    ):
                        if (
                            shutil.disk_usage(self.storage_path).free
                            < chunk["min_free_space_gb"] * 1e9
                        ):
                            self.paused = True
                            break
                        ident = next(ids, None)
                        if ident is None:
                            exhausted = True
                            break
                        futures[pool.submit(self.process_id, ident, http, chunk)] = (
                            ident
                        )
                    if not futures:
                        break
                    done, _ = wait(futures, timeout=0.1, return_when=FIRST_COMPLETED)
                    for f in done:
                        ident = futures.pop(f)
                        try:
                            result = f.result()
                            if not self.work_cancel.is_set():
                                self._post("progress", {"items": [result]}, chunk)
                        except (Cancelled, httpx.HTTPError):
                            self.work_cancel.set()
                        except Exception as e:
                            log.error(
                                "operation_failed id=%s type=%s",
                                ident,
                                type(e).__name__,
                            )
                            self.work_cancel.set()
            if not self.work_cancel.is_set():
                self._post("complete" if exhausted else "fail", {}, chunk)
        finally:
            http.close()
            with self.lock:
                self.chunk = None
                self.current.clear()

    def run(self):
        self.register()
        self.heartbeat()
        hb = threading.Thread(target=self.heartbeat_loop, name="heartbeat", daemon=True)
        hb.start()
        try:
            while not self.shutdown.is_set():
                try:
                    lease = self._request(
                        f"/api/agent/{self.name}/lease", {"job_id": self.job_id}
                    )
                    if lease["chunk"]:
                        self.process_chunk(lease["chunk"])
                    elif self.once and lease["job_status"] in {
                        "completed",
                        "failed",
                        "stopped",
                        "missing",
                    }:
                        return
                    else:
                        self.shutdown.wait(1)
                except (httpx.HTTPError, Cancelled) as e:
                    log.warning("control_interrupted type=%s", type(e).__name__)
                    self.shutdown.wait(1)
        finally:
            self.shutdown.set()
            self.work_cancel.set()
            hb.join(timeout=6)
            self.client.close()
            self.heartbeat_client.close()


def main():
    parser = argparse.ArgumentParser(description="Video Harvester agent 2")
    parser.add_argument("name", nargs="?")
    parser.add_argument("job_id", type=int, nargs="?")
    parser.add_argument("--env-file", default=".env.agent")
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--threads", type=int)
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()
    load_dotenv(args.env_file, override=False)
    if args.check:
        if not shutil.which("ffmpeg") or not shutil.which("ffprobe"):
            parser.error("Установите FFmpeg и ffprobe")
        print("Python dependencies and FFmpeg: OK")
        return
    if not args.name or args.job_id is None:
        parser.error("Нужны имя и job_id")
    config = load_agent_config()
    token = os.environ.get("AGENT_TOKEN", "")
    if len(token) < 24:
        parser.error("AGENT_TOKEN: минимум 24 символа")
    url = os.environ.get(
        "COORDINATOR_URL", config.get("coordinator_url", "https://localhost:8000")
    )
    if (
        not url.startswith("https://")
        and not os.environ.get("ALLOW_INSECURE_LOCAL_TEST") == "1"
    ):
        parser.error("Координатор должен использовать HTTPS")
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    storage = os.environ.get(
        "STORAGE_PATH", config.get("storage_path", f"./data/{args.name}")
    )
    agent = Agent(
        args.name,
        args.job_id,
        url,
        token,
        storage,
        args.threads or int(config.get("threads", 4)),
        ca_cert=os.environ.get("CA_CERT", config.get("ca_cert")),
        max_height=int(config.get("max_height", 720)),
        test_target_url=os.environ.get("TEST_TARGET_URL", "http://localhost:9000"),
        once=args.once,
    )
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: (agent.shutdown.set(), agent.work_cancel.set()))
    # A second local process must not impersonate an already-running worker.
    lockpath = Path(storage).resolve()
    lockpath.mkdir(parents=True, exist_ok=True)
    with (lockpath / f".{args.name}.lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            parser.error("Агент с этим именем уже запущен в данном каталоге")
        agent.run()


if __name__ == "__main__":
    main()

# Public utility retained for callers of protocol 1.
from agent.http import retry_after_seconds as retry_after_seconds
