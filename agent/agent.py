"""Рабочий агент распределённой системы."""
import argparse
import os
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import httpx
import psutil
import yaml

from agent.downloader import download


class RateLimiter:
    """Общий для всех потоков агента интервал между запросами."""
    def __init__(self, rps: float):
        self.lock = threading.RLock()
        self.last = 0.0
        self.rps = 1.0
        self.min_interval = 1.0
        self.set_rps(rps)

    def set_rps(self, rps: float):
        with self.lock:
            self.rps = max(float(rps), 0.1)
            self.min_interval = 1.0 / self.rps

    def wait(self):
        with self.lock:
            now = time.monotonic()
            delay = self.min_interval - (now - self.last)
            if delay > 0:
                time.sleep(delay)
            self.last = time.monotonic()


class AdaptiveController:
    """Снижает RPS при большой доле 429 и 5xx, затем медленно восстанавливает."""
    def __init__(self, limiter: RateLimiter, base_rps: float):
        self.limiter = limiter
        self.base_rps = base_rps
        self.good = 0
        self.bad = 0
        self.lock = threading.Lock()

    def set_base(self, value: float):
        with self.lock:
            self.base_rps = max(value, 0.1)
            self.limiter.set_rps(min(self.limiter.rps, self.base_rps))

    def report(self, status_code):
        with self.lock:
            if status_code == 429 or (status_code is not None and status_code >= 500):
                self.bad += 1
            else:
                self.good += 1
            total = self.good + self.bad
            if total < 20:
                return
            bad_ratio = self.bad / total
            if bad_ratio > 0.20:
                self.limiter.set_rps(max(self.limiter.rps / 2, 0.5))
            elif bad_ratio < 0.05:
                self.limiter.set_rps(min(self.limiter.rps * 1.25, self.base_rps))
            self.good = self.bad = 0


def backoff_delay(attempt: int, base: float = 0.5, cap: float = 30.0):
    return min(cap, base * (2 ** attempt)) + random.uniform(0, base)


def retry_after_seconds(headers):
    value = headers.get("retry-after") if headers else None
    try:
        return float(value) if value is not None else None
    except ValueError:
        return None


def check_one(client: httpx.Client, base_url: str, video_id: int, timeout: int):
    url = f"{base_url.rstrip('/')}/{video_id}"
    try:
        response = client.get(url, timeout=timeout)
    except httpx.TimeoutException:
        return {"external_id": video_id, "status": "retry_wait",
                "http_status": None, "error_code": "timeout"}
    except httpx.HTTPError as error:
        return {"external_id": video_id, "status": "retry_wait",
                "http_status": None, "error_code": type(error).__name__}

    code = response.status_code
    if code == 200:
        try:
            data = response.json()
        except ValueError:
            return {"external_id": video_id, "status": "failed",
                    "http_status": 200, "error_code": "bad_json"}
        return {
            "external_id": video_id,
            "status": "found",
            "http_status": 200,
            "title": data.get("title"),
            "download_url": data.get("download_url"),
            "mime": data.get("mime"),
            "expected_size": data.get("size"),
            "expected_checksum": data.get("checksum"),
            "source_url": url,
        }
    if code == 404:
        return {"external_id": video_id, "status": "not_found", "http_status": code}
    if code in (401, 403):
        return {"external_id": video_id, "status": "forbidden", "http_status": code}
    if code == 429:
        return {"external_id": video_id, "status": "rate_limited", "http_status": code,
                "retry_after": retry_after_seconds(response.headers)}
    if code >= 500:
        return {"external_id": video_id, "status": "retry_wait", "http_status": code,
                "error_code": f"http_{code}"}
    return {"external_id": video_id, "status": "failed", "http_status": code,
            "error_code": f"http_{code}"}


def load_agent_config():
    path = Path(os.environ.get("CONFIG_PATH", "config.yaml"))
    if not path.exists():
        path = Path("config.example.yaml")
    if path.exists():
        with path.open(encoding="utf-8") as stream:
            return (yaml.safe_load(stream) or {}).get("agent", {})
    return {}


class Agent:
    def __init__(self, name: str, job_id: int, coordinator_url: str, target_url: str,
                 token: str, storage_path: str, threads: int, rps: float,
                 timeout: int, max_retries: int, heartbeat_seconds: int,
                 progress_batch: int, max_downloads=None, ca_cert=None):
        self.name = name
        self.job_id = job_id
        self.coordinator_url = coordinator_url.rstrip("/")
        self.target_url = target_url.rstrip("/")
        self.storage_path = storage_path
        self.threads = threads
        self.timeout = timeout
        self.max_retries = max_retries
        self.heartbeat_seconds = heartbeat_seconds
        self.progress_batch = max(progress_batch, 1)
        self.max_downloads = max_downloads
        self.downloads_done = 0
        self.check_limiter = RateLimiter(rps)
        self.download_limiter = RateLimiter(max(rps / 4, 1))
        self.adaptive = AdaptiveController(self.check_limiter, rps)
        verify = ca_cert if ca_cert and Path(ca_cert).exists() else True
        self.coordinator_client = httpx.Client(
            headers={"X-Agent-Token": token}, verify=verify, timeout=30,
        )
        # Отдельный клиент принципиален: секрет координатора не уходит мишени.
        self.target_client = httpx.Client(timeout=60)
        self.lock = threading.Lock()
        self.started_at = time.monotonic()
        self.processed = 0
        self.bytes_downloaded = 0
        self.error_count = 0
        self.active_checks = 0
        self.active_downloads = 0
        self.last_net = psutil.net_io_counters()
        self.last_metric_time = time.monotonic()
        Path(self.storage_path).mkdir(parents=True, exist_ok=True)

    def _post(self, path: str, payload: dict):
        response = self.coordinator_client.post(
            f"{self.coordinator_url}{path}", json=payload,
        )
        response.raise_for_status()
        return response.json()

    def register(self):
        return self._post("/api/agent/register", {
            "name": self.name, "host": os.environ.get("AGENT_HOST", "local"),
            "threads": self.threads, "version": "1.0",
        })

    def collect_metrics(self):
        memory = psutil.virtual_memory()
        disk = psutil.disk_usage(self.storage_path)
        now = time.monotonic()
        net = psutil.net_io_counters()
        interval = max(now - self.last_metric_time, 0.001)
        net_in = (net.bytes_recv - self.last_net.bytes_recv) / interval
        net_out = (net.bytes_sent - self.last_net.bytes_sent) / interval
        self.last_net = net
        self.last_metric_time = now
        elapsed = max(now - self.started_at, 0.001)
        try:
            load_avg = psutil.getloadavg()[0]
        except (AttributeError, OSError):
            load_avg = 0
        try:
            open_files = len(psutil.Process().open_files())
        except (psutil.Error, OSError):
            open_files = 0
        with self.lock:
            return {
                "cpu": psutil.cpu_percent(interval=0.05),
                "ram_used": round(memory.used / 1e9, 3),
                "ram_total": round(memory.total / 1e9, 3),
                "disk_free": round(disk.free / 1e9, 3),
                "net_in": round(max(net_in, 0), 1),
                "net_out": round(max(net_out, 0), 1),
                "load_avg": round(load_avg, 2),
                "open_files": open_files,
                "active_connections": 0,
                "uptime": round(elapsed, 1),
                "ids_per_second": round(self.processed / elapsed, 2),
                "bytes_per_second": round(self.bytes_downloaded / elapsed, 2),
                "active_checks": self.active_checks,
                "active_downloads": self.active_downloads,
                "error_count": self.error_count,
            }

    def heartbeat(self):
        response = self._post(
            f"/api/agent/{self.name}/heartbeat", self.collect_metrics(),
        )
        self.threads = max(int(response.get("desired_threads", self.threads)), 1)
        return response

    def get_chunk(self):
        return self._post(
            f"/api/agent/{self.name}/lease", {"job_id": self.job_id},
        )

    def process_id(self, video_id: int):
        result = None
        for attempt in range(self.max_retries + 1):
            self.check_limiter.wait()
            with self.lock:
                self.active_checks += 1
            try:
                result = check_one(self.target_client, self.target_url, video_id, self.timeout)
            finally:
                with self.lock:
                    self.active_checks -= 1
            self.adaptive.report(result.get("http_status"))
            if result["status"] == "rate_limited":
                time.sleep(result.get("retry_after") or backoff_delay(attempt))
                continue
            if result["status"] == "retry_wait" and attempt < self.max_retries:
                time.sleep(backoff_delay(attempt))
                continue
            break

        if result["status"] == "rate_limited":
            result["status"] = "retry_wait"
            result["error_code"] = "retry_limit_exhausted"

        if result["status"] == "found" and result.get("download_url"):
            with self.lock:
                if self.max_downloads is not None and self.downloads_done >= self.max_downloads:
                    result["status"] = "skipped"
                    return result
                self.downloads_done += 1
                self.active_downloads += 1
            try:
                self.download_limiter.wait()
                file_result = download(
                    self.target_client, self.target_url, result["download_url"],
                    self.storage_path, video_id, result,
                )
            finally:
                with self.lock:
                    self.active_downloads -= 1
            if file_result["status"] == "completed":
                result["status"] = "completed"
                result["file"] = {
                    "path": file_result["path"], "size": file_result.get("size", 0),
                    "mime": file_result.get("mime", ""),
                    "checksum": file_result.get("checksum", ""),
                }
                with self.lock:
                    self.bytes_downloaded += file_result.get("size", 0)
            else:
                result["status"] = file_result["status"]
                result["error_code"] = file_result.get("error_code")

        with self.lock:
            self.processed += 1
            if result["status"] in {"failed", "retry_wait"}:
                self.error_count += 1
        return result

    def run(self):
        self.register()
        idle = 0
        while True:
            chunk = None
            try:
                self.heartbeat()
                lease = self.get_chunk()
                chunk = lease.get("chunk")
                if chunk is None:
                    if lease.get("job_status") in {"completed", "stopped", "failed", "missing"}:
                        return
                    idle += 1
                    time.sleep(min(idle + 1, 10))
                    continue
                idle = 0
                self.threads = max(int(chunk.get("threads", self.threads)), 1)
                allocated_rps = float(chunk.get("rps", self.check_limiter.rps))
                self.adaptive.set_base(allocated_rps)
                self.check_limiter.set_rps(allocated_rps)
                identifiers = list(range(chunk["next_id"], chunk["range_end"] + 1))
                aborted = False

                for offset in range(0, len(identifiers), self.progress_batch):
                    batch = identifiers[offset:offset + self.progress_batch]
                    # Пустой progress сначала продлевает аренду перед медленной пачкой.
                    self._post(f"/api/agent/{self.name}/progress", {
                        "job_id": self.job_id, "chunk_id": chunk["chunk_id"],
                        "next_id": batch[0], "items": [],
                    })
                    with ThreadPoolExecutor(max_workers=self.threads) as pool:
                        results = list(pool.map(self.process_id, batch))
                    progress = self._post(f"/api/agent/{self.name}/progress", {
                        "job_id": self.job_id, "chunk_id": chunk["chunk_id"],
                        "next_id": batch[-1] + 1, "items": results,
                    })
                    if progress.get("job_status") in {"stopped", "failed", "missing"}:
                        self._post(
                            f"/api/agent/{self.name}/fail", {"chunk_id": chunk["chunk_id"]},
                        )
                        aborted = True
                        break
                    self.heartbeat()
                if not aborted:
                    self._post(
                        f"/api/agent/{self.name}/complete", {"chunk_id": chunk["chunk_id"]},
                    )
            except KeyboardInterrupt:
                return
            except httpx.HTTPError:
                time.sleep(2)
            except Exception:
                with self.lock:
                    self.error_count += 1
                try:
                    if chunk:
                        self._post(
                            f"/api/agent/{self.name}/fail", {"chunk_id": chunk["chunk_id"]},
                        )
                except httpx.HTTPError:
                    pass
                time.sleep(2)


def build_agent():
    config = load_agent_config()
    parser = argparse.ArgumentParser(description="Рабочий агент")
    parser.add_argument("name")
    parser.add_argument("job_id", type=int)
    parser.add_argument("--threads", type=int,
                        default=int(os.environ.get("AGENT_THREADS", config.get("threads", 4))))
    parser.add_argument("--rps", type=float,
                        default=float(os.environ.get("AGENT_RPS", config.get("rps", 20))))
    parser.add_argument("--max-downloads", type=int, default=None)
    args = parser.parse_args()
    token = os.environ.get("AGENT_TOKEN", "")
    if len(token) < 16:
        raise RuntimeError("AGENT_TOKEN должен содержать не менее 16 символов")
    max_downloads = args.max_downloads
    if max_downloads is None:
        raw_limit = os.environ.get("MAX_DOWNLOADS", config.get("max_downloads"))
        max_downloads = int(raw_limit) if raw_limit not in (None, "", "null") else None
    return Agent(
        name=args.name,
        job_id=args.job_id,
        coordinator_url=os.environ.get("COORDINATOR_URL", config.get("coordinator_url", "https://localhost:8000")),
        target_url=os.environ.get("TARGET_URL", config.get("target_url", "http://localhost:9000")),
        token=token,
        storage_path=os.environ.get("STORAGE_PATH", config.get("storage_path", "./data")),
        threads=args.threads,
        rps=args.rps,
        timeout=int(config.get("request_timeout", 10)),
        max_retries=int(config.get("max_retries", 4)),
        heartbeat_seconds=int(config.get("heartbeat_seconds", 5)),
        progress_batch=int(config.get("progress_batch", 20)),
        max_downloads=max_downloads,
        ca_cert=config.get("ca_cert"),
    )


if __name__ == "__main__":
    build_agent().run()
