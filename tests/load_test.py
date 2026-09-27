"""Небольшой управляемый нагрузочный прогон.

Перед запуском поднять mock_target и три агента. Скрипт отдельно создаёт
100 одновременных HTTP-операций и показывает реакцию ограничения частоты.
"""
import argparse
import json
import statistics
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor

import httpx


def one_request(base_url: str, video_id: int):
    started = time.perf_counter()
    try:
        response = httpx.get(f"{base_url.rstrip('/')}/{video_id}", timeout=10)
        status = response.status_code
    except httpx.HTTPError:
        status = 0
    return status, time.perf_counter() - started


def percentile(values, fraction):
    ordered = sorted(values)
    index = min(int(len(ordered) * fraction), len(ordered) - 1)
    return ordered[index]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://localhost:9000")
    parser.add_argument("--requests", type=int, default=1000)
    parser.add_argument("--parallel", type=int, default=100)
    args = parser.parse_args()
    started = time.perf_counter()
    with ThreadPoolExecutor(max_workers=args.parallel) as pool:
        results = list(pool.map(
            lambda video_id: one_request(args.url, video_id),
            range(args.requests),
        ))
    elapsed = time.perf_counter() - started
    statuses = Counter(status for status, _latency in results)
    latencies = [latency for _status, latency in results]
    report = {
        "requests": args.requests,
        "parallel_operations": args.parallel,
        "elapsed_seconds": round(elapsed, 3),
        "requests_per_second": round(args.requests / elapsed, 2),
        "latency_mean_ms": round(statistics.mean(latencies) * 1000, 2),
        "latency_p95_ms": round(percentile(latencies, 0.95) * 1000, 2),
        "statuses": dict(sorted(statuses.items())),
    }
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
