"""Real processes and HTTPS: load, coordinator restart and worker crash checks."""

import argparse, json, os, socket, subprocess, sys, tempfile, time
from pathlib import Path
import httpx, yaml

ROOT = Path(__file__).resolve().parents[1]


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def run(mode="failure", workers=3):
    os.chdir(ROOT)
    with tempfile.TemporaryDirectory(prefix="harvester-system-") as temp:
        td = Path(temp)
        processes = []
        logs = []
        port = free_port()
        target_port = free_port()
        url = f"https://localhost:{port}"
        tokens = {
            f"w{i}": f"local-test-token-{i}-123456789012345" for i in range(workers)
        }
        threads = 12 if mode == "load" else 2
        config = {
            "coordinator": {
                "database_url": "sqlite:///" + str(td / "state.db"),
                "lease_seconds": 3,
                "heartbeat_seconds": 0.4,
                "allow_test_source": True,
            },
            "agent": {"ca_cert": str(ROOT / "certs/cert.pem")},
            "servers": [
                {
                    "name": n,
                    "host": "local",
                    "storage_path": str(td / n),
                    "threads": threads,
                    "rps": 10000,
                    "min_free_space_gb": 0,
                }
                for n in tokens
            ],
        }
        (td / "config.yaml").write_text(yaml.safe_dump(config))
        env = {
            **os.environ,
            "CONFIG_PATH": str(td / "config.yaml"),
            "COORDINATOR_AGENT_TOKENS": json.dumps(tokens),
            "SECRET_KEY": "system-check-secret-key-123456789",
            "BOOTSTRAP_ADMIN_PASSWORD": "test-password",
            "COORDINATOR_URL": url,
            "CA_CERT": str(ROOT / "certs/cert.pem"),
            "TEST_TARGET_URL": f"http://127.0.0.1:{target_port}",
            "TEST_RATE_LIMIT": "10000",
            "TEST_RESPONSE_DELAY": "3" if mode == "load" else "1",
        }
        env.pop("DATABASE_URL", None)
        env.pop("COORDINATOR_AGENT_TOKENS_FILE", None)
        if mode == "load":
            env["TEST_FORCE_404"] = "1"

        def start(args, extra=None):
            f = (td / f"log-{len(logs)}.txt").open("w")
            logs.append(f)
            p = subprocess.Popen(
                [sys.executable, *args],
                env={**env, **(extra or {})},
                stdout=f,
                stderr=f,
            )
            processes.append(p)
            return p

        coord_args = [
            "-m",
            "uvicorn",
            "coordinator.app:app",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
            "--ssl-keyfile",
            str(ROOT / "certs/key.pem"),
            "--ssl-certfile",
            str(ROOT / "certs/cert.pem"),
            "--no-access-log",
        ]
        auth = ("admin", "test-password")
        try:
            coordinator = start(coord_args)
            start(
                [
                    "-m",
                    "uvicorn",
                    "mock_target.server:app",
                    "--host",
                    "127.0.0.1",
                    "--port",
                    str(target_port),
                    "--no-access-log",
                ]
            )
            with httpx.Client(
                verify=str(ROOT / "certs/cert.pem"), timeout=30, trust_env=False
            ) as c:

                def ready():
                    for _ in range(100):
                        try:
                            if c.get(url + "/api/health").status_code == 200:
                                return
                        except httpx.HTTPError:
                            pass
                        time.sleep(0.1)
                    raise RuntimeError("coordinator not ready")

                ready()
                end = workers * threads * 2 - 1 if mode == "load" else 41
                r = c.post(
                    url + "/api/jobs",
                    auth=auth,
                    json={
                        "name": mode,
                        "range_text": f"[0,{end}]",
                        "source": "mock",
                        "threads": threads,
                        "chunk_size": threads * 2 if mode == "load" else 7,
                        "global_rate_limit_rps": 10000,
                        "check_rate_rps": 10000,
                        "download_rate_rps": 10000,
                        "max_connections": threads,
                        "request_timeout": 10,
                        "max_retries": 1,
                    },
                )
                r.raise_for_status()
                j = r.json()["job_id"]
                c.post(f"{url}/api/jobs/{j}/start", auth=auth).raise_for_status()
                agents = [
                    start(
                        ["-m", "agent.agent", n, str(j), "--once"],
                        {"AGENT_TOKEN": token, "STORAGE_PATH": str(td / n)},
                    )
                    for n, token in tokens.items()
                ]
                begin = time.monotonic()
                samples = []
                killed = False
                restarted = False
                before = []
                while time.monotonic() - begin < 180:
                    state = c.get(f"{url}/api/jobs/{j}", auth=auth).json()
                    samples.append(state["processed"])
                    if (
                        mode == "failure"
                        and time.monotonic() - begin > 1.5
                        and not killed
                    ):
                        agents[0].kill()
                        agents[0].wait()
                        killed = True
                    if mode == "failure" and state["processed"] >= 5 and not restarted:
                        before = c.get(f"{url}/api/jobs/{j}/items", auth=auth).json()
                        coordinator.kill()
                        coordinator.wait()
                        time.sleep(0.3)
                        coordinator = start(coord_args)
                        ready()
                        restarted = True
                    if state["status"] in {"completed", "failed"}:
                        break
                    time.sleep(0.2)
                else:
                    raise RuntimeError("system test timeout")
                items = c.get(
                    f"{url}/api/jobs/{j}/items", auth=auth, params={"limit": 1000}
                ).json()
                events = c.get(
                    f"{url}/api/jobs/{j}/events", auth=auth, params={"limit": 1000}
                ).json()
                peak = c.get(f"http://127.0.0.1:{target_port}/test/metrics").json()[
                    "peak"
                ]
                report = {
                    "mode": mode,
                    "workers": workers,
                    "threads_per_worker": threads,
                    "ids": end + 1,
                    "elapsed_seconds": round(time.monotonic() - begin, 3),
                    "peak_target_requests": peak,
                    "state": state,
                    "unique_ids": len({x["external_id"] for x in items}),
                    "worker_killed": killed,
                    "coordinator_restarted": restarted,
                    "lease_reclaims": sum(
                        e["operation"] == "lease_expired" for e in events
                    ),
                    "confirmed_before_restart": sum(
                        x["status"] in {"completed", "not_found", "forbidden", "failed"}
                        for x in before
                    ),
                }
                assert state["status"] == "completed" and len(items) == end + 1, report
                final = {x["external_id"]: x for x in items}
                assert all(
                    final[x["external_id"]]["status"] == x["status"]
                    for x in before
                    if x["status"] in {"completed", "not_found", "forbidden"}
                ), report
                if mode == "load":
                    assert peak >= 100, report
                if mode == "failure":
                    assert (
                        killed and restarted and report["lease_reclaims"] >= 1
                    ), report
                (ROOT / f"docs/{mode}-check.json").write_text(
                    json.dumps(report, ensure_ascii=False, indent=2)
                )
                print(json.dumps(report, ensure_ascii=False, indent=2))
        except BaseException:
            for f in logs:
                f.flush()
                print(Path(f.name).read_text()[-2500:], file=sys.stderr)
            raise
        finally:
            for p in reversed(processes):
                if p.poll() is None:
                    p.terminate()
                    try:
                        p.wait(timeout=6)
                    except subprocess.TimeoutExpired:
                        p.kill()
                        p.wait()
            for f in logs:
                f.close()


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--mode", choices=["load", "failure"], default="failure")
    p.add_argument("--workers", type=int, default=3)
    args = p.parse_args()
    run(args.mode, args.workers)
