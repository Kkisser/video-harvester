"""Bounded live integration check: exactly the supplied public example, no ID scan."""

import json, os, subprocess, sys, tempfile, time, socket
from pathlib import Path
import httpx, yaml
from dotenv import dotenv_values

ROOT = Path(__file__).resolve().parents[1]


def main():
    os.chdir(ROOT)
    with tempfile.TemporaryDirectory(prefix="harvester-live-") as td:
        td = Path(td)
        report_path = ROOT / "docs/live-check.json"
        config = yaml.safe_load(Path("config.example.yaml").read_text())
        config["servers"] = config["servers"][:1]
        config["servers"][0].update(
            storage_path=str(ROOT / "data/live-check"), min_free_space_gb=0.1
        )
        config["agent"]["max_height"] = 360
        config["coordinator"]["database_url"] = "sqlite:///" + str(td / "state.db")
        (td / "config.yaml").write_text(yaml.safe_dump(config))
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        url = f"https://localhost:{port}"
        env = {
            **os.environ,
            **dotenv_values(".env.coordinator"),
            "CONFIG_PATH": str(td / "config.yaml"),
        }
        env.pop("DATABASE_URL", None)
        logpath = ROOT / "data/live-check-process.log"
        logpath.parent.mkdir(exist_ok=True)
        processes = []
        with logpath.open("w") as log:
            try:
                coordinator = subprocess.Popen(
                    [
                        sys.executable,
                        "-m",
                        "uvicorn",
                        "coordinator.app:app",
                        "--host",
                        "127.0.0.1",
                        "--port",
                        str(port),
                        "--ssl-keyfile",
                        "certs/key.pem",
                        "--ssl-certfile",
                        "certs/cert.pem",
                        "--no-access-log",
                    ],
                    env=env,
                    stdout=log,
                    stderr=log,
                )
                processes.append(coordinator)
                with httpx.Client(
                    verify="certs/cert.pem", timeout=15, trust_env=False
                ) as c:
                    for _ in range(100):
                        try:
                            if c.get(url + "/api/health").is_success:
                                break
                        except httpx.HTTPError:
                            pass
                        if coordinator.poll() is not None:
                            raise RuntimeError(
                                "coordinator exited; inspect data/live-check-process.log"
                            )
                        time.sleep(0.1)
                    auth = ("admin", env["BOOTSTRAP_ADMIN_PASSWORD"])
                    r = c.post(
                        url + "/api/jobs",
                        auth=auth,
                        json={
                            "name": "Kinescope real integration",
                            "range_text": "[200810246,200810246]",
                            "chunk_size": 1,
                            "threads": 1,
                            "max_connections": 2,
                            "global_rate_limit_rps": 5,
                            "check_rate_rps": 5,
                            "download_rate_rps": 5,
                            "request_timeout": 20,
                        },
                    )
                    r.raise_for_status()
                    job_id = r.json()["job_id"]
                    c.post(
                        f"{url}/api/jobs/{job_id}/start", auth=auth
                    ).raise_for_status()
                    agentenv = {**env, "COORDINATOR_URL": url}
                    p = subprocess.Popen(
                        [
                            sys.executable,
                            "-m",
                            "agent.agent",
                            "worker-01",
                            str(job_id),
                            "--env-file",
                            ".env.worker-01",
                            "--once",
                        ],
                        env=agentenv,
                        stdout=log,
                        stderr=log,
                    )
                    processes.append(p)
                    start = time.monotonic()
                    while time.monotonic() - start < 1800:
                        state = c.get(f"{url}/api/jobs/{job_id}", auth=auth).json()
                        if state["status"] in {"completed", "failed"}:
                            break
                        if p.poll() is not None:
                            raise RuntimeError(
                                "agent exited early; inspect data/live-check-process.log"
                            )
                        time.sleep(1)
                    else:
                        raise RuntimeError("live test timeout")
                    items = c.get(f"{url}/api/jobs/{job_id}/items", auth=auth).json()
                    report = {
                        "source": "https://kinescope.io/200810246",
                        "elapsed_seconds": round(time.monotonic() - start, 2),
                        "job": state,
                        "items": items,
                        "transport": "HTTPS with verified local CA; separate coordinator and agent processes",
                    }
                    if items and items[0]["status"] == "completed":
                        path = ROOT / "data/live-check/200/200810246/video.mp4"
                        report["ffprobe"] = json.loads(
                            subprocess.check_output(
                                [
                                    "ffprobe",
                                    "-v",
                                    "error",
                                    "-show_entries",
                                    "format=duration,size:stream=codec_type,codec_name,width,height",
                                    "-of",
                                    "json",
                                    str(path),
                                ]
                            )
                        )
                    report_path.write_text(
                        json.dumps(report, ensure_ascii=False, indent=2)
                    )
                    print(json.dumps(report, ensure_ascii=False, indent=2))
                    if not items or items[0]["status"] != "completed":
                        raise SystemExit(1)
            finally:
                for p in reversed(processes):
                    if p.poll() is None:
                        p.terminate()
                        try:
                            p.wait(timeout=8)
                        except subprocess.TimeoutExpired:
                            p.kill()
                            p.wait()


if __name__ == "__main__":
    main()
