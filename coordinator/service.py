"""Прикладная логика координатора без HTTP и HTML.

Координатор выдаёт небольшие блоки в аренду. Если агент не продлил аренду,
блок снова становится pending. Это даёт модель обработки at least once.
Повторная запись результата безопасна благодаря ограничениям базы.
"""
import os
import shlex
import subprocess
import sys
import threading
from datetime import datetime, timedelta

from coordinator.db import CONFIG, Item, Job, JobWorker, RangeChunk, Session, Worker


LEASE_SECONDS = int(CONFIG["coordinator"].get("lease_seconds", 20))
CPU_LIMIT = 95.0
_lease_lock = threading.Lock()

STATES = {
    "created", "validating", "running", "pausing", "paused",
    "stopping", "stopped", "completed", "failed",
}

TRANSITIONS = {
    "created": {"validating", "running", "failed"},
    "validating": {"created", "running", "failed"},
    "running": {"pausing", "paused", "stopping", "stopped", "completed", "failed"},
    "pausing": {"paused", "running", "stopping"},
    "paused": {"running", "stopping", "stopped"},
    "stopping": {"stopped"},
    "stopped": {"running"},
    "completed": set(),
    "failed": {"running"},
}


def parse_range(text: str):
    """Возвращает (начало, конец); None вместо конца означает бесконечность."""
    text = (text or "").strip()
    if text == "":
        return 0, None
    if text.startswith("[") and text.endswith("]"):
        inner = text[1:-1]
        if "," not in inner:
            raise ValueError("ожидается формат [A,B] или [A,]")
        left, right = inner.split(",", 1)
        start = int(left.strip()) if left.strip() else 0
        end = int(right.strip()) if right.strip() else None
    else:
        start, end = 0, int(text)
    if start < 0 or (end is not None and end < 0):
        raise ValueError("границы должны быть неотрицательными")
    if end is not None and start > end:
        raise ValueError("нижняя граница больше верхней")
    return start, end


def parse_cli_range(from_arg, to_arg):
    start = int(from_arg) if from_arg is not None else 0
    end = int(to_arg) if to_arg is not None else None
    if start < 0 or (end is not None and end < 0):
        raise ValueError("границы должны быть неотрицательными")
    if end is not None and start > end:
        raise ValueError("--from больше --to")
    return start, end


def make_chunks(start: int, end, size: int):
    """Режет включительный диапазон на блоки без дыр и пересечений."""
    if size < 1:
        raise ValueError("размер блока должен быть не меньше 1")
    result = []
    current = start
    while end is None or current <= end:
        block_end = current + size - 1
        if end is not None:
            block_end = min(block_end, end)
        result.append((current, block_end))
        if end is None:
            break
        current = block_end + 1
    return result


def can_transition(old: str, new: str) -> bool:
    return new in TRANSITIONS.get(old, set())


def set_job_status(job_id: int, new_status: str) -> bool:
    if new_status not in STATES:
        return False
    session = Session()
    try:
        job = session.get(Job, job_id)
        if not job:
            return False
        if job.status == new_status:
            return True
        if not can_transition(job.status, new_status):
            return False
        job.status = new_status
        if new_status == "running" and job.started_at is None:
            job.started_at = datetime.utcnow()
        session.commit()
        return True
    finally:
        session.close()


def _consecutive_missing_reached(session, job: Job) -> bool:
    limit = job.stop_on_consecutive_missing
    if not limit:
        return False
    rows = (
        session.query(Item)
        .filter(Item.job_id == job.id)
        .order_by(Item.external_id.desc())
        .limit(limit)
        .all()
    )
    if len(rows) < limit or any(row.status != "not_found" for row in rows):
        return False
    ids = [row.external_id for row in rows]
    return all(ids[index] - ids[index + 1] == 1 for index in range(len(ids) - 1))


def _stop_reason(session, job: Job):
    if job.stop_after_ids is not None:
        processed = session.query(Item).filter(Item.job_id == job.id).count()
        if processed >= job.stop_after_ids:
            return "limit_ids"
    if job.stop_after_seconds is not None and job.started_at:
        elapsed = datetime.utcnow() - job.started_at
        if elapsed >= timedelta(seconds=job.stop_after_seconds):
            return "limit_time"
    if _consecutive_missing_reached(session, job):
        return "consecutive_missing"
    return None


def _grow_open_range(session, job: Job):
    if job.range_end is not None:
        return None
    last = (
        session.query(RangeChunk)
        .filter(RangeChunk.job_id == job.id)
        .order_by(RangeChunk.range_end.desc())
        .first()
    )
    start = job.range_start if last is None else last.range_end + 1
    first, last_id = make_chunks(start, None, job.chunk_size)[0]
    chunk = RangeChunk(
        job_id=job.id,
        range_start=first,
        range_end=last_id,
        next_id=first,
        status="pending",
    )
    session.add(chunk)
    session.flush()
    return chunk


def _worker_settings(session, job: Job, worker: Worker):
    assignment = (
        session.query(JobWorker)
        .filter(
            JobWorker.job_id == job.id,
            JobWorker.worker_id == worker.id,
            JobWorker.enabled.is_(True),
        )
        .first()
    )
    if not assignment:
        return None
    active_count = (
        session.query(JobWorker)
        .join(Worker, Worker.id == JobWorker.worker_id)
        .filter(
            JobWorker.job_id == job.id,
            JobWorker.enabled.is_(True),
            Worker.enabled.is_(True),
            Worker.status == "online",
        )
        .count()
    )
    threads = assignment.threads or worker.threads
    per_worker_rps = job.global_rate_limit_rps / max(active_count, 1)
    return threads, max(per_worker_rps, 0.1)


def lease_chunk(worker_name: str, job_id: int):
    """Атомарно выдаёт один доступный блок подходящему агенту."""
    with _lease_lock:
        session = Session()
        try:
            job = session.get(Job, job_id)
            worker = session.query(Worker).filter(Worker.name == worker_name).first()
            if not job or job.status != "running" or not worker or not worker.enabled:
                return None
            settings = _worker_settings(session, job, worker)
            if settings is None:
                return None
            if worker.cpu >= CPU_LIMIT or worker.disk_free < worker.min_free_space_gb:
                return None
            reason = _stop_reason(session, job)
            if reason:
                job.status = "completed"
                session.commit()
                return None

            chunk = (
                session.query(RangeChunk)
                .filter(RangeChunk.job_id == job.id, RangeChunk.status == "pending")
                .order_by(RangeChunk.range_start)
                .first()
            )
            if chunk is None:
                chunk = _grow_open_range(session, job)
            if chunk is None:
                unfinished = (
                    session.query(RangeChunk)
                    .filter(
                        RangeChunk.job_id == job.id,
                        RangeChunk.status.in_(["pending", "leased"]),
                    )
                    .count()
                )
                if job.range_end is not None and unfinished == 0:
                    job.status = "completed"
                    session.commit()
                return None

            now = datetime.utcnow()
            chunk.status = "leased"
            chunk.leased_by = worker_name
            chunk.leased_at = now
            chunk.lease_expires_at = now + timedelta(seconds=LEASE_SECONDS)
            chunk.attempt += 1
            threads, rps = settings
            session.commit()
            return {
                "chunk_id": chunk.id,
                "range_start": chunk.range_start,
                "range_end": chunk.range_end,
                "next_id": chunk.next_id,
                "threads": threads,
                "rps": rps,
            }
        finally:
            session.close()


def renew_lease(chunk_id: int, worker_name: str, next_id: int) -> bool:
    session = Session()
    try:
        chunk = session.get(RangeChunk, chunk_id)
        if not chunk or chunk.status != "leased" or chunk.leased_by != worker_name:
            return False
        chunk.next_id = max(chunk.range_start, min(next_id, chunk.range_end + 1))
        chunk.lease_expires_at = datetime.utcnow() + timedelta(seconds=LEASE_SECONDS)
        session.commit()
        return True
    finally:
        session.close()


def complete_chunk(chunk_id: int, worker_name: str) -> bool:
    session = Session()
    try:
        chunk = session.get(RangeChunk, chunk_id)
        if not chunk or chunk.status != "leased" or chunk.leased_by != worker_name:
            return False
        chunk.status = "done"
        chunk.next_id = chunk.range_end + 1
        chunk.lease_expires_at = None
        session.commit()
        return True
    finally:
        session.close()


def fail_chunk(chunk_id: int, worker_name: str) -> bool:
    session = Session()
    try:
        chunk = session.get(RangeChunk, chunk_id)
        if not chunk or chunk.status != "leased" or chunk.leased_by != worker_name:
            return False
        chunk.status = "pending"
        chunk.leased_by = None
        chunk.lease_expires_at = None
        session.commit()
        return True
    finally:
        session.close()


def reclaim_expired():
    """Возвращает просроченные блоки и отдаёт список их идентификаторов."""
    session = Session()
    try:
        rows = (
            session.query(RangeChunk)
            .filter(
                RangeChunk.status == "leased",
                RangeChunk.lease_expires_at < datetime.utcnow(),
            )
            .all()
        )
        reclaimed = []
        for chunk in rows:
            reclaimed.append(chunk.id)
            chunk.status = "pending"
            chunk.leased_by = None
            chunk.lease_expires_at = None
        session.commit()
        return reclaimed
    finally:
        session.close()


def remote_action(worker_name: str, action: str, job_id: int | None = None):
    """Выполняет только probe, update или start; произвольных команд здесь нет."""
    if action not in {"probe", "update", "start"}:
        raise ValueError("действие запрещено")
    session = Session()
    try:
        worker = session.query(Worker).filter(Worker.name == worker_name).first()
        if not worker:
            raise ValueError("сервер не найден")
        data = {
            "name": worker.name,
            "host": worker.host,
            "port": worker.ssh_port,
            "user": worker.ssh_user,
            "key": worker.ssh_key_path,
            "path": worker.remote_path,
            "threads": worker.threads,
            "storage": worker.storage_path,
        }
    finally:
        session.close()

    if data["host"] in {"local", "localhost", "127.0.0.1"}:
        if action == "probe":
            return {"ok": True, "output": "локальный сервер доступен"}
        if action == "update":
            return {"ok": True, "output": "локальный проект обновляется вручную"}
        if job_id is None:
            raise ValueError("для запуска нужен job_id")
        env = os.environ.copy()
        env["STORAGE_PATH"] = data["storage"]
        subprocess.Popen(
            [sys.executable, "-m", "agent.agent", data["name"], str(job_id),
             "--threads", str(data["threads"])],
            env=env,
        )
        return {"ok": True, "output": "локальный агент запущен"}

    if not data["user"] or not data["path"]:
        raise ValueError("для SSH нужны ssh_user и remote_path")
    destination = f"{data['user']}@{data['host']}"
    ssh = ["ssh", "-o", "BatchMode=yes", "-p", str(data["port"])]
    if data["key"]:
        ssh.extend(["-i", data["key"]])
    ssh.append(destination)
    remote_path = shlex.quote(data["path"])
    if action == "probe":
        command = "true"
    elif action == "update":
        command = f"cd {remote_path} && git pull --ff-only && python3 -m pip install -r requirements.txt"
    else:
        if job_id is None:
            raise ValueError("для запуска нужен job_id")
        name = shlex.quote(data["name"])
        storage = shlex.quote(data["storage"])
        command = (
            f"cd {remote_path} && STORAGE_PATH={storage} nohup python3 -m agent.agent "
            f"{name} {int(job_id)} --threads {int(data['threads'])} > agent.log 2>&1 &"
        )
    completed = subprocess.run(
        ssh + [command], capture_output=True, text=True, timeout=30, check=False,
    )
    return {
        "ok": completed.returncode == 0,
        "output": (completed.stdout or completed.stderr)[-1000:],
    }
