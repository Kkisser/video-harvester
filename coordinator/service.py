"""Transactional scheduler, fenced leases, checkpoints and shared request limits."""

import json
import secrets
import os, shlex, subprocess, sys, shutil
import time
from datetime import timedelta
from sqlalchemy import func
from coordinator.db import (
    CONFIG,
    Session,
    transaction,
    utcnow,
    Worker,
    Job,
    JobWorker,
    RangeChunk,
    Item,
    MediaMetadata,
    File,
    Download,
    Event,
    RateBucket,
)
from common.paths import inside_storage

LEASE_SECONDS = float(CONFIG["coordinator"].get("lease_seconds", 30))
TERMINAL = {"completed", "not_found", "forbidden", "skipped", "failed", "cancelled"}
TRANSITIONS = {
    "created": {"running", "failed"},
    "validating": {"created", "failed"},
    "running": {"paused", "stopped", "completed", "failed"},
    "pausing": {"paused", "stopped"},
    "paused": {"running", "stopped"},
    "stopping": {"stopped"},
    "stopped": {"running"},
    "failed": {"running"},
    "completed": set(),
}
STATES = set(TRANSITIONS)


class Conflict(ValueError):
    pass


def parse_range(value):
    value = (value or "").strip()
    if not value:
        return 0, None
    if value.startswith("[") and value.endswith("]"):
        parts = value[1:-1].split(",")
        if len(parts) != 2 or not parts[0].strip():
            raise ValueError("Ожидается [A,B] или [A,]")
        a, b = int(parts[0]), int(parts[1]) if parts[1].strip() else None
    else:
        a, b = 0, int(value)
    if (
        a < 0
        or (b is not None and b < a)
        or a > 2**63 - 1
        or (b is not None and b >= 2**63 - 1)
    ):
        raise ValueError("Диапазон должен удовлетворять 0 ≤ A ≤ B < 2^63−1")
    return a, b


def parse_cli_range(from_arg, to_arg):
    return parse_range(
        f'[{from_arg if from_arg is not None else 0},{to_arg if to_arg is not None else ""}]'
    )


def make_chunks(start, end, size):
    if size < 1:
        raise ValueError("Размер блока должен быть положительным")
    end = start + size - 1 if end is None else end
    return [(i, min(i + size - 1, end)) for i in range(start, end + 1, size)]


def can_transition(old, new):
    return new in TRANSITIONS.get(old, set())


def event(
    s,
    operation,
    job_id=None,
    worker=None,
    chunk=None,
    result="ok",
    level="INFO",
    message=None,
):
    s.add(
        Event(
            component="coordinator",
            operation=operation,
            job_id=job_id,
            worker_id=worker,
            chunk_id=chunk,
            result=result,
            level=level,
            message=message,
        )
    )


def release(s, chunk):
    chunk.status = "pending"
    chunk.leased_by = None
    chunk.lease_token = None
    chunk.lease_expires_at = None
    # Unconfirmed work can be retried; confirmed terminal results are retained.
    for item in s.query(Item).filter(
        Item.job_id == chunk.job_id,
        Item.external_id >= chunk.range_start,
        Item.external_id <= chunk.range_end,
        ~Item.status.in_(TERMINAL),
    ):
        item.status = "pending"


def set_job_status(job_id, new_status):
    with transaction() as s:
        job = s.get(Job, job_id)
        if not job or (
            job.status != new_status and not can_transition(job.status, new_status)
        ):
            return False
        job.status = new_status
        if new_status == "running":
            job.started_at = job.started_at or utcnow()
            job.finished_at = None
            job.status_reason = ""
        if new_status == "stopped":
            for chunk in s.query(RangeChunk).filter_by(job_id=job_id, status="leased"):
                release(s, chunk)
            job.finished_at = utcnow()
        event(s, "job_status", job_id, result=new_status)
        return True


def assign_workers(s, job):
    if not job.auto_assign:
        return
    assigned = {r.worker_id for r in s.query(JobWorker).filter_by(job_id=job.id)}
    for worker in s.query(Worker).filter_by(enabled=True):
        if worker.id not in assigned:
            s.add(JobWorker(job_id=job.id, worker_id=worker.id))
    s.flush()


def effective_end(job):
    end = job.range_end
    if job.stop_after_ids is not None:
        bound = min(job.range_start + job.stop_after_ids - 1, 2**63 - 2)
        end = min(end, bound) if end is not None else bound
    return end


def maintain(s):
    now = utcnow()
    reclaimed = []
    for chunk in s.query(RangeChunk).filter(
        RangeChunk.status == "leased", RangeChunk.lease_expires_at <= now
    ):
        event(
            s,
            "lease_expired",
            chunk.job_id,
            chunk.leased_by,
            chunk.id,
            "requeued",
            "WARNING",
        )
        reclaimed.append(chunk.id)
        release(s, chunk)
    for worker in s.query(Worker).filter_by(status="online"):
        if not worker.last_heartbeat or worker.last_heartbeat < now - timedelta(
            seconds=LEASE_SECONDS
        ):
            worker.status = "offline"
            event(s, "heartbeat_missing", worker=worker.name, level="WARNING")
    for job in s.query(Job).filter_by(status="running"):
        assign_workers(s, job)
        reason = None
        if (
            job.stop_after_seconds
            and job.started_at
            and (now - job.started_at).total_seconds() >= job.stop_after_seconds
        ):
            reason = "time_limit"
        online = (
            s.query(Worker)
            .join(JobWorker, JobWorker.worker_id == Worker.id)
            .filter(
                JobWorker.job_id == job.id,
                JobWorker.enabled.is_(True),
                Worker.enabled.is_(True),
                Worker.status == "online",
            )
            .count()
        )
        if not online and job.started_at:
            last = (
                s.query(func.max(Worker.last_heartbeat))
                .join(JobWorker, JobWorker.worker_id == Worker.id)
                .filter(JobWorker.job_id == job.id)
                .scalar()
            )
            if (
                now - max(last or job.started_at, job.started_at)
            ).total_seconds() >= job.no_worker_timeout:
                reason = "no_workers"
        if job.started_at and (now - job.started_at).total_seconds() > 60:
            latest = (
                s.query(func.max(Item.updated_at))
                .filter(Item.job_id == job.id)
                .scalar()
            )
            if (now - (latest or job.started_at)).total_seconds() > 60:
                last_warning = (
                    s.query(Event)
                    .filter_by(job_id=job.id, operation="job_stalled")
                    .order_by(Event.id.desc())
                    .first()
                )
                if not last_warning or (now - last_warning.ts).total_seconds() > 60:
                    event(
                        s,
                        "job_stalled",
                        job.id,
                        level="WARNING",
                        message="Нет новых результатов более 60 секунд; проверьте активные файлы",
                    )
        if reason:
            job.status = "failed" if reason == "no_workers" else "completed"
            job.status_reason = reason
            job.finished_at = now
            for chunk in s.query(RangeChunk).filter_by(job_id=job.id, status="leased"):
                release(s, chunk)


def reclaim_expired():
    with transaction() as s:
        before = [
            c.id
            for c in s.query(RangeChunk).filter(
                RangeChunk.status == "leased", RangeChunk.lease_expires_at <= utcnow()
            )
        ]
        maintain(s)
        return before


def settings(job, worker, assignment):
    return dict(
        threads=assignment.threads or job.threads,
        max_connections=job.max_connections,
        request_timeout=job.request_timeout,
        max_retries=job.max_retries,
        source=job.source,
        storage_path=worker.storage_path,
        min_free_space_gb=worker.min_free_space_gb,
        heartbeat_seconds=min(
            float(CONFIG["coordinator"].get("heartbeat_seconds", 3)), LEASE_SECONDS / 3
        ),
        lease_seconds=LEASE_SECONDS,
    )


def overloaded(worker):
    m = json.loads(worker.metrics_json or "{}")
    ram = worker.ram_used / worker.ram_total * 100 if worker.ram_total else 0
    return (
        worker.cpu >= 95
        or ram >= worker.ram_limit_percent
        or worker.disk_free < worker.min_free_space_gb
        or (
            worker.network_limit_mbps > 0
            and (m.get("net_in", 0) + m.get("net_out", 0)) / 1e6
            >= worker.network_limit_mbps
        )
        or (
            worker.disk_limit_mbps > 0
            and (m.get("disk_read", 0) + m.get("disk_write", 0)) / 1e6
            >= worker.disk_limit_mbps
        )
        or m.get("error_ratio", 0) > 0.8
    )


def lease_chunk(worker_name, job_id):
    with transaction() as s:
        maintain(s)
        job = s.get(Job, job_id)
        worker = s.query(Worker).filter_by(name=worker_name).first()
        if (
            not job
            or job.status != "running"
            or not worker
            or not worker.enabled
            or worker.status != "online"
        ):
            return None
        assignment = (
            s.query(JobWorker)
            .filter_by(job_id=job.id, worker_id=worker.id, enabled=True)
            .first()
        )
        if not assignment or overloaded(worker):
            return None
        # One live block per worker and job; repeat lease requests return the same fenced lease.
        chunk = (
            s.query(RangeChunk)
            .filter_by(job_id=job_id, status="leased", leased_by=worker_name)
            .first()
        )
        if not chunk:
            chunk = (
                s.query(RangeChunk)
                .filter_by(job_id=job_id, status="pending")
                .order_by(RangeChunk.range_start)
                .first()
            )
            if not chunk:
                end = effective_end(job)
                start = (
                    job.next_range_start
                    if job.next_range_start is not None
                    else job.range_start
                )
                if end is None or start <= end:
                    last = min(
                        start + job.chunk_size - 1,
                        end if end is not None else 2**63 - 2,
                    )
                    chunk = RangeChunk(
                        job_id=job.id,
                        range_start=start,
                        range_end=last,
                        next_id=start,
                        status="pending",
                    )
                    s.add(chunk)
                    s.flush()
                    job.next_range_start = last + 1
                elif (
                    not s.query(RangeChunk)
                    .filter_by(job_id=job_id, status="leased")
                    .count()
                ):
                    job.status = "completed"
                    job.finished_at = utcnow()
                    job.status_reason = "range_complete"
                    return None
            if not chunk:
                return None
            chunk.status = "leased"
            chunk.leased_by = worker_name
            chunk.leased_at = utcnow()
            chunk.lease_expires_at = utcnow() + timedelta(seconds=LEASE_SECONDS)
            chunk.lease_token = secrets.token_urlsafe(24)
            chunk.attempt += 1
            event(s, "lease", job_id, worker_name, chunk.id)
        done = [
            row.external_id
            for row in s.query(Item).filter(
                Item.job_id == job_id,
                Item.external_id >= chunk.next_id,
                Item.external_id <= chunk.range_end,
                Item.status.in_(TERMINAL),
            )
        ]
        return dict(
            chunk_id=chunk.id,
            job_id=job_id,
            range_start=chunk.range_start,
            range_end=chunk.range_end,
            next_id=chunk.next_id,
            lease_token=chunk.lease_token,
            attempt=chunk.attempt,
            skip_ids=done,
            **settings(job, worker, assignment),
        )


def owned(s, name, job_id, chunk_id, token, allow_done=False):
    chunk = s.get(RangeChunk, chunk_id)
    job = s.get(Job, job_id)
    if (
        not job
        or not chunk
        or chunk.job_id != job_id
        or chunk.leased_by != name
        or not token
        or chunk.lease_token != token
    ):
        raise Conflict("Блок принадлежит другой попытке или агенту")
    if allow_done and chunk.status == "done":
        return job, chunk
    if (
        chunk.status != "leased"
        or not chunk.lease_expires_at
        or chunk.lease_expires_at <= utcnow()
        or job.status not in {"running", "paused"}
    ):
        raise Conflict("Аренда истекла или задание остановлено")
    return job, chunk


def renew_lease(chunk_id, worker_name, next_id=None, lease_token=None, job_id=None):
    with transaction() as s:
        chunk = s.get(RangeChunk, chunk_id)
        if not chunk:
            return False
        try:
            job, chunk = owned(
                s, worker_name, job_id or chunk.job_id, chunk_id, lease_token
            )
        except Conflict:
            return False
        chunk.lease_expires_at = utcnow() + timedelta(seconds=LEASE_SECONDS)
        # A heartbeat cannot move a checkpoint. Only committed item results can.
        return True


def advance(s, chunk):
    rows = (
        s.query(Item)
        .filter(
            Item.job_id == chunk.job_id,
            Item.external_id >= chunk.next_id,
            Item.external_id <= chunk.range_end,
        )
        .order_by(Item.external_id)
        .all()
    )
    cursor = chunk.next_id
    for item in rows:
        if item.external_id != cursor or item.status not in TERMINAL:
            break
        cursor += 1
    chunk.next_id = max(chunk.next_id, cursor)


def save_progress(name, data):
    with transaction() as s:
        job, chunk = owned(
            s,
            name,
            data["job_id"],
            data["chunk_id"],
            data["lease_token"],
            allow_done=True,
        )
        worker = s.query(Worker).filter_by(name=name).one()
        for result in data.get("items", []):
            ident = result["external_id"]
            if not chunk.range_start <= ident <= chunk.range_end:
                raise Conflict("ID вне блока")
            status = result["status"]
            if status not in TERMINAL:
                raise ValueError("Для результата нужен конечный статус")
            item = s.query(Item).filter_by(job_id=job.id, external_id=ident).first()
            if item and item.status in TERMINAL:
                continue
            if chunk.status == "done":
                raise Conflict("Блок уже завершён")
            if item is None:
                item = Item(job_id=job.id, external_id=ident)
                s.add(item)
                s.flush()
            f = result.get("file")
            if status == "completed" and not f:
                raise ValueError("Завершённая загрузка должна содержать файл")
            if f and not inside_storage(
                worker.storage_path,
                f["path"],
                worker.host not in {"local", "localhost", "127.0.0.1"},
            ):
                raise ValueError("Путь вне каталога хранения")
            item.status = status
            item.http_status = result.get("http_status")
            item.title = result.get("title")
            item.error_code = result.get("error_code")
            item.worker_name = name
            if result.get("download_url"):
                md = s.query(MediaMetadata).filter_by(item_id=item.id).first()
                if not md:
                    md = MediaMetadata(item_id=item.id)
                    s.add(md)
                for key in [
                    "title",
                    "source_url",
                    "download_url",
                    "mime",
                    "expected_size",
                    "expected_checksum",
                ]:
                    setattr(md, key, result.get(key))
            for attempt in result.get("attempts", []):
                d = (
                    s.query(Download)
                    .filter_by(
                        item_id=item.id,
                        lease_token=chunk.lease_token,
                        attempt=attempt["attempt"],
                    )
                    .first()
                )
                if not d:
                    d = Download(
                        item_id=item.id,
                        worker_name=name,
                        lease_token=chunk.lease_token,
                        attempt=attempt["attempt"],
                    )
                    s.add(d)
                d.status = attempt["status"]
                d.error_code = attempt.get("error_code")
                from datetime import datetime

                d.started_at = datetime.fromisoformat(attempt["started_at"])
                d.finished_at = datetime.fromisoformat(attempt["finished_at"])
            if f:
                d = (
                    s.query(Download)
                    .filter_by(
                        item_id=item.id,
                        lease_token=chunk.lease_token,
                        status="completed",
                    )
                    .order_by(Download.attempt.desc())
                    .first()
                )
                if not d:
                    d = Download(
                        item_id=item.id,
                        worker_name=name,
                        lease_token=chunk.lease_token,
                        status="completed",
                        started_at=utcnow(),
                        finished_at=utcnow(),
                    )
                    s.add(d)
                    s.flush()
                old = s.query(File).filter_by(item_id=item.id).first()
                if old:
                    raise Conflict("Подтверждённый файл уже существует")
                s.add(
                    File(
                        item_id=item.id,
                        download_id=d.id,
                        storage_path=f["path"],
                        size=f["size"],
                        mime=f.get("mime"),
                        checksum=f["checksum"],
                    )
                )
            s.add(
                Event(
                    component="agent",
                    operation="item_result",
                    job_id=job.id,
                    worker_id=name,
                    chunk_id=chunk.id,
                    external_id=ident,
                    result=status,
                    error_code=item.error_code,
                    level="ERROR" if status == "failed" else "INFO",
                )
            )
        s.flush()
        advance(s, chunk)
        if chunk.status == "leased":
            chunk.lease_expires_at = utcnow() + timedelta(seconds=LEASE_SECONDS)
        # Missing-run stopping uses the highest CONTIGUOUS confirmed prefix, not arrival order.
        if job.stop_on_consecutive_missing:
            earlier = (
                s.query(RangeChunk)
                .filter(
                    RangeChunk.job_id == job.id,
                    RangeChunk.range_start < chunk.range_start,
                    RangeChunk.status != "done",
                )
                .count()
            )
            if not earlier:
                end = chunk.next_id - 1
                limit = job.stop_on_consecutive_missing
                count = (
                    s.query(Item)
                    .filter(
                        Item.job_id == job.id,
                        Item.external_id > end - limit,
                        Item.external_id <= end,
                        Item.status == "not_found",
                    )
                    .count()
                )
                if count == limit:
                    job.status = "completed"
                    job.status_reason = "consecutive_missing"
                    job.finished_at = utcnow()
                    for active in s.query(RangeChunk).filter_by(
                        job_id=job.id, status="leased"
                    ):
                        release(s, active)
        return dict(ok=True, next_id=chunk.next_id, job_status=job.status)


def item_state(name, data):
    with transaction() as s:
        job, chunk = owned(
            s, name, data["job_id"], data["chunk_id"], data["lease_token"]
        )
        ident = data["external_id"]
        if not chunk.range_start <= ident <= chunk.range_end:
            raise Conflict("ID вне блока")
        item = s.query(Item).filter_by(job_id=job.id, external_id=ident).first()
        if item and item.status in TERMINAL:
            return {"ok": True, "already_done": True}
        if not item:
            item = Item(job_id=job.id, external_id=ident)
            s.add(item)
            s.flush()
        item.status = data["status"]
        item.worker_name = name
        item.title = data.get("title") or item.title
        if data.get("attempt"):
            d = (
                s.query(Download)
                .filter_by(
                    item_id=item.id,
                    lease_token=chunk.lease_token,
                    attempt=data["attempt"],
                )
                .first()
            )
            if not d:
                s.add(
                    Download(
                        item_id=item.id,
                        worker_name=name,
                        lease_token=chunk.lease_token,
                        attempt=data["attempt"],
                        status="downloading",
                        started_at=utcnow(),
                    )
                )
        return {"ok": True, "already_done": False}


def complete_chunk(chunk_id, worker_name, lease_token=None, job_id=None):
    with transaction() as s:
        c = s.get(RangeChunk, chunk_id)
        if not c:
            return False
        try:
            job, c = owned(
                s,
                worker_name,
                job_id or c.job_id,
                chunk_id,
                lease_token,
                allow_done=True,
            )
        except Conflict:
            return False
        advance(s, c)
        if c.next_id <= c.range_end:
            return False
        c.status = "done"
        c.lease_expires_at = None
        event(s, "chunk_complete", job.id, worker_name, c.id)
        return True


def fail_chunk(chunk_id, worker_name, lease_token=None, job_id=None):
    with transaction() as s:
        c = s.get(RangeChunk, chunk_id)
        if not c:
            return False
        try:
            owned(s, worker_name, job_id or c.job_id, chunk_id, lease_token)
        except Conflict:
            return False
        release(s, c)
        return True


def retry_failed(job_id):
    with transaction() as s:
        job = s.get(Job, job_id)
        if not job:
            raise ValueError("Задание не найдено")
        if s.query(RangeChunk).filter_by(job_id=job_id, status="leased").count():
            raise Conflict("Сначала остановите задание: есть активные блоки")
        rows = (
            s.query(Item)
            .filter(
                Item.job_id == job_id,
                Item.status.in_(["failed", "retry_wait", "cancelled"]),
            )
            .all()
        )
        for item in rows:
            item.status = "pending"
            item.error_code = None
            c = (
                s.query(RangeChunk)
                .filter(
                    RangeChunk.job_id == job_id,
                    RangeChunk.range_start <= item.external_id,
                    RangeChunk.range_end >= item.external_id,
                )
                .one()
            )
            release(s, c)
            c.next_id = min(c.next_id, item.external_id)
        if rows:
            job.status = "running"
            job.finished_at = None
            job.started_at = utcnow()
            job.status_reason = "explicit_retry"
        event(s, "retry_failed", job_id, message=f"items={len(rows)}")
        return len(rows)


def permit(name, data):
    """Grant now, never reserve future slots. SQLite commits serialize all agents."""
    with transaction() as s:
        job, c = owned(s, name, data["job_id"], data["chunk_id"], data["lease_token"])
        w = s.query(Worker).filter_by(name=name).one()
        if not w.enabled:
            raise Conflict("Сервер отключён")
        now = time.time()
        kind = data["kind"]
        factor = job.adaptive_factor
        limits = [
            (f"job:{job.id}", job.global_rate_limit_rps * factor),
            (
                f"job:{job.id}:{kind}",
                (job.check_rate_rps if kind == "check" else job.download_rate_rps)
                * factor,
            ),
            (f"worker:{name}", w.rps),
        ]
        buckets = []
        for key, rate in limits:
            row = s.get(RateBucket, key)
            if not row:
                row = RateBucket(key=key, next_at=0)
                s.add(row)
            buckets.append((row, max(rate, 0.01)))
        wait = max([0] + [r.next_at - now for r, _ in buckets])
        if wait > 0:
            return {"granted": False, "wait_seconds": min(wait, 2)}
        for row, rate in buckets:
            row.next_at = now + 1 / rate
        return {"granted": True, "wait_seconds": 0}


def feedback(name, data):
    with transaction() as s:
        job, c = owned(s, name, data["job_id"], data["chunk_id"], data["lease_token"])
        code = data["status_code"]
        if code == 429 or code >= 500:
            job.recent_bad += 1
        else:
            job.recent_good += 1
        total = job.recent_bad + job.recent_good
        if total >= 20:
            ratio = job.recent_bad / total
            if ratio > 0.2:
                job.adaptive_factor = max(0.05, job.adaptive_factor / 2)
                event(s, "rate_reduced", job.id, level="WARNING")
            elif ratio < 0.05:
                job.adaptive_factor = min(1, job.adaptive_factor * 1.25)
            job.recent_bad = job.recent_good = 0
        return {"factor": job.adaptive_factor}


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
            return {
                "ok": bool(shutil.which("ffmpeg") and shutil.which("ffprobe")),
                "output": "Проверка локальных FFmpeg и ffprobe",
            }
        if action == "update":
            result = subprocess.run(
                ["git", "pull", "--ff-only"], capture_output=True, text=True, timeout=60
            )
            if result.returncode == 0:
                result = subprocess.run(
                    [sys.executable, "-m", "pip", "install", "-r", "requirements.txt"],
                    capture_output=True,
                    text=True,
                    timeout=180,
                )
            return {
                "ok": result.returncode == 0,
                "output": "Обновление выполнено"
                if result.returncode == 0
                else "Обновление не выполнено; проверьте серверный журнал",
            }
        if job_id is None:
            raise ValueError("для запуска нужен job_id")
        env = os.environ.copy()
        env["STORAGE_PATH"] = data["storage"]
        subprocess.Popen(
            [
                sys.executable,
                "-m",
                "agent.agent",
                data["name"],
                str(job_id),
                "--threads",
                str(data["threads"]),
                "--env-file",
                f".env.{data['name']}",
            ],
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
        command = f"cd {remote_path} && test -x .venv/bin/python && test -r .env.agent && .venv/bin/python -m agent.agent --check"
    elif action == "update":
        command = f"cd {remote_path} && git pull --ff-only && .venv/bin/python -m pip install -r requirements.txt"
    else:
        if job_id is None:
            raise ValueError("для запуска нужен job_id")
        name = shlex.quote(data["name"])
        command = (
            f"cd {remote_path} && test -x .venv/bin/python && test -r .env.agent && {{ nohup .venv/bin/python -m agent.agent "
            f"{name} {int(job_id)} --env-file .env.agent >> agent.log 2>&1 < /dev/null & }}"
        )
    completed = subprocess.run(
        ssh + [command],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    return {
        "ok": completed.returncode == 0,
        "output": "Команда выполнена"
        if completed.returncode == 0
        else "Команда завершилась ошибкой; проверьте узел и журнал SSH",
    }
