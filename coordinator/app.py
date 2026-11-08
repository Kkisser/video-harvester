"""HTTP API и веб-интерфейс координатора."""

import csv
import asyncio
import json
from contextlib import asynccontextmanager
from typing import Literal
import io
import logging
import re
from datetime import datetime, timedelta
from pathlib import Path

from fastapi import Depends, FastAPI, Form, Header, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field

from coordinator import security, service
from coordinator.db import MediaMetadata, Download
from coordinator.db import (
    CONFIG,
    AuditLog,
    Event,
    File,
    Item,
    Job,
    JobWorker,
    transaction,
    utcnow,
    RangeChunk,
    Session,
    Worker,
    WorkerMetric,
    init_db,
)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)


@asynccontextmanager
async def lifespan(app):
    async def sweeper():
        while True:
            try:
                await asyncio.to_thread(service.reclaim_expired)
            except Exception as error:
                logging.getLogger("coordinator").error(
                    "maintenance_failed type=%s", type(error).__name__
                )
            await asyncio.sleep(1)

    task = asyncio.create_task(sweeper())
    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


app = FastAPI(title="Video Harvester Coordinator", version="2.0", lifespan=lifespan)


@app.exception_handler(service.Conflict)
async def conflict_handler(request, error):
    from fastapi.responses import JSONResponse

    return JSONResponse({"detail": str(error)}, status_code=409)


@app.exception_handler(ValueError)
async def value_handler(request, error):
    from fastapi.responses import JSONResponse

    return JSONResponse({"detail": str(error)}, status_code=422)


templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))


SECRET_PATTERNS = [
    re.compile(r"(password\s*[:=]\s*)\S+", re.I),
    re.compile(r"(token\s*[:=]\s*)\S+", re.I),
    re.compile(r"(authorization\s*[:=]\s*)\S+", re.I),
    re.compile(r"(cookie\s*[:=]\s*)\S+", re.I),
]


def mask_secrets(text: str | None):
    if not text:
        return text
    for pattern in SECRET_PATTERNS:
        text = pattern.sub(r"\1***", text)
    return text


def log_event(
    component: str,
    operation: str,
    result: str = "ok",
    level: str = "INFO",
    job_id=None,
    worker_id=None,
    chunk_id=None,
    external_id=None,
    error_code=None,
    message=None,
):
    message = mask_secrets(message)
    session = Session()
    try:
        session.add(
            Event(
                component=component,
                operation=operation,
                result=result,
                level=level,
                job_id=job_id,
                worker_id=str(worker_id) if worker_id is not None else None,
                chunk_id=chunk_id,
                external_id=external_id,
                error_code=error_code,
                message=message,
            )
        )
        session.commit()
    finally:
        session.close()
    logging.getLogger(component).log(
        getattr(logging, level, logging.INFO),
        "%s %s %s",
        operation,
        result,
        message or "",
    )


def audit(user: str, action: str, target=None, details=None):
    session = Session()
    try:
        session.add(
            AuditLog(
                user=user,
                action=action,
                target=target,
                details=mask_secrets(details),
            )
        )
        session.commit()
    finally:
        session.close()


def bootstrap_workers():
    with transaction() as session:
        for spec in CONFIG.get("servers", []):
            if session.query(Worker).filter_by(name=spec["name"]).first():
                continue
            allowed = {column.name for column in Worker.__table__.columns}
            session.add(Worker(**{k: v for k, v in spec.items() if k in allowed}))


init_db()
security.ensure_bootstrap_users()
bootstrap_workers()


# ---------------- модели входных данных ----------------
class JobCreate(BaseModel):
    source: Literal["kinescope", "mock"] = "kinescope"
    threads: int = Field(default=4, ge=1, le=256)
    max_connections: int = Field(default=8, ge=1, le=256)
    check_rate_rps: float = Field(default=10, gt=0, le=10000)
    download_rate_rps: float = Field(default=10, gt=0, le=10000)
    auto_assign: bool = True
    no_worker_timeout: int = Field(default=120, ge=10, le=86400)
    worker_threads: dict[str, int] = Field(default_factory=dict)

    name: str = Field(min_length=1, max_length=100)
    range_text: str = Field(default="", max_length=100)
    chunk_size: int = Field(default=100, ge=1, le=100_000)
    request_timeout: int = Field(default=10, ge=1, le=300)
    max_retries: int = Field(default=4, ge=0, le=20)
    global_rate_limit_rps: float = Field(default=20, gt=0, le=10_000)
    stop_on_consecutive_missing: int | None = Field(default=None, ge=1)
    stop_after_ids: int | None = Field(default=None, ge=1)
    stop_after_seconds: int | None = Field(default=None, ge=1)
    worker_names: list[str] = Field(default_factory=list)


class JobUpdate(BaseModel):
    threads: int | None = Field(default=None, ge=1, le=256)
    check_rate_rps: float | None = Field(default=None, gt=0, le=10000)
    download_rate_rps: float | None = Field(default=None, gt=0, le=10000)

    global_rate_limit_rps: float | None = Field(default=None, gt=0, le=10_000)


class WorkerRegister(BaseModel):
    name: str = Field(min_length=1, max_length=100, pattern=r"^[a-zA-Z0-9_-]+$")
    host: str = Field(
        default="local", max_length=255, pattern=r"^[a-zA-Z0-9][a-zA-Z0-9.:-]*$"
    )
    threads: int = Field(default=4, ge=1, le=256)
    version: str = Field(default="2.0", max_length=50)


class MetricsPayload(BaseModel):
    job_id: int | None = None
    chunk_id: int | None = None
    lease_token: str | None = None
    swap_used: float = Field(default=0, ge=0)
    disk_total: float = Field(default=0, ge=0)
    disk_read: float = Field(default=0, ge=0)
    disk_write: float = Field(default=0, ge=0)
    error_ratio: float = Field(default=0, ge=0, le=1)
    http_requests: int = Field(default=0, ge=0)
    status_counts: dict[str, int] = Field(default_factory=dict)
    current: list[dict] = Field(default_factory=list, max_length=256)
    mean_check_seconds: float = Field(default=0, ge=0)
    mean_download_seconds: float = Field(default=0, ge=0)
    retries: int = Field(default=0, ge=0)

    cpu: float = Field(default=0, ge=0, le=100, allow_inf_nan=False)
    ram_used: float = Field(default=0, ge=0, allow_inf_nan=False)
    ram_total: float = Field(default=0, ge=0, allow_inf_nan=False)
    disk_free: float = Field(default=0, ge=0, allow_inf_nan=False)
    net_in: float = Field(default=0, ge=0, allow_inf_nan=False)
    net_out: float = Field(default=0, ge=0, allow_inf_nan=False)
    load_avg: float = Field(default=0, ge=0, allow_inf_nan=False)
    open_files: int = 0
    active_connections: int = 0
    uptime: float = Field(default=0, ge=0, allow_inf_nan=False)
    ids_per_second: float = Field(default=0, ge=0, allow_inf_nan=False)
    bytes_per_second: float = Field(default=0, ge=0, allow_inf_nan=False)
    active_checks: int = 0
    active_downloads: int = 0
    error_count: int = 0


class LeaseRequest(BaseModel):
    job_id: int


class ChunkRequest(BaseModel):
    job_id: int
    chunk_id: int
    lease_token: str = Field(min_length=16, max_length=128)


class FileResult(BaseModel):
    path: str
    size: int = Field(ge=0)
    mime: str = "video/mp4"
    checksum: str = Field(pattern=r"^[a-f0-9]{64}$")


class AttemptResult(BaseModel):
    attempt: int = Field(ge=1)
    status: Literal["completed", "failed", "retry_wait", "cancelled"]
    started_at: str
    finished_at: str
    error_code: str | None = None


class ItemResult(BaseModel):
    attempts: list[AttemptResult] = Field(default_factory=list, max_length=21)
    external_id: int = Field(ge=0)
    status: str
    http_status: int | None = None
    title: str | None = None
    error_code: str | None = None
    source_url: str | None = None
    download_url: str | None = None
    mime: str | None = None
    expected_size: int | None = None
    expected_checksum: str | None = None
    file: FileResult | None = None


class ProgressPayload(ChunkRequest):
    job_id: int
    chunk_id: int
    next_id: int | None = Field(default=None, ge=0)
    items: list[ItemResult] = Field(default_factory=list)


class WorkerUpdate(BaseModel):
    rps: float | None = Field(default=None, gt=0, le=10000)
    min_free_space_gb: float | None = Field(default=None, ge=0)
    ram_limit_percent: float | None = Field(default=None, gt=0, le=100)
    network_limit_mbps: float | None = Field(default=None, ge=0)
    disk_limit_mbps: float | None = Field(default=None, ge=0)

    threads: int | None = Field(default=None, ge=1, le=256)
    enabled: bool | None = None


class ServerCreate(BaseModel):
    rps: float = Field(default=20, gt=0, le=10000)

    name: str = Field(min_length=1, max_length=100, pattern=r"^[a-zA-Z0-9_-]+$")
    host: str = Field(
        default="local", max_length=255, pattern=r"^[a-zA-Z0-9][a-zA-Z0-9.:-]*$"
    )
    ssh_port: int = Field(default=22, ge=1, le=65535)
    ssh_user: str = Field(
        default="", max_length=100, pattern=r"^(?:[a-zA-Z_][a-zA-Z0-9_-]*)?$"
    )
    ssh_key_path: str = Field(default="", max_length=500)
    remote_path: str = Field(default="", max_length=500)
    storage_path: str = Field(default="./data", max_length=500)
    threads: int = Field(default=4, ge=1, le=256)
    min_free_space_gb: float = Field(default=0.1, ge=0)


def require_agent(
    request: Request,
    x_agent_token: str = Header(default=""),
    x_agent_name: str = Header(default=""),
):
    if not security.check_agent_token(x_agent_name, x_agent_token):
        raise HTTPException(401, "агент не авторизован")
    path_name = request.path_params.get("name")
    if path_name and path_name != x_agent_name:
        raise HTTPException(403, "токен принадлежит другому агенту")
    return x_agent_name


def _session_data(request: Request):
    return security.read_session(request.cookies.get("session"))


def _web_user(request: Request, minimum="viewer"):
    data = _session_data(request)
    if not data:
        return None
    username = data["username"]
    if not security.has_role(username, minimum):
        raise HTTPException(403, "недостаточно прав")
    return username


def _check_csrf(request: Request, token: str):
    if not security.verify_csrf(_session_data(request), token):
        raise HTTPException(403, "ошибка CSRF")


def _valid_worker_path(worker: Worker, value: str) -> bool:
    from common.paths import inside_storage

    return inside_storage(
        worker.storage_path,
        value,
        worker.host not in {"local", "localhost", "127.0.0.1"},
    )


def _create_job(data: JobCreate):
    start, end = service.parse_range(data.range_text)
    if data.source == "mock" and not CONFIG["coordinator"].get(
        "allow_test_source", False
    ):
        raise ValueError("Тестовый источник отключён в рабочей конфигурации")
    if any(t < 1 or t > 256 for t in data.worker_threads.values()):
        raise ValueError("Потоки: 1..256")
    with transaction() as session:
        values = data.model_dump(
            exclude={"range_text", "worker_names", "worker_threads"}
        )
        if data.worker_names:
            values["auto_assign"] = False
        job = Job(
            **values,
            range_start=start,
            range_end=end,
            next_range_start=start,
            status="created",
        )
        session.add(job)
        session.flush()
        workers = session.query(Worker).filter(Worker.enabled.is_(True)).all()
        names = {w.name for w in workers}
        if not set(data.worker_names).issubset(names) or not set(
            data.worker_threads
        ).issubset(names):
            raise ValueError("Неизвестный или отключённый сервер")
        for w in workers:
            if not data.worker_names or w.name in data.worker_names:
                session.add(
                    JobWorker(
                        job_id=job.id,
                        worker_id=w.id,
                        threads=data.worker_threads.get(w.name),
                    )
                )
        return job.id


def _oldest_pending(session, job_id):
    row = (
        session.query(RangeChunk)
        .filter(RangeChunk.job_id == job_id, RangeChunk.status == "pending")
        .order_by(RangeChunk.created_at)
        .first()
    )
    return round((utcnow() - row.created_at).total_seconds(), 1) if row else 0


def _job_summary(session, job: Job):
    processed = (
        session.query(Item)
        .filter(Item.job_id == job.id, Item.status.in_(service.TERMINAL))
        .count()
    )
    found = (
        session.query(Item)
        .filter(
            Item.job_id == job.id,
            Item.status.in_(["found", "completed"]),
        )
        .count()
    )
    completed = (
        session.query(Item)
        .filter(
            Item.job_id == job.id,
            Item.status == "completed",
        )
        .count()
    )
    errors = (
        session.query(Item)
        .filter(
            Item.job_id == job.id,
            Item.status.in_(["failed", "retry_wait"]),
        )
        .count()
    )
    downloaded = (
        session.query(File)
        .join(Item, Item.id == File.item_id)
        .filter(Item.job_id == job.id)
        .all()
    )
    elapsed = 0.0
    if job.started_at:
        elapsed = max(
            ((job.finished_at or utcnow()) - job.started_at).total_seconds(), 0.001
        )
    speed = processed / elapsed if elapsed else 0.0
    eta = None
    if service.effective_end(job) is not None and speed > 0:
        total = service.effective_end(job) - job.range_start + 1
        eta = max((total - processed) / speed, 0)
    assigned = (
        session.query(Worker)
        .join(JobWorker, JobWorker.worker_id == Worker.id)
        .filter(
            JobWorker.job_id == job.id,
            JobWorker.enabled.is_(True),
            Worker.enabled.is_(True),
            Worker.status == "online",
        )
        .all()
    )
    return {
        "current_ids_per_second": round(sum(w.ids_per_second for w in assigned), 2),
        "current_bytes_per_second": round(sum(w.bytes_per_second for w in assigned), 2),
        "active_servers": len(assigned),
        "active_threads": sum(w.active_checks + w.active_downloads for w in assigned),
        "id": job.id,
        "name": job.name,
        "status": job.status,
        "status_reason": job.status_reason,
        "source": job.source,
        "threads": job.threads,
        "range_start": job.range_start,
        "range_end": job.range_end,
        "processed": processed,
        "found": found,
        "completed": completed,
        "errors": errors,
        "ids_per_second": round(speed, 2),
        "downloaded_bytes": sum(row.size for row in downloaded),
        "bytes_per_second": round(sum(row.size for row in downloaded) / elapsed, 2)
        if elapsed
        else 0,
        "pending": session.query(Item)
        .filter(Item.job_id == job.id, ~Item.status.in_(service.TERMINAL))
        .count(),
        "oldest_pending_seconds": _oldest_pending(session, job.id),
        "eta_seconds": round(eta) if eta is not None else None,
    }


def _mark_stale_workers():
    service.reclaim_expired()


# ---------------- пользовательский REST API ----------------
@app.post("/api/jobs")
def api_create_job(data: JobCreate, user=Depends(security.api_role("operator"))):
    job_id = _create_job(data)
    audit(user, "job_create", str(job_id), data.name)
    log_event("coordinator", "job_create", job_id=job_id, message=data.name)
    return {"job_id": job_id, "status": "created"}


@app.get("/api/jobs")
def api_list_jobs(_user=Depends(security.api_role("viewer"))):
    session = Session()
    try:
        return [
            _job_summary(session, job)
            for job in session.query(Job).order_by(Job.id.desc())
        ]
    finally:
        session.close()


@app.get("/api/jobs/{job_id}")
def api_job(job_id: int, _user=Depends(security.api_role("viewer"))):
    session = Session()
    try:
        job = session.get(Job, job_id)
        if not job:
            raise HTTPException(404, "задание не найдено")
        return _job_summary(session, job)
    finally:
        session.close()


@app.patch("/api/jobs/{job_id}")
def api_update_job(
    job_id: int, data: JobUpdate, user=Depends(security.api_role("operator"))
):
    session = Session()
    try:
        job = session.get(Job, job_id)
        if not job:
            raise HTTPException(404, "задание не найдено")
        for key, value in data.model_dump(exclude_none=True).items():
            setattr(job, key, value)
        session.commit()
        audit(user, "job_update", str(job_id))
        return {"job_id": job.id, "global_rate_limit_rps": job.global_rate_limit_rps}
    finally:
        session.close()


@app.post("/api/jobs/{job_id}/{action}")
def api_control_job(
    job_id: int, action: str, user=Depends(security.api_role("operator"))
):
    statuses = {
        "start": "running",
        "pause": "paused",
        "resume": "running",
        "stop": "stopped",
    }
    if action == "retry_failed":
        count = service.retry_failed(job_id)
        audit(user, "job_retry_failed", str(job_id))
        return {"restarted": count}
    if action not in statuses:
        raise HTTPException(400, "неизвестное действие")
    if not service.set_job_status(job_id, statuses[action]):
        raise HTTPException(409, "недопустимый переход состояния")
    audit(user, f"job_{action}", str(job_id))
    log_event("coordinator", "job_status", result=statuses[action], job_id=job_id)
    return {"job_id": job_id, "status": statuses[action]}


@app.get("/api/jobs/{job_id}/items")
def api_items(
    job_id: int,
    status: str | None = None,
    limit: int = 200,
    offset: int = 0,
    _user=Depends(security.api_role("viewer")),
):
    session = Session()
    try:
        query = session.query(Item).filter(Item.job_id == job_id)
        if status:
            query = query.filter(Item.status == status)
        rows = (
            query.order_by(Item.external_id)
            .offset(max(offset, 0))
            .limit(max(1, min(limit, 1000)))
            .all()
        )
        return [
            {
                "external_id": row.external_id,
                "status": row.status,
                "http_status": row.http_status,
                "title": row.title,
                "error_code": row.error_code,
            }
            for row in rows
        ]
    finally:
        session.close()


@app.get("/api/jobs/{job_id}/events")
def api_events(
    job_id: int,
    limit: int = 200,
    offset: int = 0,
    _user=Depends(security.api_role("viewer")),
):
    session = Session()
    try:
        rows = (
            session.query(Event)
            .filter(Event.job_id == job_id)
            .order_by(Event.id.desc())
            .offset(max(offset, 0))
            .limit(max(1, min(limit, 1000)))
            .all()
        )
        return [
            {
                "ts": row.ts.isoformat(),
                "level": row.level,
                "operation": row.operation,
                "result": row.result,
                "worker_id": row.worker_id,
                "chunk_id": row.chunk_id,
                "external_id": row.external_id,
                "message": row.message,
            }
            for row in rows
        ]
    finally:
        session.close()


@app.get("/api/workers")
def api_workers(_user=Depends(security.api_role("viewer"))):
    _mark_stale_workers()
    session = Session()
    try:
        return [
            {
                "name": row.name,
                "host": row.host,
                "status": row.status,
                "enabled": row.enabled,
                "threads": row.threads,
                "cpu": row.cpu,
                "ram_used": row.ram_used,
                "disk_free": row.disk_free,
                "ids_per_second": row.ids_per_second,
                "bytes_per_second": row.bytes_per_second,
                "last_heartbeat": row.last_heartbeat.isoformat()
                if row.last_heartbeat
                else None,
            }
            for row in session.query(Worker).order_by(Worker.name)
        ]
    finally:
        session.close()


@app.patch("/api/workers/{name}")
def api_update_worker(
    name: str, data: WorkerUpdate, user=Depends(security.api_role("operator"))
):
    session = Session()
    try:
        worker = session.query(Worker).filter(Worker.name == name).first()
        if not worker:
            raise HTTPException(404, "сервер не найден")
        for key, value in data.model_dump(exclude_none=True).items():
            setattr(worker, key, value)
        if data.threads is not None:
            for assignment in session.query(JobWorker).filter_by(worker_id=worker.id):
                assignment.threads = data.threads
        session.commit()
        audit(user, "worker_update", name)
        return {"name": name, "threads": worker.threads, "enabled": worker.enabled}
    finally:
        session.close()


@app.post("/api/servers")
def api_add_server(
    data: ServerCreate, user=Depends(security.api_role("administrator"))
):
    session = Session()
    try:
        if session.query(Worker).filter(Worker.name == data.name).first():
            raise HTTPException(409, "сервер уже существует")
        session.add(Worker(**data.model_dump(), status="offline"))
        session.commit()
        audit(user, "server_add", data.name)
        return {"name": data.name, "status": "offline"}
    finally:
        session.close()


@app.post("/api/admin/workers/{name}/{action}")
def api_remote_action(
    name: str,
    action: str,
    job_id: int | None = None,
    user=Depends(security.api_role("administrator")),
):
    try:
        result = service.remote_action(name, action, job_id)
    except ValueError as error:
        raise HTTPException(400, str(error)) from error
    audit(user, f"worker_{action}", name)
    return result


# ---------------- API агента ----------------
@app.post("/api/agent/register")
def agent_register(data: WorkerRegister, identity=Depends(require_agent)):
    if data.name != identity:
        raise HTTPException(403, "Токен другого агента")
    if data.version.split(".")[0] != "2":
        raise HTTPException(409, "Требуется агент протокола 2.x")
    with transaction() as session:
        worker = session.query(Worker).filter_by(name=data.name).first()
        if worker is None:
            worker = Worker(
                name=data.name,
                host=data.host,
                threads=data.threads,
                storage_path=f"./data/{data.name}",
            )
            session.add(worker)
            session.flush()
        if data.version != "2.0":
            service.event(
                session, "agent_version_difference", worker=data.name, level="WARNING"
            )
        worker.version = data.version
        worker.status = "online"
        worker.last_heartbeat = utcnow()
        for job in session.query(Job).filter(
            Job.status.in_(["running", "created", "paused"])
        ):
            service.assign_workers(session, job)
        return {
            "worker_id": worker.id,
            "name": worker.name,
            "enabled": worker.enabled,
            "heartbeat_seconds": min(
                float(CONFIG["coordinator"].get("heartbeat_seconds", 3)),
                service.LEASE_SECONDS / 3,
            ),
            "lease_seconds": service.LEASE_SECONDS,
            "storage_path": worker.storage_path,
        }


@app.post("/api/agent/{name}/heartbeat")
def agent_heartbeat(name: str, data: MetricsPayload, _ok=Depends(require_agent)):
    with transaction() as session:
        worker = session.query(Worker).filter_by(name=name).first()
        if not worker:
            raise HTTPException(404, "Агент не зарегистрирован")
        worker.status = "online"
        worker.last_heartbeat = utcnow()
        for field in (
            "cpu",
            "ram_used",
            "ram_total",
            "disk_free",
            "ids_per_second",
            "bytes_per_second",
            "active_checks",
            "active_downloads",
            "error_count",
        ):
            setattr(worker, field, getattr(data, field))
        metrics = data.model_dump(exclude={"lease_token", "chunk_id", "job_id"})
        worker.metrics_json = json.dumps(
            {**metrics, "chunk_id": data.chunk_id}, ensure_ascii=False
        )
        fields = {col.name for col in WorkerMetric.__table__.columns} - {
            "id",
            "worker_id",
            "ts",
        }
        session.add(
            WorkerMetric(
                worker_id=worker.id, **{k: v for k, v in metrics.items() if k in fields}
            )
        )
        renewed = False
        job = session.get(Job, data.job_id) if data.job_id else None
        assignment = (
            session.query(JobWorker)
            .filter_by(job_id=data.job_id, worker_id=worker.id, enabled=True)
            .first()
            if job
            else None
        )
        if data.chunk_id and data.lease_token and worker.enabled:
            try:
                job, chunk = service.owned(
                    session, name, data.job_id, data.chunk_id, data.lease_token
                )
                chunk.lease_expires_at = utcnow() + timedelta(
                    seconds=service.LEASE_SECONDS
                )
                renewed = True
            except service.Conflict:
                pass
        desired = (
            (assignment.threads or job.threads)
            if assignment and job
            else worker.threads
        )
        settings = (
            service.settings(job, worker, assignment) if job and assignment else {}
        )
        if service.overloaded(worker):
            # Throttle warnings to one per minute, not one per heartbeat.
            last = (
                session.query(Event)
                .filter_by(worker_id=name, operation="resource_warning")
                .order_by(Event.id.desc())
                .first()
            )
            if not last or (utcnow() - last.ts).total_seconds() > 60:
                service.event(session, "resource_warning", worker=name, level="WARNING")
        return {
            "ok": True,
            "enabled": worker.enabled,
            "desired_threads": desired,
            "lease_renewed": renewed,
            "job_status": job.status if job else "missing",
            "settings": settings,
        }


@app.post("/api/agent/{name}/lease")
def agent_lease(name: str, data: LeaseRequest, _ok=Depends(require_agent)):
    chunk = service.lease_chunk(name, data.job_id)
    session = Session()
    try:
        job = session.get(Job, data.job_id)
        status = job.status if job else "missing"
    finally:
        session.close()
    if chunk:
        log_event(
            "coordinator",
            "lease",
            job_id=data.job_id,
            worker_id=name,
            chunk_id=chunk["chunk_id"],
        )
    return {"chunk": chunk, "job_status": status}


@app.post("/api/agent/{name}/progress")
def agent_progress(name: str, data: ProgressPayload, _ok=Depends(require_agent)):
    return service.save_progress(name, data.model_dump())


@app.post("/api/agent/{name}/complete")
def agent_complete(name: str, data: ChunkRequest, _ok=Depends(require_agent)):
    if not service.complete_chunk(data.chunk_id, name, data.lease_token, data.job_id):
        raise HTTPException(409, "агент не владеет блоком")
    log_event("coordinator", "chunk_complete", worker_id=name, chunk_id=data.chunk_id)
    return {"ok": True}


@app.post("/api/agent/{name}/fail")
def agent_fail(name: str, data: ChunkRequest, _ok=Depends(require_agent)):
    if not service.fail_chunk(data.chunk_id, name, data.lease_token, data.job_id):
        raise HTTPException(409, "агент не владеет блоком")
    log_event(
        "coordinator",
        "chunk_fail",
        result="requeued",
        level="WARNING",
        worker_id=name,
        chunk_id=data.chunk_id,
    )
    return {"ok": True}


@app.get("/api/health")
def health():
    return {"status": "ok"}


# ---------------- вход и страницы ----------------
@app.get("/login", response_class=HTMLResponse)
def login_page(request: Request):
    return templates.TemplateResponse("login.html", {"request": request, "error": None})


@app.post("/login")
def login(request: Request, username: str = Form(...), password: str = Form(...)):
    if not security.authenticate(username, password):
        return templates.TemplateResponse(
            "login.html",
            {"request": request, "error": "Неверный логин или пароль"},
            status_code=401,
        )
    response = RedirectResponse("/", status_code=303)
    response.set_cookie(
        "session",
        security.make_session(username),
        httponly=True,
        secure=bool(CONFIG["coordinator"].get("cookie_secure", True)),
        samesite="strict",
        max_age=security.SESSION_MAX_AGE,
    )
    audit(username, "login")
    return response


@app.get("/logout")
def logout(request: Request):
    data = _session_data(request)
    response = RedirectResponse("/login", status_code=303)
    response.delete_cookie("session")
    if data:
        audit(data["username"], "logout")
    return response


@app.get("/", response_class=HTMLResponse)
def dashboard(request: Request):
    username = _web_user(request)
    if not username:
        return RedirectResponse("/login", status_code=303)
    _mark_stale_workers()
    session = Session()
    try:
        jobs = [
            _job_summary(session, job)
            for job in session.query(Job).order_by(Job.id.desc())
        ]
        workers = session.query(Worker).order_by(Worker.name).all()
        data = _session_data(request)
        return templates.TemplateResponse(
            "dashboard.html",
            {
                "request": request,
                "user": username,
                "role": security.user_role(username),
                "csrf": data["csrf"],
                "jobs": jobs,
                "workers": workers,
            },
        )
    finally:
        session.close()


@app.post("/jobs/create")
async def web_create_job(request: Request):
    username = _web_user(request, "operator")
    if not username:
        return RedirectResponse("/login", 303)
    form = dict(await request.form())
    _check_csrf(request, form.pop("csrf", ""))
    form = {k: v for k, v in form.items() if v != ""}
    names = form.pop("worker_names", "")
    overrides = form.pop("worker_threads", "{}")
    form["worker_names"] = [x.strip() for x in names.split(",") if x.strip()]
    form["worker_threads"] = json.loads(overrides)
    job_id = _create_job(JobCreate(**form))
    audit(username, "job_create", str(job_id))
    return RedirectResponse(f"/jobs/{job_id}", 303)


@app.get("/jobs/{job_id}", response_class=HTMLResponse)
def job_page(request: Request, job_id: int, status: str = "", offset: int = 0):
    username = _web_user(request)
    if not username:
        return RedirectResponse("/login", status_code=303)
    session = Session()
    try:
        job = session.get(Job, job_id)
        if not job:
            raise HTTPException(404, "задание не найдено")
        query = session.query(Item).filter(Item.job_id == job_id)
        if status:
            query = query.filter(Item.status == status)
        items = query.order_by(Item.external_id).offset(max(0, offset)).limit(300).all()
        events = (
            session.query(Event)
            .filter(Event.job_id == job_id)
            .order_by(Event.id.desc())
            .limit(100)
            .all()
        )
        return templates.TemplateResponse(
            "job.html",
            {
                "request": request,
                "user": username,
                "role": security.user_role(username),
                "csrf": _session_data(request)["csrf"],
                "job": job,
                "summary": _job_summary(session, job),
                "filter_status": status,
                "offset": offset,
                "items": items,
                "events": events,
            },
        )
    finally:
        session.close()


@app.post("/jobs/{job_id}/assign")
def web_assign(
    request: Request,
    job_id: int,
    csrf: str = Form(...),
    worker_name: str = Form(...),
    threads: str = Form(""),
):
    username = _web_user(request, "operator")
    _check_csrf(request, csrf)
    if not username:
        raise HTTPException(401)
    assign_worker(
        job_id,
        worker_name,
        AssignmentRequest(threads=int(threads) if threads else None),
        username,
    )
    return RedirectResponse(f"/jobs/{job_id}", 303)


@app.post("/jobs/{job_id}/threads")
def web_threads(
    request: Request, job_id: int, csrf: str = Form(...), threads: int = Form(...)
):
    username = _web_user(request, "operator")
    _check_csrf(request, csrf)
    if not username:
        raise HTTPException(401)
    api_update_job(job_id, JobUpdate(threads=threads), username)
    return RedirectResponse(f"/jobs/{job_id}", 303)


@app.post("/jobs/{job_id}/{action}")
def web_job_action(
    request: Request,
    job_id: int,
    action: str,
    csrf: str = Form(...),
    rate: float | None = Form(None),
):
    username = _web_user(request, "operator")
    if not username:
        return RedirectResponse("/login", status_code=303)
    _check_csrf(request, csrf)
    statuses = {
        "start": "running",
        "pause": "paused",
        "resume": "running",
        "stop": "stopped",
    }
    if action in statuses:
        if not service.set_job_status(job_id, statuses[action]):
            raise HTTPException(409, "недопустимый переход")
    elif action == "update_rate" and rate is not None and rate > 0:
        session = Session()
        try:
            job = session.get(Job, job_id)
            if not job:
                raise HTTPException(404, "задание не найдено")
            job.global_rate_limit_rps = rate
            session.commit()
        finally:
            session.close()
    elif action == "retry_failed":
        service.retry_failed(job_id)
    else:
        raise HTTPException(400, "неизвестное действие")
    audit(username, f"job_{action}", str(job_id))
    return RedirectResponse(f"/jobs/{job_id}", status_code=303)


@app.get("/jobs/{job_id}/export")
def export_job(request: Request, job_id: int):
    username = _web_user(request)
    if not username:
        return RedirectResponse("/login", status_code=303)
    session = Session()
    try:
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["external_id", "status", "http_status", "title", "error_code"])
        for item in (
            session.query(Item).filter(Item.job_id == job_id).order_by(Item.external_id)
        ):
            writer.writerow(
                [
                    item.external_id,
                    item.status,
                    item.http_status or "",
                    item.title or "",
                    item.error_code or "",
                ]
            )
        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv; charset=utf-8",
            headers={"Content-Disposition": f"attachment; filename=job_{job_id}.csv"},
        )
    finally:
        session.close()


@app.get("/servers", response_class=HTMLResponse)
def servers_page(request: Request):
    username = _web_user(request)
    if not username:
        return RedirectResponse("/login", status_code=303)
    _mark_stale_workers()
    session = Session()
    try:
        workers = session.query(Worker).order_by(Worker.name).all()
        recent_items = {
            worker.name: session.query(Item)
            .filter(Item.worker_name == worker.name)
            .order_by(Item.updated_at.desc())
            .limit(5)
            .all()
            for worker in workers
        }
        return templates.TemplateResponse(
            "servers.html",
            {
                "request": request,
                "user": username,
                "role": security.user_role(username),
                "csrf": _session_data(request)["csrf"],
                "workers": workers,
                "recent_items": recent_items,
                "metrics": {
                    w.name: json.loads(w.metrics_json or "{}") for w in workers
                },
            },
        )
    finally:
        session.close()


@app.post("/servers/add")
async def web_add_server(request: Request):
    username = _web_user(request, "administrator")
    if not username:
        return RedirectResponse("/login", 303)
    form = dict(await request.form())
    _check_csrf(request, form.pop("csrf", ""))
    data = ServerCreate(**{k: v for k, v in form.items() if v != ""})
    with transaction() as session:
        if session.query(Worker).filter_by(name=data.name).first():
            raise ValueError("Сервер уже существует")
        session.add(Worker(**data.model_dump(), status="offline"))
    audit(username, "server_add", data.name)
    return RedirectResponse("/servers", 303)


@app.post("/servers/{name}/{action}")
def web_server_action(
    request: Request,
    name: str,
    action: str,
    csrf: str = Form(...),
    job_id: int | None = Form(None),
    threads: int | None = Form(None),
):
    username = _web_user(request, "administrator")
    if not username:
        return RedirectResponse("/login", status_code=303)
    _check_csrf(request, csrf)
    if action in {"probe", "update", "start"}:
        result = service.remote_action(name, action, job_id)
        log_event(
            "coordinator",
            f"worker_{action}",
            result="ok" if result["ok"] else "failed",
            worker_id=name,
            message=result["output"],
        )
    elif action == "threads" and threads is not None and 1 <= threads <= 256:
        session = Session()
        try:
            worker = session.query(Worker).filter(Worker.name == name).first()
            if not worker:
                raise HTTPException(404, "сервер не найден")
            worker.threads = threads
            for assignment in session.query(JobWorker).filter_by(worker_id=worker.id):
                assignment.threads = threads
            session.commit()
        finally:
            session.close()
    elif action in {"disable", "enable"}:
        session = Session()
        try:
            worker = session.query(Worker).filter(Worker.name == name).first()
            if worker:
                worker.enabled = action == "enable"
                if not worker.enabled:
                    worker.status = "offline"
                session.commit()
        finally:
            session.close()
    else:
        raise HTTPException(400, "неизвестное действие")
    audit(username, f"worker_{action}", name)
    return RedirectResponse("/servers", status_code=303)


@app.get("/logs", response_class=HTMLResponse)
def logs_page(
    request: Request,
    q: str = "",
    level: str = "",
    job_id: str = "",
    worker_id: str = "",
    external_id: str = "",
    result: str = "",
    from_ts: str = "",
    to_ts: str = "",
):
    username = _web_user(request)
    if not username:
        return RedirectResponse("/login", status_code=303)
    session = Session()
    try:
        query = session.query(Event).order_by(Event.id.desc())
        if q:
            query = query.filter(Event.message.contains(q))
        if level:
            query = query.filter(Event.level == level)
        if job_id:
            query = query.filter(Event.job_id == int(job_id))
        if worker_id:
            query = query.filter(Event.worker_id == worker_id)
        if external_id:
            query = query.filter(Event.external_id == int(external_id))
        if result:
            query = query.filter(Event.result == result)
        try:
            if from_ts:
                query = query.filter(Event.ts >= datetime.fromisoformat(from_ts))
            if to_ts:
                query = query.filter(Event.ts <= datetime.fromisoformat(to_ts))
        except ValueError:
            raise HTTPException(400, "неверный формат времени")
        events = query.limit(300).all()
        return templates.TemplateResponse(
            "logs.html",
            {
                "request": request,
                "user": username,
                "role": security.user_role(username),
                "events": events,
                "filters": {
                    "q": q,
                    "level": level,
                    "job_id": job_id or "",
                    "worker_id": worker_id,
                    "external_id": external_id or "",
                    "result": result,
                    "from_ts": from_ts,
                    "to_ts": to_ts,
                },
            },
        )
    finally:
        session.close()


# Protocol 2: request admission, live states and explicit worker assignment.
class PermitRequest(ChunkRequest):
    kind: Literal["check", "download"]


class FeedbackRequest(ChunkRequest):
    status_code: int = Field(ge=0, le=599)


class StateRequest(ChunkRequest):
    external_id: int = Field(ge=0)
    status: Literal["checking", "found", "download_queued", "downloading", "retry_wait"]
    title: str | None = None
    attempt: int | None = Field(default=None, ge=1, le=21)


class AssignmentRequest(BaseModel):
    enabled: bool = True
    threads: int | None = Field(default=None, ge=1, le=256)


@app.post("/api/agent/{name}/permit")
def agent_permit(name: str, data: PermitRequest, _ok=Depends(require_agent)):
    return service.permit(name, data.model_dump())


@app.post("/api/agent/{name}/feedback")
def agent_feedback(name: str, data: FeedbackRequest, _ok=Depends(require_agent)):
    return service.feedback(name, data.model_dump())


@app.post("/api/agent/{name}/state")
def agent_state(name: str, data: StateRequest, _ok=Depends(require_agent)):
    return service.item_state(name, data.model_dump())


@app.put("/api/jobs/{job_id}/workers/{name}")
def assign_worker(
    job_id: int,
    name: str,
    data: AssignmentRequest,
    user=Depends(security.api_role("operator")),
):
    with transaction() as session:
        worker = session.query(Worker).filter_by(name=name).first()
        if not worker or not session.get(Job, job_id):
            raise HTTPException(404, "Нет сервера или задания")
        row = (
            session.query(JobWorker)
            .filter_by(job_id=job_id, worker_id=worker.id)
            .first()
        )
        if not row:
            row = JobWorker(job_id=job_id, worker_id=worker.id)
            session.add(row)
        row.enabled = data.enabled
        row.threads = data.threads
    audit(user, "assign_worker", f"{job_id}/{name}")
    return {"ok": True}


@app.get("/api/workers/{name}")
def worker_detail(name: str, _user=Depends(security.api_role("viewer"))):
    with Session() as session:
        w = session.query(Worker).filter_by(name=name).first()
        if not w:
            raise HTTPException(404, "Нет сервера")
        chunk = (
            session.query(RangeChunk).filter_by(leased_by=name, status="leased").first()
        )
        return {
            "name": w.name,
            "host": w.host,
            "enabled": w.enabled,
            "status": w.status,
            "version": w.version,
            "chunk": {
                "id": chunk.id,
                "from": chunk.range_start,
                "to": chunk.range_end,
                "next_id": chunk.next_id,
            }
            if chunk
            else None,
            "metrics": json.loads(w.metrics_json or "{}"),
        }


@app.get("/api/workers/{name}/metrics")
def worker_metrics(
    name: str, limit: int = 60, _user=Depends(security.api_role("viewer"))
):
    with Session() as session:
        w = session.query(Worker).filter_by(name=name).first()
        if not w:
            raise HTTPException(404, "Нет сервера")
        return [
            {col.name: getattr(m, col.name) for col in WorkerMetric.__table__.columns}
            for m in session.query(WorkerMetric)
            .filter_by(worker_id=w.id)
            .order_by(WorkerMetric.id.desc())
            .limit(max(1, min(limit, 1000)))
        ]


@app.get("/api/audit")
def api_audit(limit: int = 100, _user=Depends(security.api_role("administrator"))):
    with Session() as session:
        return [
            {
                "ts": r.ts,
                "user": r.user,
                "action": r.action,
                "target": r.target,
                "details": r.details,
            }
            for r in session.query(AuditLog)
            .order_by(AuditLog.id.desc())
            .limit(max(1, min(limit, 1000)))
        ]


@app.get("/api/jobs/{job_id}/items/{external_id}")
def item_details(
    job_id: int, external_id: int, _user=Depends(security.api_role("viewer"))
):
    with Session() as session:
        item = (
            session.query(Item)
            .filter_by(job_id=job_id, external_id=external_id)
            .first()
        )
        if not item:
            raise HTTPException(404, "Нет результата")
        file = session.query(File).filter_by(item_id=item.id).first()
        metadata = session.query(MediaMetadata).filter_by(item_id=item.id).first()
        attempts = (
            session.query(Download)
            .filter_by(item_id=item.id)
            .order_by(Download.id)
            .all()
        )
        return {
            "external_id": external_id,
            "status": item.status,
            "title": item.title,
            "source_url": metadata.source_url if metadata else None,
            "file": {
                "path": file.storage_path,
                "size": file.size,
                "checksum": file.checksum,
                "mime": file.mime,
            }
            if file
            else None,
            "attempts": [
                {
                    "attempt": d.attempt,
                    "status": d.status,
                    "started_at": d.started_at,
                    "finished_at": d.finished_at,
                    "error_code": d.error_code,
                }
                for d in attempts
            ],
        }


@app.get("/audit", response_class=HTMLResponse)
def audit_page(request: Request):
    username = _web_user(request, "administrator")
    if not username:
        return RedirectResponse("/login", 303)
    return templates.TemplateResponse(
        "audit.html",
        {
            "request": request,
            "user": username,
            "role": "administrator",
            "events": api_audit(100, username),
        },
    )
