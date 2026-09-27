"""HTTP API и веб-интерфейс координатора."""
import csv
import io
import logging
import re
from datetime import datetime, timedelta
from pathlib import Path, PurePosixPath

from fastapi import Depends, FastAPI, Form, Header, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field

from coordinator import security, service
from coordinator.db import (
    CONFIG, AuditLog, Download, Event, File, Item, Job, JobWorker,
    MediaMetadata, RangeChunk, Session, Worker, WorkerMetric, init_db,
)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
app = FastAPI(title="Video Harvester Coordinator")
templates = Jinja2Templates(directory="coordinator/templates")


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


def log_event(component: str, operation: str, result: str = "ok", level: str = "INFO",
              job_id=None, worker_id=None, chunk_id=None, external_id=None,
              error_code=None, message=None):
    message = mask_secrets(message)
    session = Session()
    try:
        session.add(Event(
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
        ))
        session.commit()
    finally:
        session.close()
    logging.getLogger(component).log(
        getattr(logging, level, logging.INFO),
        "%s %s %s", operation, result, message or "",
    )


def audit(user: str, action: str, target=None, details=None):
    session = Session()
    try:
        session.add(AuditLog(
            user=user,
            action=action,
            target=target,
            details=mask_secrets(details),
        ))
        session.commit()
    finally:
        session.close()


def bootstrap_workers():
    session = Session()
    try:
        for spec in CONFIG.get("servers", []):
            worker = session.query(Worker).filter(Worker.name == spec["name"]).first()
            if worker is None:
                worker = Worker(name=spec["name"])
                session.add(worker)
            worker.host = spec.get("host", "local")
            worker.ssh_port = int(spec.get("ssh_port", 22))
            worker.ssh_user = spec.get("ssh_user", "")
            worker.ssh_key_path = spec.get("ssh_key_path", "")
            worker.remote_path = spec.get("remote_path", "")
            worker.storage_path = spec.get("storage_path", "./data")
            worker.threads = int(spec.get("threads", 4))
            worker.min_free_space_gb = float(spec.get("min_free_space_gb", 0.1))
        session.commit()
    finally:
        session.close()


init_db()
security.ensure_bootstrap_users()
bootstrap_workers()


# ---------------- модели входных данных ----------------
class JobCreate(BaseModel):
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
    global_rate_limit_rps: float | None = Field(default=None, gt=0, le=10_000)


class WorkerRegister(BaseModel):
    name: str = Field(min_length=1, max_length=100)
    host: str = Field(default="local", max_length=255)
    threads: int = Field(default=4, ge=1, le=256)
    version: str = Field(default="1.0", max_length=50)


class MetricsPayload(BaseModel):
    cpu: float = 0
    ram_used: float = 0
    ram_total: float = 0
    disk_free: float = 0
    net_in: float = 0
    net_out: float = 0
    load_avg: float = 0
    open_files: int = 0
    active_connections: int = 0
    uptime: float = 0
    ids_per_second: float = 0
    bytes_per_second: float = 0
    active_checks: int = 0
    active_downloads: int = 0
    error_count: int = 0


class LeaseRequest(BaseModel):
    job_id: int


class ChunkRequest(BaseModel):
    chunk_id: int


class FileResult(BaseModel):
    path: str
    size: int = 0
    mime: str = ""
    checksum: str = ""


class ItemResult(BaseModel):
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


class ProgressPayload(BaseModel):
    job_id: int
    chunk_id: int
    next_id: int = Field(ge=0)
    items: list[ItemResult] = Field(default_factory=list)


class WorkerUpdate(BaseModel):
    threads: int | None = Field(default=None, ge=1, le=256)
    enabled: bool | None = None


class ServerCreate(BaseModel):
    name: str = Field(min_length=1, max_length=100)
    host: str = Field(default="local", max_length=255)
    ssh_port: int = Field(default=22, ge=1, le=65535)
    ssh_user: str = Field(default="", max_length=100)
    ssh_key_path: str = Field(default="", max_length=500)
    remote_path: str = Field(default="", max_length=500)
    storage_path: str = Field(default="./data", max_length=500)
    threads: int = Field(default=4, ge=1, le=256)
    min_free_space_gb: float = Field(default=0.1, ge=0)


def require_agent(x_agent_token: str = Header(default="")):
    if not security.check_agent_token(x_agent_token):
        raise HTTPException(401, "агент не авторизован")
    return True


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
    """Проверяет, что присланный путь находится внутри каталога этого сервера."""
    if worker.host in {"local", "localhost", "127.0.0.1"}:
        root = Path(worker.storage_path).resolve()
        candidate = Path(value).resolve()
    else:
        root = PurePosixPath(worker.storage_path)
        candidate = PurePosixPath(value)
    return candidate == root or root in candidate.parents


def _create_job(data: JobCreate):
    start, end = service.parse_range(data.range_text)
    session = Session()
    try:
        job = Job(
            name=data.name,
            range_start=start,
            range_end=end,
            chunk_size=data.chunk_size,
            request_timeout=data.request_timeout,
            max_retries=data.max_retries,
            global_rate_limit_rps=data.global_rate_limit_rps,
            stop_on_consecutive_missing=data.stop_on_consecutive_missing,
            stop_after_ids=data.stop_after_ids,
            stop_after_seconds=data.stop_after_seconds,
            status="validating",
        )
        session.add(job)
        session.flush()
        worker_query = session.query(Worker).filter(Worker.enabled.is_(True))
        if data.worker_names:
            worker_query = worker_query.filter(Worker.name.in_(data.worker_names))
        workers = worker_query.all()
        for worker in workers:
            session.add(JobWorker(job_id=job.id, worker_id=worker.id, threads=None))
        if end is not None:
            for first, last in service.make_chunks(start, end, data.chunk_size):
                session.add(RangeChunk(
                    job_id=job.id,
                    range_start=first,
                    range_end=last,
                    next_id=first,
                ))
        job.status = "created"
        session.commit()
        return job.id
    finally:
        session.close()


def _job_summary(session, job: Job):
    processed = session.query(Item).filter(Item.job_id == job.id).count()
    found = session.query(Item).filter(
        Item.job_id == job.id, Item.status.in_(["found", "completed"]),
    ).count()
    completed = session.query(Item).filter(
        Item.job_id == job.id, Item.status == "completed",
    ).count()
    errors = session.query(Item).filter(
        Item.job_id == job.id, Item.status.in_(["failed", "forbidden"]),
    ).count()
    downloaded = (
        session.query(File)
        .join(Item, Item.id == File.item_id)
        .filter(Item.job_id == job.id)
        .all()
    )
    elapsed = 0.0
    if job.started_at:
        elapsed = max((datetime.utcnow() - job.started_at).total_seconds(), 0.001)
    speed = processed / elapsed if elapsed else 0.0
    eta = None
    if job.range_end is not None and speed > 0:
        total = job.range_end - job.range_start + 1
        eta = max((total - processed) / speed, 0)
    return {
        "id": job.id,
        "name": job.name,
        "status": job.status,
        "range_start": job.range_start,
        "range_end": job.range_end,
        "processed": processed,
        "found": found,
        "completed": completed,
        "errors": errors,
        "ids_per_second": round(speed, 2),
        "downloaded_bytes": sum(row.size for row in downloaded),
        "eta_seconds": round(eta) if eta is not None else None,
    }


def _mark_stale_workers():
    cutoff = datetime.utcnow() - timedelta(seconds=service.LEASE_SECONDS * 2)
    session = Session()
    try:
        rows = session.query(Worker).filter(Worker.status == "online").all()
        for worker in rows:
            if worker.last_heartbeat and worker.last_heartbeat < cutoff:
                worker.status = "offline"
        session.commit()
    finally:
        session.close()


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
        return [_job_summary(session, job) for job in session.query(Job).order_by(Job.id.desc())]
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
def api_update_job(job_id: int, data: JobUpdate,
                   user=Depends(security.api_role("operator"))):
    session = Session()
    try:
        job = session.get(Job, job_id)
        if not job:
            raise HTTPException(404, "задание не найдено")
        if data.global_rate_limit_rps is not None:
            job.global_rate_limit_rps = data.global_rate_limit_rps
        session.commit()
        audit(user, "job_update", str(job_id))
        return {"job_id": job.id, "global_rate_limit_rps": job.global_rate_limit_rps}
    finally:
        session.close()


@app.post("/api/jobs/{job_id}/{action}")
def api_control_job(job_id: int, action: str,
                    user=Depends(security.api_role("operator"))):
    statuses = {"start": "running", "pause": "paused", "resume": "running", "stop": "stopped"}
    if action not in statuses:
        raise HTTPException(400, "неизвестное действие")
    if not service.set_job_status(job_id, statuses[action]):
        raise HTTPException(409, "недопустимый переход состояния")
    audit(user, f"job_{action}", str(job_id))
    log_event("coordinator", "job_status", result=statuses[action], job_id=job_id)
    return {"job_id": job_id, "status": statuses[action]}


@app.get("/api/jobs/{job_id}/items")
def api_items(job_id: int, status: str | None = None, limit: int = 200,
              _user=Depends(security.api_role("viewer"))):
    session = Session()
    try:
        query = session.query(Item).filter(Item.job_id == job_id)
        if status:
            query = query.filter(Item.status == status)
        rows = query.order_by(Item.external_id).limit(min(limit, 1000)).all()
        return [{
            "external_id": row.external_id,
            "status": row.status,
            "http_status": row.http_status,
            "title": row.title,
            "error_code": row.error_code,
        } for row in rows]
    finally:
        session.close()


@app.get("/api/jobs/{job_id}/events")
def api_events(job_id: int, limit: int = 200,
               _user=Depends(security.api_role("viewer"))):
    session = Session()
    try:
        rows = (
            session.query(Event)
            .filter(Event.job_id == job_id)
            .order_by(Event.id.desc())
            .limit(min(limit, 1000))
            .all()
        )
        return [{
            "ts": row.ts.isoformat(), "level": row.level,
            "operation": row.operation, "result": row.result,
            "worker_id": row.worker_id, "chunk_id": row.chunk_id,
            "external_id": row.external_id, "message": row.message,
        } for row in rows]
    finally:
        session.close()


@app.get("/api/workers")
def api_workers(_user=Depends(security.api_role("viewer"))):
    _mark_stale_workers()
    session = Session()
    try:
        return [{
            "name": row.name, "host": row.host, "status": row.status,
            "enabled": row.enabled, "threads": row.threads,
            "cpu": row.cpu, "ram_used": row.ram_used,
            "disk_free": row.disk_free, "ids_per_second": row.ids_per_second,
            "bytes_per_second": row.bytes_per_second,
            "last_heartbeat": row.last_heartbeat.isoformat() if row.last_heartbeat else None,
        } for row in session.query(Worker).order_by(Worker.name)]
    finally:
        session.close()


@app.patch("/api/workers/{name}")
def api_update_worker(name: str, data: WorkerUpdate,
                      user=Depends(security.api_role("operator"))):
    session = Session()
    try:
        worker = session.query(Worker).filter(Worker.name == name).first()
        if not worker:
            raise HTTPException(404, "сервер не найден")
        if data.threads is not None:
            worker.threads = data.threads
        if data.enabled is not None:
            worker.enabled = data.enabled
        session.commit()
        audit(user, "worker_update", name)
        return {"name": name, "threads": worker.threads, "enabled": worker.enabled}
    finally:
        session.close()


@app.post("/api/servers")
def api_add_server(data: ServerCreate,
                   user=Depends(security.api_role("administrator"))):
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
def api_remote_action(name: str, action: str, job_id: int | None = None,
                      user=Depends(security.api_role("administrator"))):
    try:
        result = service.remote_action(name, action, job_id)
    except ValueError as error:
        raise HTTPException(400, str(error)) from error
    audit(user, f"worker_{action}", name)
    return result


# ---------------- API агента ----------------
@app.post("/api/agent/register")
def agent_register(data: WorkerRegister, _ok=Depends(require_agent)):
    session = Session()
    try:
        worker = session.query(Worker).filter(Worker.name == data.name).first()
        if worker is None:
            worker = Worker(name=data.name)
            session.add(worker)
        worker.host = data.host
        worker.threads = data.threads
        worker.version = data.version
        worker.status = "online"
        worker.enabled = True
        worker.last_heartbeat = datetime.utcnow()
        session.commit()
        log_event("coordinator", "worker_register", worker_id=data.name)
        return {"worker_id": worker.id, "name": worker.name}
    finally:
        session.close()


@app.post("/api/agent/{name}/heartbeat")
def agent_heartbeat(name: str, data: MetricsPayload, _ok=Depends(require_agent)):
    session = Session()
    try:
        worker = session.query(Worker).filter(Worker.name == name).first()
        if not worker:
            raise HTTPException(404, "агент не зарегистрирован")
        worker.status = "online"
        worker.last_heartbeat = datetime.utcnow()
        for field in (
            "cpu", "ram_used", "ram_total", "disk_free", "ids_per_second",
            "bytes_per_second", "active_checks", "active_downloads", "error_count",
        ):
            setattr(worker, field, getattr(data, field))
        session.add(WorkerMetric(worker_id=worker.id, **data.model_dump(exclude={"error_count"})))
        session.commit()
        desired_threads = worker.threads
        min_free = worker.min_free_space_gb
    finally:
        session.close()

    if data.disk_free < min_free:
        log_event("agent", "disk_warning", result="low_space", level="WARNING",
                  worker_id=name, message=f"free_gb={data.disk_free}")
    if data.cpu >= 95 or data.error_count > 10:
        log_event("agent", "load_warning", result="overloaded", level="WARNING",
                  worker_id=name, message=f"cpu={data.cpu} errors={data.error_count}")
    reclaimed = service.reclaim_expired()
    for chunk_id in reclaimed:
        log_event("coordinator", "lease_expired", result="requeued", level="WARNING",
                  chunk_id=chunk_id)
    _mark_stale_workers()
    return {"ok": True, "desired_threads": desired_threads, "reclaimed": reclaimed}


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
        log_event("coordinator", "lease", job_id=data.job_id, worker_id=name,
                  chunk_id=chunk["chunk_id"])
    return {"chunk": chunk, "job_status": status}


@app.post("/api/agent/{name}/progress")
def agent_progress(name: str, data: ProgressPayload, _ok=Depends(require_agent)):
    session = Session()
    try:
        worker = session.query(Worker).filter(Worker.name == name).first()
        if not worker:
            raise HTTPException(404, "агент не зарегистрирован")
        for result in data.items:
            item = (
                session.query(Item)
                .filter(Item.job_id == data.job_id, Item.external_id == result.external_id)
                .first()
            )
            if item is None:
                item = Item(job_id=data.job_id, external_id=result.external_id)
                session.add(item)
                session.flush()
            item.status = result.status
            item.http_status = result.http_status
            item.title = result.title
            item.error_code = result.error_code
            item.worker_name = name

            if result.download_url:
                metadata = session.query(MediaMetadata).filter(
                    MediaMetadata.item_id == item.id,
                ).first()
                if metadata is None:
                    metadata = MediaMetadata(item_id=item.id)
                    session.add(metadata)
                metadata.title = result.title
                metadata.source_url = result.source_url
                metadata.download_url = result.download_url
                metadata.mime = result.mime
                metadata.expected_size = result.expected_size
                metadata.expected_checksum = result.expected_checksum

            if result.file and not _valid_worker_path(worker, result.file.path):
                item.status = "failed"
                item.error_code = "invalid_storage_path"
                session.add(Event(
                    component="coordinator", operation="path_rejected", result="failed",
                    level="WARNING", job_id=data.job_id, worker_id=name,
                    chunk_id=data.chunk_id, external_id=result.external_id,
                    error_code="invalid_storage_path", message="путь вне storage_path",
                ))
                continue

            if result.file and session.query(File).filter(File.item_id == item.id).first() is None:
                download = Download(
                    item_id=item.id,
                    worker_name=name,
                    status="completed",
                    started_at=datetime.utcnow(),
                    finished_at=datetime.utcnow(),
                )
                session.add(download)
                session.flush()
                session.add(File(
                    item_id=item.id,
                    download_id=download.id,
                    storage_path=result.file.path,
                    size=result.file.size,
                    mime=result.file.mime,
                    checksum=result.file.checksum,
                ))
            session.add(Event(
                component="agent", operation="item_result", result=item.status,
                level="ERROR" if item.status == "failed" else "INFO",
                job_id=data.job_id, worker_id=name, chunk_id=data.chunk_id,
                external_id=item.external_id, error_code=item.error_code,
                message=item.title,
            ))
        session.commit()
    finally:
        session.close()
    renewed = service.renew_lease(data.chunk_id, name, data.next_id)
    session = Session()
    try:
        job = session.get(Job, data.job_id)
        job_status = job.status if job else "missing"
    finally:
        session.close()
    return {"ok": True, "lease_renewed": renewed, "job_status": job_status}


@app.post("/api/agent/{name}/complete")
def agent_complete(name: str, data: ChunkRequest, _ok=Depends(require_agent)):
    if not service.complete_chunk(data.chunk_id, name):
        raise HTTPException(409, "агент не владеет блоком")
    log_event("coordinator", "chunk_complete", worker_id=name, chunk_id=data.chunk_id)
    return {"ok": True}


@app.post("/api/agent/{name}/fail")
def agent_fail(name: str, data: ChunkRequest, _ok=Depends(require_agent)):
    if not service.fail_chunk(data.chunk_id, name):
        raise HTTPException(409, "агент не владеет блоком")
    log_event("coordinator", "chunk_fail", result="requeued", level="WARNING",
              worker_id=name, chunk_id=data.chunk_id)
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
            "login.html", {"request": request, "error": "Неверный логин или пароль"},
            status_code=401,
        )
    response = RedirectResponse("/", status_code=303)
    response.set_cookie(
        "session", security.make_session(username), httponly=True,
        secure=bool(CONFIG["coordinator"].get("cookie_secure", True)),
        samesite="strict", max_age=security.SESSION_MAX_AGE,
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
        jobs = [_job_summary(session, job) for job in session.query(Job).order_by(Job.id.desc())]
        workers = session.query(Worker).order_by(Worker.name).all()
        data = _session_data(request)
        return templates.TemplateResponse("dashboard.html", {
            "request": request,
            "user": username,
            "role": security.user_role(username),
            "csrf": data["csrf"],
            "jobs": jobs,
            "workers": workers,
        })
    finally:
        session.close()


@app.post("/jobs/create")
def web_create_job(
    request: Request,
    csrf: str = Form(...),
    name: str = Form(...),
    range_text: str = Form(""),
    chunk_size: int = Form(100),
    global_rate_limit_rps: float = Form(20),
):
    username = _web_user(request, "operator")
    if not username:
        return RedirectResponse("/login", status_code=303)
    _check_csrf(request, csrf)
    data = JobCreate(
        name=name,
        range_text=range_text,
        chunk_size=chunk_size,
        global_rate_limit_rps=global_rate_limit_rps,
    )
    job_id = _create_job(data)
    audit(username, "job_create", str(job_id), name)
    return RedirectResponse(f"/jobs/{job_id}", status_code=303)


@app.get("/jobs/{job_id}", response_class=HTMLResponse)
def job_page(request: Request, job_id: int):
    username = _web_user(request)
    if not username:
        return RedirectResponse("/login", status_code=303)
    session = Session()
    try:
        job = session.get(Job, job_id)
        if not job:
            raise HTTPException(404, "задание не найдено")
        items = session.query(Item).filter(Item.job_id == job_id).order_by(Item.external_id).limit(300).all()
        events = session.query(Event).filter(Event.job_id == job_id).order_by(Event.id.desc()).limit(100).all()
        return templates.TemplateResponse("job.html", {
            "request": request,
            "user": username,
            "role": security.user_role(username),
            "csrf": _session_data(request)["csrf"],
            "job": job,
            "summary": _job_summary(session, job),
            "items": items,
            "events": events,
        })
    finally:
        session.close()


@app.post("/jobs/{job_id}/{action}")
def web_job_action(request: Request, job_id: int, action: str,
                   csrf: str = Form(...), rate: float | None = Form(None)):
    username = _web_user(request, "operator")
    if not username:
        return RedirectResponse("/login", status_code=303)
    _check_csrf(request, csrf)
    statuses = {"start": "running", "pause": "paused", "resume": "running", "stop": "stopped"}
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
        session = Session()
        try:
            failed = session.query(Item).filter(
                Item.job_id == job_id, Item.status.in_(["failed", "retry_wait"]),
            ).all()
            for item in failed:
                item.status = "pending"
                chunk = session.query(RangeChunk).filter(
                    RangeChunk.job_id == job_id,
                    RangeChunk.range_start <= item.external_id,
                    RangeChunk.range_end >= item.external_id,
                ).first()
                if chunk:
                    chunk.status = "pending"
                    chunk.next_id = min(chunk.next_id, item.external_id)
                    chunk.leased_by = None
                    chunk.lease_expires_at = None
            session.commit()
        finally:
            session.close()
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
        for item in session.query(Item).filter(Item.job_id == job_id).order_by(Item.external_id):
            writer.writerow([item.external_id, item.status, item.http_status or "",
                             item.title or "", item.error_code or ""])
        return StreamingResponse(
            iter([output.getvalue()]), media_type="text/csv; charset=utf-8",
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
            worker.name: session.query(Item).filter(Item.worker_name == worker.name)
            .order_by(Item.updated_at.desc()).limit(5).all()
            for worker in workers
        }
        return templates.TemplateResponse("servers.html", {
            "request": request, "user": username,
            "role": security.user_role(username),
            "csrf": _session_data(request)["csrf"], "workers": workers,
            "recent_items": recent_items,
        })
    finally:
        session.close()


@app.post("/servers/add")
def web_add_server(
    request: Request, csrf: str = Form(...), name: str = Form(...),
    host: str = Form("local"), ssh_port: int = Form(22), ssh_user: str = Form(""),
    remote_path: str = Form(""), storage_path: str = Form("./data"),
    threads: int = Form(4),
):
    username = _web_user(request, "administrator")
    if not username:
        return RedirectResponse("/login", status_code=303)
    _check_csrf(request, csrf)
    data = ServerCreate(
        name=name, host=host, ssh_port=ssh_port, ssh_user=ssh_user,
        remote_path=remote_path, storage_path=storage_path, threads=threads,
    )
    session = Session()
    try:
        if session.query(Worker).filter(Worker.name == data.name).first() is None:
            session.add(Worker(**data.model_dump(), status="offline"))
            session.commit()
    finally:
        session.close()
    audit(username, "server_add", name)
    return RedirectResponse("/servers", status_code=303)


@app.post("/servers/{name}/{action}")
def web_server_action(request: Request, name: str, action: str,
                      csrf: str = Form(...), job_id: int | None = Form(None),
                      threads: int | None = Form(None)):
    username = _web_user(request, "administrator")
    if not username:
        return RedirectResponse("/login", status_code=303)
    _check_csrf(request, csrf)
    if action in {"probe", "update", "start"}:
        result = service.remote_action(name, action, job_id)
        log_event("coordinator", f"worker_{action}", result="ok" if result["ok"] else "failed",
                  worker_id=name, message=result["output"])
    elif action == "threads" and threads is not None and 1 <= threads <= 256:
        session = Session()
        try:
            worker = session.query(Worker).filter(Worker.name == name).first()
            if not worker:
                raise HTTPException(404, "сервер не найден")
            worker.threads = threads
            session.commit()
        finally:
            session.close()
    elif action == "disable":
        session = Session()
        try:
            worker = session.query(Worker).filter(Worker.name == name).first()
            if worker:
                worker.enabled = False
                worker.status = "offline"
                session.commit()
        finally:
            session.close()
    else:
        raise HTTPException(400, "неизвестное действие")
    audit(username, f"worker_{action}", name)
    return RedirectResponse("/servers", status_code=303)


@app.get("/logs", response_class=HTMLResponse)
def logs_page(request: Request, q: str = "", level: str = "", job_id: int | None = None,
              worker_id: str = "", external_id: int | None = None, result: str = "",
              from_ts: str = "", to_ts: str = ""):
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
        if job_id is not None:
            query = query.filter(Event.job_id == job_id)
        if worker_id:
            query = query.filter(Event.worker_id == worker_id)
        if external_id is not None:
            query = query.filter(Event.external_id == external_id)
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
        return templates.TemplateResponse("logs.html", {
            "request": request, "user": username,
            "role": security.user_role(username), "events": events,
            "filters": {"q": q, "level": level, "job_id": job_id or "",
                        "worker_id": worker_id, "external_id": external_id or "",
                        "result": result, "from_ts": from_ts, "to_ts": to_ts},
        })
    finally:
        session.close()
