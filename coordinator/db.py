"""Настройки и модель данных координатора.

SQLite используется для простого лабораторного стенда. Режим WAL уменьшает
число конфликтов чтения и записи. DATABASE_URL можно заменить на PostgreSQL.
Все отметки времени сохраняются в UTC.
"""
import os
from datetime import datetime
from pathlib import Path

import yaml
from sqlalchemy import (
    BigInteger, Boolean, Column, DateTime, Float, ForeignKey, Integer,
    String, Text, UniqueConstraint, create_engine, event,
)
from sqlalchemy.orm import declarative_base, sessionmaker


def utcnow():
    return datetime.utcnow()


def load_config():
    path = Path(os.environ.get("CONFIG_PATH", "config.yaml"))
    if not path.exists():
        path = Path("config.example.yaml")
    data = {}
    if path.exists():
        with path.open(encoding="utf-8") as stream:
            data = yaml.safe_load(stream) or {}
    data.setdefault("coordinator", {})
    data.setdefault("agent", {})
    data.setdefault("servers", [])
    data["coordinator"]["database_url"] = os.environ.get(
        "DATABASE_URL",
        data["coordinator"].get("database_url", "sqlite:///coordinator.db"),
    )
    return data


CONFIG = load_config()
DATABASE_URL = CONFIG["coordinator"]["database_url"]

engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False} if DATABASE_URL.startswith("sqlite") else {},
)


if DATABASE_URL.startswith("sqlite"):
    @event.listens_for(engine, "connect")
    def configure_sqlite(dbapi_connection, _connection_record):
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.close()


Session = sessionmaker(bind=engine, expire_on_commit=False)
Base = declarative_base()


class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True)
    username = Column(String, unique=True, nullable=False)
    password_hash = Column(String, nullable=False)
    created_at = Column(DateTime, default=utcnow, nullable=False)


class Role(Base):
    __tablename__ = "roles"
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True, nullable=False)


class UserRole(Base):
    __tablename__ = "user_roles"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    role_id = Column(Integer, ForeignKey("roles.id"), nullable=False)
    __table_args__ = (UniqueConstraint("user_id", "role_id", name="uq_user_role"),)


class Worker(Base):
    __tablename__ = "workers"
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True, nullable=False)
    host = Column(String, default="local", nullable=False)
    ssh_port = Column(Integer, default=22, nullable=False)
    ssh_user = Column(String, default="")
    ssh_key_path = Column(String, default="")
    remote_path = Column(String, default="")
    storage_path = Column(String, default="./data")
    min_free_space_gb = Column(Float, default=0.1)
    enabled = Column(Boolean, default=True, nullable=False)
    status = Column(String, default="offline", nullable=False)
    threads = Column(Integer, default=4, nullable=False)
    version = Column(String, default="1.0")
    last_heartbeat = Column(DateTime, nullable=True)
    cpu = Column(Float, default=0)
    ram_used = Column(Float, default=0)
    ram_total = Column(Float, default=0)
    disk_free = Column(Float, default=0)
    ids_per_second = Column(Float, default=0)
    bytes_per_second = Column(Float, default=0)
    active_checks = Column(Integer, default=0)
    active_downloads = Column(Integer, default=0)
    error_count = Column(Integer, default=0)
    created_at = Column(DateTime, default=utcnow, nullable=False)


class WorkerMetric(Base):
    __tablename__ = "worker_metrics"
    id = Column(Integer, primary_key=True)
    worker_id = Column(Integer, ForeignKey("workers.id"), nullable=False)
    ts = Column(DateTime, default=utcnow, nullable=False)
    cpu = Column(Float, default=0)
    ram_used = Column(Float, default=0)
    ram_total = Column(Float, default=0)
    disk_free = Column(Float, default=0)
    net_in = Column(Float, default=0)
    net_out = Column(Float, default=0)
    load_avg = Column(Float, default=0)
    open_files = Column(Integer, default=0)
    active_connections = Column(Integer, default=0)
    uptime = Column(Float, default=0)
    ids_per_second = Column(Float, default=0)
    bytes_per_second = Column(Float, default=0)
    active_checks = Column(Integer, default=0)
    active_downloads = Column(Integer, default=0)


class Job(Base):
    __tablename__ = "jobs"
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    range_start = Column(BigInteger, default=0, nullable=False)
    range_end = Column(BigInteger, nullable=True)
    chunk_size = Column(Integer, default=100, nullable=False)
    request_timeout = Column(Integer, default=10, nullable=False)
    max_retries = Column(Integer, default=4, nullable=False)
    global_rate_limit_rps = Column(Float, default=20, nullable=False)
    stop_on_consecutive_missing = Column(Integer, nullable=True)
    stop_after_ids = Column(BigInteger, nullable=True)
    stop_after_seconds = Column(Integer, nullable=True)
    status = Column(String, default="created", nullable=False)
    created_at = Column(DateTime, default=utcnow, nullable=False)
    updated_at = Column(DateTime, default=utcnow, onupdate=utcnow, nullable=False)
    started_at = Column(DateTime, nullable=True)


class JobWorker(Base):
    __tablename__ = "job_workers"
    id = Column(Integer, primary_key=True)
    job_id = Column(Integer, ForeignKey("jobs.id"), nullable=False)
    worker_id = Column(Integer, ForeignKey("workers.id"), nullable=False)
    threads = Column(Integer, nullable=True)
    enabled = Column(Boolean, default=True, nullable=False)
    __table_args__ = (UniqueConstraint("job_id", "worker_id", name="uq_job_worker"),)


class RangeChunk(Base):
    __tablename__ = "range_chunks"
    id = Column(Integer, primary_key=True)
    job_id = Column(Integer, ForeignKey("jobs.id"), nullable=False)
    range_start = Column(BigInteger, nullable=False)
    range_end = Column(BigInteger, nullable=False)
    next_id = Column(BigInteger, nullable=False)
    status = Column(String, default="pending", nullable=False)
    leased_by = Column(String, nullable=True)
    leased_at = Column(DateTime, nullable=True)
    lease_expires_at = Column(DateTime, nullable=True)
    attempt = Column(Integer, default=0, nullable=False)
    created_at = Column(DateTime, default=utcnow, nullable=False)
    updated_at = Column(DateTime, default=utcnow, onupdate=utcnow, nullable=False)
    __table_args__ = (
        UniqueConstraint("job_id", "range_start", "range_end", name="uq_range_chunk"),
    )


class Item(Base):
    __tablename__ = "items"
    id = Column(Integer, primary_key=True)
    job_id = Column(Integer, ForeignKey("jobs.id"), nullable=False)
    external_id = Column(BigInteger, nullable=False)
    status = Column(String, default="pending", nullable=False)
    http_status = Column(Integer, nullable=True)
    title = Column(String, nullable=True)
    error_code = Column(String, nullable=True)
    worker_name = Column(String, nullable=True)
    updated_at = Column(DateTime, default=utcnow, onupdate=utcnow, nullable=False)
    __table_args__ = (UniqueConstraint("job_id", "external_id", name="uq_job_item"),)


class MediaMetadata(Base):
    __tablename__ = "media_metadata"
    id = Column(Integer, primary_key=True)
    item_id = Column(Integer, ForeignKey("items.id"), unique=True, nullable=False)
    title = Column(String, nullable=True)
    source_url = Column(String, nullable=True)
    download_url = Column(String, nullable=True)
    mime = Column(String, nullable=True)
    expected_size = Column(BigInteger, nullable=True)
    expected_checksum = Column(String, nullable=True)
    discovered_at = Column(DateTime, default=utcnow, nullable=False)


class Download(Base):
    __tablename__ = "downloads"
    id = Column(Integer, primary_key=True)
    item_id = Column(Integer, ForeignKey("items.id"), nullable=False)
    worker_name = Column(String, nullable=False)
    status = Column(String, default="queued", nullable=False)
    attempt = Column(Integer, default=1, nullable=False)
    started_at = Column(DateTime, nullable=True)
    finished_at = Column(DateTime, nullable=True)
    error_code = Column(String, nullable=True)


class File(Base):
    __tablename__ = "files"
    id = Column(Integer, primary_key=True)
    item_id = Column(Integer, ForeignKey("items.id"), unique=True, nullable=False)
    download_id = Column(Integer, ForeignKey("downloads.id"), nullable=False)
    storage_path = Column(String, nullable=False)
    size = Column(BigInteger, default=0, nullable=False)
    mime = Column(String, nullable=True)
    checksum = Column(String, nullable=False)
    created_at = Column(DateTime, default=utcnow, nullable=False)


class Event(Base):
    __tablename__ = "events"
    id = Column(Integer, primary_key=True)
    ts = Column(DateTime, default=utcnow, nullable=False)
    level = Column(String, default="INFO", nullable=False)
    component = Column(String, nullable=False)
    job_id = Column(Integer, nullable=True)
    worker_id = Column(String, nullable=True)
    chunk_id = Column(Integer, nullable=True)
    external_id = Column(BigInteger, nullable=True)
    operation = Column(String, nullable=False)
    result = Column(String, nullable=True)
    error_code = Column(String, nullable=True)
    message = Column(Text, nullable=True)


class AuditLog(Base):
    __tablename__ = "audit_log"
    id = Column(Integer, primary_key=True)
    ts = Column(DateTime, default=utcnow, nullable=False)
    user = Column(String, nullable=False)
    action = Column(String, nullable=False)
    target = Column(String, nullable=True)
    details = Column(Text, nullable=True)


def init_db():
    Base.metadata.create_all(engine)
