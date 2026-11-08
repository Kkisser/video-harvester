"""Idempotent schema v2 migration for the supported SQLite deployment."""

from sqlalchemy import inspect, text

ADDITIONS = {
    "workers": {
        "rps": "FLOAT NOT NULL DEFAULT 20",
        "metrics_json": "TEXT NOT NULL DEFAULT '{}'",
        "ram_limit_percent": "FLOAT NOT NULL DEFAULT 95",
        "network_limit_mbps": "FLOAT NOT NULL DEFAULT 0",
        "disk_limit_mbps": "FLOAT NOT NULL DEFAULT 0",
    },
    "jobs": {
        "source": "VARCHAR NOT NULL DEFAULT 'mock'",
        "threads": "INTEGER NOT NULL DEFAULT 4",
        "max_connections": "INTEGER NOT NULL DEFAULT 8",
        "check_rate_rps": "FLOAT NOT NULL DEFAULT 10",
        "download_rate_rps": "FLOAT NOT NULL DEFAULT 10",
        "auto_assign": "BOOLEAN NOT NULL DEFAULT 1",
        "next_range_start": "BIGINT",
        "no_worker_timeout": "INTEGER NOT NULL DEFAULT 120",
        "status_reason": "VARCHAR NOT NULL DEFAULT ''",
        "adaptive_factor": "FLOAT NOT NULL DEFAULT 1",
        "recent_good": "INTEGER NOT NULL DEFAULT 0",
        "recent_bad": "INTEGER NOT NULL DEFAULT 0",
        "finished_at": "DATETIME",
    },
    "range_chunks": {"lease_token": "VARCHAR"},
    "downloads": {"lease_token": "VARCHAR NOT NULL DEFAULT 'legacy'"},
}


def upgrade(engine):
    if engine.dialect.name != "sqlite":
        raise RuntimeError(
            "This deployment supports SQLite; use a dedicated migration for another DBMS"
        )
    with engine.begin() as conn:
        for table, fields in ADDITIONS.items():
            existing = {c["name"] for c in inspect(conn).get_columns(table)}
            for name, sql_type in fields.items():
                if name not in existing:
                    conn.execute(
                        text(f"ALTER TABLE {table} ADD COLUMN {name} {sql_type}")
                    )
        conn.execute(
            text(
                "UPDATE jobs SET next_range_start = COALESCE((SELECT MAX(range_end)+1 FROM range_chunks WHERE job_id=jobs.id),range_start) WHERE next_range_start IS NULL"
            )
        )
        conn.execute(
            text(
                "UPDATE range_chunks SET status='pending', leased_by=NULL, lease_expires_at=NULL WHERE status='leased' AND lease_token IS NULL"
            )
        )
        for sql in [
            "CREATE INDEX IF NOT EXISTS ix_chunks_status ON range_chunks(job_id,status)",
            "CREATE INDEX IF NOT EXISTS ix_items_status ON items(job_id,status)",
            "CREATE INDEX IF NOT EXISTS ix_events_search ON events(job_id,worker_id,external_id,ts)",
            "CREATE INDEX IF NOT EXISTS ix_download_attempt ON downloads(item_id,lease_token,attempt)",
            "CREATE INDEX IF NOT EXISTS ix_worker_metrics ON worker_metrics(worker_id,ts)",
        ]:
            conn.execute(text(sql))
        conn.execute(text("PRAGMA user_version=2"))
