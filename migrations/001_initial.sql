PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;

CREATE TABLE IF NOT EXISTS users (
  id INTEGER PRIMARY KEY,
  username VARCHAR NOT NULL UNIQUE,
  password_hash VARCHAR NOT NULL,
  created_at DATETIME NOT NULL
);

CREATE TABLE IF NOT EXISTS roles (
  id INTEGER PRIMARY KEY,
  name VARCHAR NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS user_roles (
  id INTEGER PRIMARY KEY,
  user_id INTEGER NOT NULL REFERENCES users(id),
  role_id INTEGER NOT NULL REFERENCES roles(id),
  CONSTRAINT uq_user_role UNIQUE (user_id, role_id)
);

CREATE TABLE IF NOT EXISTS workers (
  id INTEGER PRIMARY KEY,
  name VARCHAR NOT NULL UNIQUE,
  host VARCHAR NOT NULL DEFAULT 'local',
  ssh_port INTEGER NOT NULL DEFAULT 22,
  ssh_user VARCHAR DEFAULT '',
  ssh_key_path VARCHAR DEFAULT '',
  remote_path VARCHAR DEFAULT '',
  storage_path VARCHAR DEFAULT './data',
  min_free_space_gb FLOAT DEFAULT 0.1,
  enabled BOOLEAN NOT NULL DEFAULT 1,
  status VARCHAR NOT NULL DEFAULT 'offline',
  threads INTEGER NOT NULL DEFAULT 4,
  version VARCHAR DEFAULT '1.0',
  last_heartbeat DATETIME,
  cpu FLOAT DEFAULT 0,
  ram_used FLOAT DEFAULT 0,
  ram_total FLOAT DEFAULT 0,
  disk_free FLOAT DEFAULT 0,
  ids_per_second FLOAT DEFAULT 0,
  bytes_per_second FLOAT DEFAULT 0,
  active_checks INTEGER DEFAULT 0,
  active_downloads INTEGER DEFAULT 0,
  error_count INTEGER DEFAULT 0,
  created_at DATETIME NOT NULL
);

CREATE TABLE IF NOT EXISTS worker_metrics (
  id INTEGER PRIMARY KEY,
  worker_id INTEGER NOT NULL REFERENCES workers(id),
  ts DATETIME NOT NULL,
  cpu FLOAT DEFAULT 0,
  ram_used FLOAT DEFAULT 0,
  ram_total FLOAT DEFAULT 0,
  disk_free FLOAT DEFAULT 0,
  net_in FLOAT DEFAULT 0,
  net_out FLOAT DEFAULT 0,
  load_avg FLOAT DEFAULT 0,
  open_files INTEGER DEFAULT 0,
  active_connections INTEGER DEFAULT 0,
  uptime FLOAT DEFAULT 0,
  ids_per_second FLOAT DEFAULT 0,
  bytes_per_second FLOAT DEFAULT 0,
  active_checks INTEGER DEFAULT 0,
  active_downloads INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS jobs (
  id INTEGER PRIMARY KEY,
  name VARCHAR NOT NULL,
  range_start BIGINT NOT NULL DEFAULT 0,
  range_end BIGINT,
  chunk_size INTEGER NOT NULL DEFAULT 100,
  request_timeout INTEGER NOT NULL DEFAULT 10,
  max_retries INTEGER NOT NULL DEFAULT 4,
  global_rate_limit_rps FLOAT NOT NULL DEFAULT 20,
  stop_on_consecutive_missing INTEGER,
  stop_after_ids BIGINT,
  stop_after_seconds INTEGER,
  status VARCHAR NOT NULL DEFAULT 'created',
  created_at DATETIME NOT NULL,
  updated_at DATETIME NOT NULL,
  started_at DATETIME
);

CREATE TABLE IF NOT EXISTS job_workers (
  id INTEGER PRIMARY KEY,
  job_id INTEGER NOT NULL REFERENCES jobs(id),
  worker_id INTEGER NOT NULL REFERENCES workers(id),
  threads INTEGER,
  enabled BOOLEAN NOT NULL DEFAULT 1,
  CONSTRAINT uq_job_worker UNIQUE (job_id, worker_id)
);

CREATE TABLE IF NOT EXISTS range_chunks (
  id INTEGER PRIMARY KEY,
  job_id INTEGER NOT NULL REFERENCES jobs(id),
  range_start BIGINT NOT NULL,
  range_end BIGINT NOT NULL,
  next_id BIGINT NOT NULL,
  status VARCHAR NOT NULL DEFAULT 'pending',
  leased_by VARCHAR,
  leased_at DATETIME,
  lease_expires_at DATETIME,
  attempt INTEGER NOT NULL DEFAULT 0,
  created_at DATETIME NOT NULL,
  updated_at DATETIME NOT NULL,
  CONSTRAINT uq_range_chunk UNIQUE (job_id, range_start, range_end)
);

CREATE TABLE IF NOT EXISTS items (
  id INTEGER PRIMARY KEY,
  job_id INTEGER NOT NULL REFERENCES jobs(id),
  external_id BIGINT NOT NULL,
  status VARCHAR NOT NULL DEFAULT 'pending',
  http_status INTEGER,
  title VARCHAR,
  error_code VARCHAR,
  worker_name VARCHAR,
  updated_at DATETIME NOT NULL,
  CONSTRAINT uq_job_item UNIQUE (job_id, external_id)
);

CREATE TABLE IF NOT EXISTS media_metadata (
  id INTEGER PRIMARY KEY,
  item_id INTEGER NOT NULL UNIQUE REFERENCES items(id),
  title VARCHAR,
  source_url VARCHAR,
  download_url VARCHAR,
  mime VARCHAR,
  expected_size BIGINT,
  expected_checksum VARCHAR,
  discovered_at DATETIME NOT NULL
);

CREATE TABLE IF NOT EXISTS downloads (
  id INTEGER PRIMARY KEY,
  item_id INTEGER NOT NULL REFERENCES items(id),
  worker_name VARCHAR NOT NULL,
  status VARCHAR NOT NULL DEFAULT 'queued',
  attempt INTEGER NOT NULL DEFAULT 1,
  started_at DATETIME,
  finished_at DATETIME,
  error_code VARCHAR
);

CREATE TABLE IF NOT EXISTS files (
  id INTEGER PRIMARY KEY,
  item_id INTEGER NOT NULL UNIQUE REFERENCES items(id),
  download_id INTEGER NOT NULL REFERENCES downloads(id),
  storage_path VARCHAR NOT NULL,
  size BIGINT NOT NULL DEFAULT 0,
  mime VARCHAR,
  checksum VARCHAR NOT NULL,
  created_at DATETIME NOT NULL
);

CREATE TABLE IF NOT EXISTS events (
  id INTEGER PRIMARY KEY,
  ts DATETIME NOT NULL,
  level VARCHAR NOT NULL DEFAULT 'INFO',
  component VARCHAR NOT NULL,
  job_id INTEGER,
  worker_id VARCHAR,
  chunk_id INTEGER,
  external_id BIGINT,
  operation VARCHAR NOT NULL,
  result VARCHAR,
  error_code VARCHAR,
  message TEXT
);

CREATE TABLE IF NOT EXISTS audit_log (
  id INTEGER PRIMARY KEY,
  ts DATETIME NOT NULL,
  user VARCHAR NOT NULL,
  action VARCHAR NOT NULL,
  target VARCHAR,
  details TEXT
);

CREATE INDEX IF NOT EXISTS ix_chunks_status ON range_chunks(job_id, status);
CREATE INDEX IF NOT EXISTS ix_items_status ON items(job_id, status);
CREATE INDEX IF NOT EXISTS ix_events_search ON events(job_id, worker_id, external_id, ts);
PRAGMA user_version = 1;
