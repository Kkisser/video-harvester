import os
from datetime import datetime, timedelta

import pytest
from fastapi.testclient import TestClient

os.environ["CONFIG_PATH"] = "missing-test-config.yaml"
os.environ["DATABASE_URL"] = "sqlite:///integration_test.db"
os.environ["SECRET_KEY"] = "integration-secret-key-123456789"
os.environ["AGENT_TOKEN"] = "integration-agent-token-123456789"
os.environ["BOOTSTRAP_ADMIN_USERNAME"] = "admin"
os.environ["BOOTSTRAP_ADMIN_PASSWORD"] = "admin-password"
os.environ["BOOTSTRAP_OPERATOR_PASSWORD"] = "operator-password"
os.environ["BOOTSTRAP_VIEWER_PASSWORD"] = "viewer-password"

from coordinator import app as coordinator_app
from coordinator import security, service
from coordinator.db import Base, File, Item, Job, RangeChunk, Session, engine, init_db
from mock_target.server import app as mock_app, file_bytes


client = TestClient(coordinator_app.app)
mock_client = TestClient(mock_app)
ADMIN = ("admin", "admin-password")
VIEWER = ("viewer", "viewer-password")
AGENT_HEADERS = {"X-Agent-Token": os.environ["AGENT_TOKEN"]}


@pytest.fixture(autouse=True)
def clean_database():
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    security.ensure_bootstrap_users()
    coordinator_app.bootstrap_workers()
    yield


def register(name, threads=4):
    response = client.post(
        "/api/agent/register",
        headers=AGENT_HEADERS,
        json={"name": name, "host": "local", "threads": threads, "version": "test"},
    )
    assert response.status_code == 200
    heartbeat = client.post(
        f"/api/agent/{name}/heartbeat",
        headers=AGENT_HEADERS,
        json={"cpu": 1, "disk_free": 10},
    )
    assert heartbeat.status_code == 200


def create_job(range_text="[0,14]", chunk_size=5):
    response = client.post(
        "/api/jobs",
        auth=ADMIN,
        json={
            "name": "integration",
            "range_text": range_text,
            "chunk_size": chunk_size,
            "global_rate_limit_rps": 30,
        },
    )
    assert response.status_code == 200
    job_id = response.json()["job_id"]
    assert client.post(f"/api/jobs/{job_id}/start", auth=ADMIN).status_code == 200
    return job_id


def lease(name, job_id):
    response = client.post(
        f"/api/agent/{name}/lease",
        headers=AGENT_HEADERS,
        json={"job_id": job_id},
    )
    assert response.status_code == 200
    return response.json()["chunk"]


def test_user_api_requires_role():
    assert client.get("/api/jobs").status_code == 401
    assert client.post("/api/jobs", auth=VIEWER, json={"name": "x"}).status_code == 403


def test_three_agents_receive_different_chunks():
    for index in range(1, 4):
        register(f"worker-0{index}", threads=index + 2)
    job_id = create_job()
    chunks = [lease(f"worker-0{index}", job_id) for index in range(1, 4)]
    assert all(chunks)
    assert len({chunk["chunk_id"] for chunk in chunks}) == 3


def test_expired_lease_is_given_to_another_agent():
    register("worker-01")
    register("worker-02")
    job_id = create_job("[0,4]", 5)
    first = lease("worker-01", job_id)
    session = Session()
    chunk = session.get(RangeChunk, first["chunk_id"])
    chunk.lease_expires_at = datetime.utcnow() - timedelta(seconds=1)
    session.commit()
    session.close()
    assert first["chunk_id"] in service.reclaim_expired()
    second = lease("worker-02", job_id)
    assert second["chunk_id"] == first["chunk_id"]
    assert second["next_id"] == first["next_id"]


def test_repeated_progress_does_not_duplicate_item_or_file():
    register("worker-01")
    job_id = create_job("[7,7]", 1)
    chunk = lease("worker-01", job_id)
    payload = {
        "job_id": job_id,
        "chunk_id": chunk["chunk_id"],
        "next_id": 8,
        "items": [{
            "external_id": 7,
            "status": "completed",
            "http_status": 200,
            "title": "test",
            "download_url": "/download/7",
            "file": {"path": "data/worker-01/7/7/video.mp4", "size": 10,
                     "mime": "video/mp4", "checksum": "abc"},
        }],
    }
    for _ in range(2):
        response = client.post(
            "/api/agent/worker-01/progress", headers=AGENT_HEADERS, json=payload,
        )
        assert response.status_code == 200
    session = Session()
    assert session.query(Item).filter(Item.job_id == job_id, Item.external_id == 7).count() == 1
    assert session.query(File).count() == 1
    session.close()


def test_database_survives_reinitialization():
    register("worker-01")
    job_id = create_job("[0,0]", 1)
    init_db()
    session = Session()
    assert session.get(Job, job_id) is not None
    session.close()


def test_thread_change_arrives_in_heartbeat():
    register("worker-01", threads=4)
    response = client.patch(
        "/api/workers/worker-01", auth=ADMIN, json={"threads": 12},
    )
    assert response.status_code == 200
    heartbeat = client.post(
        "/api/agent/worker-01/heartbeat", headers=AGENT_HEADERS,
        json={"cpu": 1, "disk_free": 10},
    )
    assert heartbeat.json()["desired_threads"] == 12


def test_mock_statuses_and_range():
    assert mock_client.get("/7").status_code == 200
    assert mock_client.get("/8").status_code == 404
    assert mock_client.get("/70").status_code == 403
    assert mock_client.get("/91").status_code == 500
    response = mock_client.get("/download/7", headers={"Range": "bytes=100-"})
    assert response.status_code == 206
    assert response.content == file_bytes(7)[100:]
