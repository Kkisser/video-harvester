from tests.test_integration import clean_database as clean_database

"""Regression cases reproduce the defects documented in the initial review."""
import hashlib
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from pathlib import Path
import httpx
import pytest
from tests.test_integration import (
    client,
    register,
    create_job,
    lease,
    agent_headers,
    ADMIN,
)
from coordinator import app as api, service, security
from coordinator.db import Session, Item, RangeChunk, Worker, utcnow
from agent.http import TargetHTTP, SourceError, retry_after_seconds
from agent.sources import discover
from agent.downloader import fetch_file, atomic_json, sha256_file, units
import m3u8


def payload(j, c, ident=7, status="not_found"):
    return dict(
        job_id=j,
        chunk_id=c["chunk_id"],
        lease_token=c["lease_token"],
        items=[dict(external_id=ident, status=status)],
    )


def post(name, p):
    return client.post(
        f"/api/agent/{name}/progress", headers=agent_headers(name), json=p
    )


def setup_one():
    register("worker-01")
    j = create_job("[7,9]", 3)
    return j, lease("worker-01", j)


def test_kinescope_actual_html_contract():
    html = (Path(__file__).parent / "fixtures/kinescope_public.html").read_bytes()
    h = TargetHTTP(
        test_source=True,
        client=httpx.Client(
            transport=httpx.MockTransport(lambda r: httpx.Response(200, content=html))
        ),
    )
    d = discover(h, "kinescope", "http://testserver", 200810246)
    assert (
        d["kind"] == "hls" and d["title"] and d["download_url"].endswith("master.m3u8")
    )


def test_non_owner_and_wrong_token_cannot_write():
    j, c = setup_one()
    register("worker-02")
    assert post("worker-02", payload(j, c)).status_code == 409
    p = payload(j, c)
    p["lease_token"] = "wrong-token-123456789"
    assert post("worker-01", p).status_code == 409
    with Session() as s:
        assert s.query(Item).count() == 0
    # Identity itself is bound to the per-worker token, before scheduler validation.
    r = client.post(
        "/api/agent/worker-02/lease",
        headers=agent_headers("worker-01"),
        json={"job_id": j},
    )
    assert r.status_code == 403


def test_expired_lease_same_owner_new_attempt_fenced():
    j, c = setup_one()
    with Session() as s:
        s.get(RangeChunk, c["chunk_id"]).lease_expires_at = utcnow() - timedelta(
            seconds=1
        )
        s.commit()
    service.reclaim_expired()
    new = lease("worker-01", j)
    assert new["lease_token"] != c["lease_token"]
    assert post("worker-01", payload(j, c)).status_code == 409
    assert post("worker-01", payload(j, new)).status_code == 200


def test_out_of_order_and_duplicate_checkpoint_never_regress():
    j, c = setup_one()
    assert post("worker-01", payload(j, c, 8)).json()["next_id"] == 7
    assert post("worker-01", payload(j, c, 7)).json()["next_id"] == 9
    p = payload(j, c, 7)
    p["next_id"] = 7
    assert post("worker-01", p).json()["next_id"] == 9
    # A replay cannot change a confirmed result.
    post("worker-01", payload(j, c, 7, "failed"))
    with Session() as s:
        assert s.query(Item).filter_by(external_id=7).one().status == "not_found"


def test_complete_rejects_unconfirmed_ids_and_is_idempotent():
    j, c = setup_one()
    assert not service.complete_chunk(c["chunk_id"], "worker-01", c["lease_token"], j)
    for n in range(7, 10):
        post("worker-01", payload(j, c, n))
    assert service.complete_chunk(c["chunk_id"], "worker-01", c["lease_token"], j)
    assert service.complete_chunk(c["chunk_id"], "worker-01", c["lease_token"], j)


def test_late_worker_gets_existing_job():
    register("worker-01")
    j = create_job("[1,10]", 2)
    register("worker-02")
    assert lease("worker-02", j)


def test_retry_completed_job_reactivates_and_skips_success():
    j, c = setup_one()
    for n in range(7, 10):
        post("worker-01", payload(j, c, n, "failed" if n == 8 else "not_found"))
    service.complete_chunk(c["chunk_id"], "worker-01", c["lease_token"], j)
    assert lease("worker-01", j) is None
    assert (
        client.post(f"/api/jobs/{j}/retry_failed", auth=ADMIN).json()["restarted"] == 1
    )
    new = lease("worker-01", j)
    assert new["next_id"] == 8 and 9 in new["skip_ids"]


def test_settings_reach_agent_and_global_threads_change():
    register("worker-01")
    r = client.post(
        "/api/jobs",
        auth=ADMIN,
        json={
            "name": "x",
            "range_text": "[1,3]",
            "threads": 17,
            "request_timeout": 123,
            "max_retries": 7,
        },
    )
    j = r.json()["job_id"]
    client.post(f"/api/jobs/{j}/start", auth=ADMIN)
    c = lease("worker-01", j)
    assert (c["threads"], c["request_timeout"], c["max_retries"]) == (17, 123, 7)
    client.patch(f"/api/jobs/{j}", auth=ADMIN, json={"threads": 9})
    hb = client.post(
        "/api/agent/worker-01/heartbeat",
        headers=agent_headers("worker-01"),
        json={"job_id": j, "cpu": 1, "disk_free": 100},
    )
    assert hb.json()["desired_threads"] == 9


def test_global_rate_grants_are_shared_and_not_preallocated(monkeypatch):
    register("worker-01")
    register("worker-02")
    j = create_job("[1,20]", 5)
    a = lease("worker-01", j)
    b = lease("worker-02", j)
    monkeypatch.setattr(service.time, "time", lambda: 1000.0)

    def ask(i):
        name, c = ("worker-01", a) if i % 2 else ("worker-02", b)
        return service.permit(name, {**payload(j, c), "kind": "check"})["granted"]

    with ThreadPoolExecutor(max_workers=10) as pool:
        assert sum(pool.map(ask, range(20))) == 1


def test_ranges_are_lazy_and_id_limit_is_exact():
    register("worker-01")
    r = client.post(
        "/api/jobs",
        auth=ADMIN,
        json={
            "name": "huge",
            "range_text": "[0,1000000000000]",
            "chunk_size": 100,
            "stop_after_ids": 3,
        },
    )
    j = r.json()["job_id"]
    with Session() as s:
        assert s.query(RangeChunk).count() == 0
    client.post(f"/api/jobs/{j}/start", auth=ADMIN)
    c = lease("worker-01", j)
    assert (c["range_start"], c["range_end"]) == (0, 2)


def test_stop_revokes_active_lease():
    j, c = setup_one()
    client.post(f"/api/jobs/{j}/stop", auth=ADMIN)
    assert post("worker-01", payload(j, c)).status_code == 409


def test_remote_path_and_registration_preserve_admin_config():
    assert not api._valid_worker_path(
        Worker(host="remote", storage_path="/data/videos"), "/data/videos/../../evil"
    )
    assert not api._valid_worker_path(
        Worker(host="remote", storage_path="/data/videos"), "relative/file"
    )
    register("worker-01")
    with Session() as s:
        w = s.query(Worker).filter_by(name="worker-01").one()
        w.host = "server.example"
        w.enabled = False
        s.commit()
    register("worker-01")
    with Session() as s:
        w = s.query(Worker).filter_by(name="worker-01").one()
        assert w.host == "server.example" and not w.enabled


def test_bad_range_is_422_and_empty_log_filters_work():
    assert (
        client.post(
            "/api/jobs", auth=ADMIN, json={"name": "x", "range_text": "[20,10]"}
        ).status_code
        == 422
    )
    client.cookies.set("session", security.make_session("admin"))
    assert client.get("/logs?job_id=&external_id=").status_code == 200


def target(handler):
    return TargetHTTP(
        test_source=True, client=httpx.Client(transport=httpx.MockTransport(handler))
    )


def seed_part(path, url, body, expected):
    part = Path(str(path) + ".part")
    part.write_bytes(body)
    identity = hashlib.sha256(f"{url}|0|None".encode()).hexdigest()
    atomic_json(
        Path(str(part) + ".json"),
        dict(
            identity=identity,
            size=len(body),
            checksum=sha256_file(part),
            expected_size=expected,
        ),
    )


def test_complete_part_promoted_without_http(tmp_path):
    url = "http://testserver/file"
    dest = tmp_path / "video.mp4"
    seed_part(dest, url, b"abc", 3)
    h = target(lambda r: pytest.fail("Should not request Range after complete partial"))
    result = fetch_file(
        h,
        url,
        dest,
        expected_size=3,
        expected_checksum=hashlib.sha256(b"abc").hexdigest(),
    )
    assert result["size"] == 3 and dest.read_bytes() == b"abc"


def test_partial_resume_and_corruption_recovery(tmp_path):
    url = "http://testserver/file"
    dest = tmp_path / "video.mp4"
    seed_part(dest, url, b"abc", 6)

    def handler(r):
        assert r.headers["range"] == "bytes=3-"
        return httpx.Response(
            206, content=b"def", headers={"Content-Range": "bytes 3-5/6"}
        )

    fetch_file(
        target(handler),
        url,
        dest,
        expected_size=6,
        expected_checksum=hashlib.sha256(b"abcdef").hexdigest(),
    )
    assert dest.read_bytes() == b"abcdef"
    dest.unlink()
    seed_part(dest, url, b"abc", 6)
    Path(str(dest) + ".part").write_bytes(b"BAD")

    def full(r):
        assert "range" not in r.headers
        return httpx.Response(200, content=b"abcdef")

    fetch_file(target(full), url, dest, expected_size=6)
    assert dest.read_bytes() == b"abcdef"


def test_retry_after_date_and_http_error():
    assert retry_after_seconds({"retry-after": "Sun, 06 Nov 1994 08:49:37 GMT"}) == 0
    with pytest.raises(SourceError) as error:
        target(lambda r: httpx.Response(500)).get("http://testserver/x")
    assert error.value.retryable


def test_hls_byterange_coalesces_and_encryption_rejected():
    p = m3u8.loads(
        '#EXTM3U\n#EXT-X-MAP:URI="f.mp4",BYTERANGE="4@0"\n#EXTINF:4,\n#EXT-X-BYTERANGE:6@4\nf.mp4\n#EXT-X-ENDLIST',
        uri="http://testserver/x.m3u8",
    )
    assert units(p) == [("http://testserver/f.mp4", 0, 10)]
    from agent.downloader import read_playlist

    h = target(
        lambda r: httpx.Response(
            200,
            text='#EXTM3U\n#EXT-X-KEY:METHOD=AES-128,URI="key"\n#EXTINF:4,\nx.ts\n#EXT-X-ENDLIST',
        )
    )
    with pytest.raises(SourceError, match="encrypted_hls"):
        read_playlist(h, "http://testserver/x.m3u8")


def test_bounded_range_requests_reassemble_exact_bytes(tmp_path):
    body = b"x" * (8 * 1024 * 1024 + 123)
    requests = []

    def handler(r):
        value = r.headers["range"]
        requests.append(value)
        a, b = map(int, value.removeprefix("bytes=").split("-"))
        return httpx.Response(
            206,
            content=body[a : b + 1],
            headers={"Content-Range": f"bytes {a}-{b}/{len(body)}"},
        )

    dest = tmp_path / "track.bin"
    fetch_file(target(handler), "http://testserver/media", dest, byte_length=len(body))
    assert dest.read_bytes() == body and len(requests) == 2


def test_disk_reserve_and_cancel_prevent_confirmation(tmp_path, monkeypatch):
    from agent.downloader import ensure_space
    from agent.http import Cancelled
    import shutil

    monkeypatch.setattr(
        shutil, "disk_usage", lambda p: shutil._ntuple_diskusage(100, 99, 1)
    )
    with pytest.raises(SourceError, match="disk_reserve"):
        ensure_space(tmp_path, 1)
    h = target(lambda r: httpx.Response(200, content=b"abc"))
    h.cancelled.set()
    with pytest.raises(Cancelled):
        h.get("http://testserver/file")


def test_progress_outside_chunk_is_rejected_before_write():
    j, c = setup_one()
    assert post("worker-01", payload(j, c, 999)).status_code == 409
    with Session() as s:
        assert s.query(Item).count() == 0


def test_migration_is_idempotent_and_preserves_confirmed_result():
    from coordinator.db import init_db, engine
    from sqlalchemy import inspect

    j, c = setup_one()
    post("worker-01", payload(j, c, 7))
    init_db()
    init_db()
    with Session() as s:
        assert (
            s.query(Item).filter_by(job_id=j, external_id=7).one().status == "not_found"
        )
    assert "ix_items_status" in {
        x["name"] for x in inspect(engine).get_indexes("items")
    }


def test_schema1_upgrade_preserves_old_items_and_reclaims_unfenced_lease(tmp_path):
    import sqlite3
    from sqlalchemy import create_engine, text
    from coordinator.db import Base
    from coordinator.migrate import upgrade

    database = tmp_path / "old.db"
    connection = sqlite3.connect(database)
    connection.executescript(
        (Path(__file__).parents[1] / "migrations/001_initial.sql").read_text()
    )
    connection.execute(
        "INSERT INTO jobs(id,name,range_start,range_end,created_at,updated_at) VALUES (1,'old',0,9,'2026-11-01','2026-11-01')"
    )
    connection.execute(
        "INSERT INTO range_chunks(id,job_id,range_start,range_end,next_id,status,created_at,updated_at) VALUES (1,1,0,9,2,'leased','2026-11-01','2026-11-01')"
    )
    connection.execute(
        "INSERT INTO items(job_id,external_id,status,updated_at) VALUES (1,1,'not_found','2026-11-01')"
    )
    connection.commit()
    connection.close()
    eng = create_engine("sqlite:///" + str(database))
    Base.metadata.create_all(eng)
    upgrade(eng)
    upgrade(eng)
    with eng.connect() as c:
        assert c.execute(text("SELECT status FROM items")).scalar() == "not_found"
        assert c.execute(text("SELECT status FROM range_chunks")).scalar() == "pending"
        assert c.execute(text("SELECT next_range_start FROM jobs")).scalar() == 10
        assert c.execute(text("PRAGMA user_version")).scalar() == 2
    eng.dispose()


@pytest.mark.parametrize(
    "options", ["[]", '{"playlist":{"bad":1}}', '{"playlist":[{"sources":[]}]}']
)
def test_malformed_player_metadata_is_classified(options):
    h = target(
        lambda r: httpx.Response(200, text="var playerOptions = " + options + ";")
    )
    with pytest.raises(SourceError):
        discover(h, "kinescope", "http://testserver", 1)
