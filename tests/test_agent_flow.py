import hashlib
import httpx
from agent.agent import Agent
from agent.http import TargetHTTP


def test_download_500_retried_with_full_attempt_history(tmp_path, monkeypatch):
    a = Agent("test", 1, "http://testserver", "x" * 32, str(tmp_path))
    a.state = lambda *args, **kwargs: None
    calls = []

    def handler(r):
        calls.append(r.url.path)
        if r.url.path == "/7":
            return httpx.Response(
                200,
                json={
                    "title": "fixture",
                    "download_url": "/file",
                    "size": 3,
                    "checksum": hashlib.sha256(b"abc").hexdigest(),
                },
            )
        if calls.count("/file") == 1:
            return httpx.Response(500)
        return httpx.Response(200, content=b"abc")

    h = TargetHTTP(
        test_source=True, client=httpx.Client(transport=httpx.MockTransport(handler))
    )
    h.sleep = lambda _: None
    a.test_target_url = "http://testserver"
    result = a.process_id(
        7, h, {"source": "mock", "max_retries": 2, "min_free_space_gb": 0}
    )
    assert result["status"] == "completed" and calls.count("/file") == 2
    assert [v["status"] for v in result["attempts"]] == ["retry_wait", "completed"]
    a.client.close()
    a.heartbeat_client.close()
    h.close()
