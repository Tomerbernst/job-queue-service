"""Job submission, retrieval, validation and listing."""

EMAIL_JOB = {
    "type": "email",
    "payload": {"to": "user@example.com", "subject": "hello", "body": "hi"},
}


async def test_submit_and_retrieve_job(api_client):
    resp = await api_client.post("/jobs", json=EMAIL_JOB)
    assert resp.status_code == 201
    job = resp.json()
    assert job["status"] == "pending"
    assert job["type"] == "email"
    assert job["attempts"] == 0
    assert job["created_at"] is not None

    resp = await api_client.get(f"/jobs/{job['id']}")
    assert resp.status_code == 200
    assert resp.json()["id"] == job["id"]


async def test_get_missing_job_404(api_client):
    resp = await api_client.get("/jobs/does-not-exist")
    assert resp.status_code == 404


async def test_unknown_job_type_rejected(api_client):
    resp = await api_client.post("/jobs", json={"type": "rm-rf", "payload": {}})
    assert resp.status_code == 422


async def test_invalid_payload_rejected(api_client):
    # bad email address
    resp = await api_client.post(
        "/jobs",
        json={"type": "email", "payload": {"to": "not-an-email", "subject": "x"}},
    )
    assert resp.status_code == 422

    # unexpected extra field (extra="forbid" — queue-poisoning defence)
    resp = await api_client.post(
        "/jobs",
        json={
            "type": "email",
            "payload": {"to": "a@b.co", "subject": "x", "__proto__": "evil"},
        },
    )
    assert resp.status_code == 422


async def test_submitting_enqueues_job(api_client, queue):
    assert await queue.depth() == 0
    await api_client.post("/jobs", json=EMAIL_JOB)
    assert await queue.depth() == 1


async def test_list_jobs_with_filters(api_client):
    await api_client.post("/jobs", json=EMAIL_JOB)
    await api_client.post("/jobs", json=EMAIL_JOB)
    await api_client.post(
        "/jobs",
        json={"type": "report", "payload": {"report_type": "sales"}},
    )

    resp = await api_client.get("/jobs")
    assert resp.json()["total"] == 3

    resp = await api_client.get("/jobs", params={"type": "email"})
    body = resp.json()
    assert body["total"] == 2
    assert all(j["type"] == "email" for j in body["jobs"])

    resp = await api_client.get("/jobs", params={"status": "pending", "type": "report"})
    assert resp.json()["total"] == 1

    resp = await api_client.get("/jobs", params={"status": "completed"})
    assert resp.json()["total"] == 0


async def test_health_endpoint(api_client, worker):
    resp = await api_client.get("/health")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "ok"
    assert body["database"] == "ok"
    assert body["redis"] == "ok"
    assert body["queue"]["depth"] == 0
    assert "jobs" in body
    # queue-age signals present (null when nothing is waiting/running)
    assert body["queue_age"]["oldest_pending_age_seconds"] is None
    assert body["queue_age"]["oldest_processing_age_seconds"] is None


async def test_health_reports_oldest_pending_age(api_client):
    await api_client.post("/jobs", json=EMAIL_JOB)
    body = (await api_client.get("/health")).json()
    assert body["jobs"]["pending"] == 1
    assert body["queue_age"]["oldest_pending_age_seconds"] >= 0


async def test_liveness_probe_is_cheap(api_client):
    resp = await api_client.get("/health/live")
    assert resp.status_code == 200
    assert resp.json() == {"status": "alive"}


async def test_dead_letter_inspection(api_client, worker, queue):
    from tests.conftest import drain_queue

    # a webhook that always fails, exhausted on the first attempt → dead-letter
    await api_client.post(
        "/jobs",
        json={"type": "webhook",
              "payload": {"url": "https://x.co/h", "event": "e", "fail_always": True},
              "max_attempts": 1},
    )
    await drain_queue(worker, queue)

    resp = await api_client.get("/dead-letter")
    assert resp.status_code == 200
    body = resp.json()
    assert body["count"] == 1
    assert body["entries"][0]["type"] == "webhook"


async def test_future_scheduled_job_is_not_enqueued(api_client, queue):
    resp = await api_client.post(
        "/jobs",
        json={**EMAIL_JOB, "scheduled_at": "2099-01-01T00:00:00Z"},
    )
    assert resp.json()["status"] == "scheduled"
    assert await queue.depth() == 0   # held back, not enqueued at submit time


async def test_webhook_rejects_internal_host(api_client):
    for bad in ("http://169.254.169.254/latest/meta-data/",
                "http://127.0.0.1/hook",
                "http://localhost/hook",
                "http://10.0.0.5/hook"):
        resp = await api_client.post(
            "/jobs", json={"type": "webhook", "payload": {"url": bad, "event": "e"}}
        )
        assert resp.status_code == 422, bad


async def test_health_degraded_when_db_unavailable(queue, settings):
    """If the database is unreachable, /health reports degraded rather than 500."""
    from httpx import ASGITransport, AsyncClient

    from app.api.main import create_app

    def broken_session_factory():
        raise RuntimeError("database down")

    app = create_app(
        settings=settings, session_factory=broken_session_factory,
        queue=queue, manage_resources=False,
    )
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        body = (await client.get("/health")).json()
    assert body["status"] == "degraded"
    assert body["database"] == "unavailable"
    assert body["redis"] == "ok"
