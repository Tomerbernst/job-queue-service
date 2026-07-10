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
