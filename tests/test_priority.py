"""Priority ordering: higher-priority jobs dequeue first; FIFO within a priority."""


async def test_higher_priority_dequeued_first(api_client, queue):
    low = (await api_client.post(
        "/jobs",
        json={"type": "email", "payload": {"to": "a@b.co", "subject": "low"}, "priority": 1},
    )).json()["id"]
    high = (await api_client.post(
        "/jobs",
        json={"type": "email", "payload": {"to": "a@b.co", "subject": "high"}, "priority": 10},
    )).json()["id"]
    mid = (await api_client.post(
        "/jobs",
        json={"type": "email", "payload": {"to": "a@b.co", "subject": "mid"}, "priority": 5},
    )).json()["id"]

    order = [await queue.dequeue(timeout=0.1) for _ in range(3)]
    assert order == [high, mid, low]


async def test_fifo_within_same_priority(api_client, queue):
    ids = []
    for i in range(4):
        resp = await api_client.post(
            "/jobs",
            json={"type": "email", "payload": {"to": "a@b.co", "subject": str(i)}, "priority": 5},
        )
        ids.append(resp.json()["id"])

    order = [await queue.dequeue(timeout=0.1) for _ in range(4)]
    assert order == ids            # earliest-submitted first


async def test_priority_respected_end_to_end(api_client, worker, queue):
    """Drain with a single-slot worker and confirm completion order follows priority."""
    for prio in (1, 9, 3, 7):
        await api_client.post(
            "/jobs",
            json={"type": "email", "payload": {"to": "a@b.co", "subject": f"p{prio}"},
                  "priority": prio},
        )

    completed_order = []
    for _ in range(4):
        job_id = await queue.dequeue(timeout=0.1)
        await worker.process_job(job_id)
        job = (await api_client.get(f"/jobs/{job_id}")).json()
        completed_order.append(job["priority"])

    assert completed_order == [9, 7, 3, 1]
