from fastapi.testclient import TestClient
from backend.api.main import create_app


def test_health_and_basic_flow():
    app = create_app()
    c = TestClient(app)

    # health
    r = c.get("/health")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"

    # seed truck and jobs
    assert c.post("/trucks", json={"name": "T1"}).status_code == 200
    for _ in range(5):
        assert (
            c.post(
                "/jobs",
                json={
                    "address": "A",
                    "lat": 34.05,
                    "lng": -118.25,
                    "service_minutes": 5,
                    "priority": 1,
                },
            ).status_code
            == 200
        )

    date = "2025-09-16"
    assert c.post("/optimize", json={"date": date}).status_code == 200
    r2 = c.get(f"/routes/1", params={"date": date})
    assert r2.status_code == 200
    assert len(r2.json().get("stops", [])) <= 3
    assert c.post(f"/routes/1/done", params={"date": date}).status_code == 200
    assert c.post(f"/routes/1/reopt", params={"date": date}).status_code == 200
