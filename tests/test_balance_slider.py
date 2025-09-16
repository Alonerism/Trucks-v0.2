from fastapi.testclient import TestClient
from app.api import create_app


def _optimize(client: TestClient, slider: float):
    payload = {
        "date": "2099-01-01",  # empty jobs seeds → service returns empty plan; we'll just test response shape
        "auto": "overtime",
        "single_truck_mode": False,
        "solver_strategy": "greedy",
        "balance_slider": slider,
    }
    return client.post("/optimize", json=payload)


def test_balance_slider_echo_and_ranges():
    app = create_app()
    with TestClient(app) as client:
        # s=1 => f=g=1
        r = _optimize(client, 1.0)
        assert r.status_code in (200, 409)
        data = r.json()
        # Both 200 result and 409 detail case carry objective_breakdown
        if r.status_code == 409:
            data = data["detail"]["overtime_plan"]
        ob = data.get("objective_breakdown", {})
        bal = ob.get("balance", {})
        assert bal.get("balance_slider") == 1.0
        assert 0.9 <= bal.get("f", 0) <= 1.1
        assert 0.9 <= bal.get("g", 0) <= 1.1

        # s=0 => f>>g
        r0 = _optimize(client, 0.0)
        assert r0.status_code in (200, 409)
        ob0 = (r0.json()["detail"]["overtime_plan"] if r0.status_code == 409 else r0.json()).get("objective_breakdown", {})
        b0 = ob0.get("balance", {})
        assert b0.get("balance_slider") == 0.0
        assert b0.get("f", 0) > b0.get("g", 0)

        # s=2 => g>>f
        r2 = _optimize(client, 2.0)
        assert r2.status_code in (200, 409)
        ob2 = (r2.json()["detail"]["overtime_plan"] if r2.status_code == 409 else r2.json()).get("objective_breakdown", {})
        b2 = ob2.get("balance", {})
        assert b2.get("balance_slider") == 2.0
        assert b2.get("g", 0) > b2.get("f", 0)
