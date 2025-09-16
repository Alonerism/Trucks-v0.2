from datetime import datetime

from fastapi.testclient import TestClient

from app.api import create_app


def today():
    return datetime.now().date().isoformat()


def _assert_str_or_none(val):
    assert val is None or isinstance(val, str), f"Expected string or None, got {type(val)}"


def test_optimize_datetimes_are_json_strings():
    app = create_app()
    with TestClient(app) as client:
        resp = client.post("/optimize", json={"date": today(), "auto": "overtime", "solver_strategy": "pyvrp"})
        # Either 200 with plan or 409 with decision; both should be JSON-safe
        assert resp.status_code in (200, 409)
        data = resp.json()

        if resp.status_code == 409:
            # Decision payload
            for key in ("overtime_plan", "defer_plan"):
                plan = data["detail"][key]
                for route in plan.get("routes", []):
                    for stop in route.get("stops", []):
                        _assert_str_or_none(stop.get("estimated_arrival"))
                        _assert_str_or_none(stop.get("service_start"))
                        _assert_str_or_none(stop.get("estimated_departure"))
        else:
            # Direct plan
            for route in data.get("routes", []):
                for stop in route.get("stops", []):
                    _assert_str_or_none(stop.get("estimated_arrival"))
                    _assert_str_or_none(stop.get("service_start"))
                    _assert_str_or_none(stop.get("estimated_departure"))
