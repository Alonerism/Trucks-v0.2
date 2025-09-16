import pytest
from fastapi.testclient import TestClient
from app.api import create_app

@pytest.mark.parametrize("strategy",["pyvrp", None])
def test_two_single_stop_trucks_independent_drive_times(strategy):
    app = create_app()
    client = TestClient(app)

    # Seed two trucks already in config; create two jobs on different locations
    # Create day
    date = "2025-09-02"
    # Create first job
    j1 = {
        "date": date,
        "location": {"name": "LocA", "address": "111 A St, Los Angeles, CA", "lat": 34.05, "lon": -118.24},
        "action": "drop",
        "priority": 1,
        "items": []
    }
    j2 = {
        "date": date,
        "location": {"name": "LocB", "address": "222 B St, Los Angeles, CA", "lat": 34.055, "lon": -118.245},
        "action": "drop",
        "priority": 1,
        "items": []
    }
    for job in (j1,j2):
        resp = client.post("/jobs", json=job)
        assert resp.status_code == 201

    # Optimize with debug for visibility
    params = {"date": date, "debug": 1}
    if strategy:
        params["solver_strategy"] = strategy
    r = client.post("/optimize", json=params)
    assert r.status_code == 200
    data = r.json()
    assert data["solver_used"] == "pyvrp"
    routes = data["routes"]
    # Expect at least 2 routes (may contain empty) - filter those with stops
    active = [rt for rt in routes if rt.get("stops")]   
    assert len(active) >= 2
    # Each active route should have non-zero display_drive_seconds (after post-processing)
    # If display fields absent (edge), fall back to total_drive_minutes*60
    drives = []
    for rt in active[:2]:
        drv = rt.get("display_drive_seconds") or int(rt.get("total_drive_minutes",0)*60)
        drives.append(drv)
        assert drv > 0, f"Route drive time should be >0: {rt}"
        # Ensure debug payload present
        assert "debug" in rt and isinstance(rt["debug"], dict)
    # Drives should not be identical zeros; ensure independence
    assert not (drives[0] == 0 and drives[1] == 0)
