import json
from datetime import datetime, timedelta

from fastapi.testclient import TestClient

from app.api import create_app
from app.api import service as global_service


def _today_str():
    return datetime.now().date().isoformat()


def _yesterday_str():
    return (datetime.now().date() - timedelta(days=1)).isoformat()


def test_delete_nonexistent_truck_returns_404():
    app = create_app()
    with TestClient(app) as client:
        resp = client.delete("/catalog/trucks/999999")
        assert resp.status_code == 404
        assert resp.json()["detail"] == "Truck not found"


def test_delete_truck_with_active_assignment_returns_409():
    app = create_app()
    with TestClient(app) as client:
        # Create a truck via API
        create_resp = client.post(
            "/catalog/trucks",
            json={
                "name": f"Test Active Truck {_today_str()}",
                "max_weight_lb": 1000,
                "bed_len_ft": 8,
                "bed_width_ft": 5,
                "height_limit_ft": 8,
                "large_capable": False,
            },
        )
        assert create_resp.status_code == 200
        truck_id = create_resp.json()["truck"]["id"]

        # Create an active assignment (today)
        svc = global_service or None
        if svc is None:
            # Fallback: create a fresh service via app dependency
            from app.api import get_service
            svc = get_service()
        svc.repo.create_route_assignment(
            {
                "truck_id": truck_id,
                "date": _today_str(),
                "total_drive_minutes": 10.0,
                "total_service_minutes": 5.0,
                "total_weight_lb": 100.0,
                "overtime_minutes": 0.0,
            }
        )

        # Attempt delete → 409
        del_resp = client.delete(f"/catalog/trucks/{truck_id}")
    assert del_resp.status_code == 409
    assert del_resp.json()["detail"] == "Truck still referenced in routes"


def test_delete_truck_with_only_past_assignments_returns_204():
    app = create_app()
    with TestClient(app) as client:
        # Create truck
        create_resp = client.post(
            "/catalog/trucks",
            json={
                "name": f"Test Past Truck {_today_str()}-1",
                "max_weight_lb": 1000,
                "bed_len_ft": 8,
                "bed_width_ft": 5,
                "height_limit_ft": 8,
                "large_capable": False,
            },
        )
        assert create_resp.status_code == 200
        truck_id = create_resp.json()["truck"]["id"]

        # Add past assignment
        svc = global_service or None
        if svc is None:
            from app.api import get_service
            svc = get_service()
        svc.repo.create_route_assignment(
            {
                "truck_id": truck_id,
                "date": _yesterday_str(),
                "total_drive_minutes": 10.0,
                "total_service_minutes": 5.0,
                "total_weight_lb": 100.0,
                "overtime_minutes": 0.0,
            }
        )

        del_resp = client.delete(f"/catalog/trucks/{truck_id}")
        assert del_resp.status_code == 204

        # Verify it's gone
        trucks = client.get("/trucks").json()
        assert all(t["id"] != truck_id for t in trucks)


def test_force_delete_truck_with_past_assignments_returns_204():
    app = create_app()
    with TestClient(app) as client:
        # Create truck
        create_resp = client.post(
            "/catalog/trucks",
            json={
                "name": f"Test Past Truck Force {_today_str()}-2",
                "max_weight_lb": 1000,
                "bed_len_ft": 8,
                "bed_width_ft": 5,
                "height_limit_ft": 8,
                "large_capable": False,
            },
        )
        assert create_resp.status_code == 200
        truck_id = create_resp.json()["truck"]["id"]

        # Add multiple past assignments
        svc = global_service or None
        if svc is None:
            from app.api import get_service
            svc = get_service()
        for days in (2, 5, 10):
            svc.repo.create_route_assignment(
                {
                    "truck_id": truck_id,
                    "date": (datetime.now().date() - timedelta(days=days)).isoformat(),
                    "total_drive_minutes": 10.0,
                    "total_service_minutes": 5.0,
                    "total_weight_lb": 100.0,
                    "overtime_minutes": 0.0,
                }
            )

        del_resp = client.delete(f"/catalog/trucks/{truck_id}?force=true")
        assert del_resp.status_code == 204

        # Verify it's gone
        trucks = client.get("/trucks").json()
        assert all(t["id"] != truck_id for t in trucks)
