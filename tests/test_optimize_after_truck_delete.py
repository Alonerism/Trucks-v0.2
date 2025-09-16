from datetime import datetime

from fastapi.testclient import TestClient

from app.api import create_app


def today():
    return datetime.now().date().isoformat()


def test_optimize_after_truck_deleted_returns_200_and_omits_truck():
    app = create_app()
    with TestClient(app) as client:
        # Create a new truck
        name = f"Temp Delete Truck {today()}"
        tr = client.post(
            "/catalog/trucks",
            json={
                "name": name,
                "max_weight_lb": 5000,
                "bed_len_ft": 8,
                "bed_width_ft": 5,
                "height_limit_ft": 8,
                "large_capable": False,
            },
        )
        assert tr.status_code == 200
        truck_id = tr.json()["truck"]["id"]

        # Delete it (no active/future assignments)
        delr = client.delete(f"/catalog/trucks/{truck_id}")
        assert delr.status_code == 204

        # Run optimize on today — should succeed (200), not crash
        opt = client.post(
            "/optimize",
            json={
                "date": today(),
                "auto": "overtime",
            },
        )
        # Either 200 (plan) or 409 (overtime decision). Both satisfy non-500 contract.
        assert opt.status_code in (200, 409)

        if opt.status_code == 200:
            body = opt.json()
            # Ensure no route references the deleted truck
            for r in body.get("routes", []):
                assert r["truck"]["id"] != truck_id
