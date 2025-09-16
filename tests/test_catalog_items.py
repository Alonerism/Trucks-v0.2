from datetime import datetime
from fastapi.testclient import TestClient

from app.api import create_app


def _unique_name(prefix: str = "Test Item") -> str:
    return f"{prefix} {datetime.now().strftime('%Y%m%d%H%M%S%f')}"


def test_add_item_with_dims_and_path_returns_full_item_and_persists():
    app = create_app()
    client = TestClient(app)

    name = _unique_name()
    payload = {
        "name": name,
        "category": "material",
        "weight_lb_per_unit": 12.5,
        "requires_large_truck": False,
        "dims_lwh_ft": [1.5, 2.0, 0.75],
        "path": ["Catalog", "Concrete", "Bags"],
    }

    # Create
    resp = client.post("/catalog/items", json=payload)
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data.get("success") is True
    item = data.get("item")
    assert item and isinstance(item, dict)
    assert item.get("id")
    assert item.get("name") == name
    assert item.get("category") == "material"
    assert item.get("weight_lb_per_unit") == 12.5
    assert item.get("requires_large_truck") is False
    # Dims and path are properly serialized
    assert item.get("dims_lwh_ft") == [1.5, 2.0, 0.75]
    assert item.get("path") == ["Catalog", "Concrete", "Bags"]

    item_id = item["id"]

    # Fetch list and ensure it exists with same fields
    list_resp = client.get("/catalog/items")
    assert list_resp.status_code == 200, list_resp.text
    items = list_resp.json()
    found = next((i for i in items if i.get("id") == item_id), None)
    assert found is not None
    assert found.get("name") == name
    assert found.get("category") == "material"
    assert found.get("dims_lwh_ft") == [1.5, 2.0, 0.75]
    assert found.get("path") == ["Catalog", "Concrete", "Bags"]
