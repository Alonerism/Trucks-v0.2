from fastapi.testclient import TestClient
from app.api import create_app


def test_per_route_objective_breakdown_presence():
    app = create_app()
    with TestClient(app) as client:
        r = client.post('/optimize', json={'date': '2099-01-02', 'solver_strategy': 'greedy'})
        assert r.status_code in (200, 409)
        payload = r.json()
        # In 409 overtime decision case, objective data nested under overtime_plan
        if r.status_code == 409:
            payload = payload['detail']['overtime_plan']
        routes = payload.get('routes', [])
        for route in routes:
            # Each route should now carry objective_breakdown dict with core keys
            ob = route.get('objective_breakdown')
            assert isinstance(ob, dict)
            for k in ('drive_minutes','service_minutes','overtime_minutes','total_cost'):
                assert k in ob
                assert isinstance(ob[k], (int,float))