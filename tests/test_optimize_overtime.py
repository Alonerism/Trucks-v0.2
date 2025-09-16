from datetime import datetime, timedelta
from fastapi.testclient import TestClient
from app.api import create_app


def today():
    return datetime.now().date().isoformat()


def test_overtime_fallback_structure_empty_day():
    app = create_app()
    with TestClient(app) as client:
        resp = client.post('/optimize', json={'date': today(), 'auto': 'overtime'})
        assert resp.status_code == 200
        data = resp.json()
        # Should include objective_breakdown.balance if slider default present (may be None)
        assert 'routes' in data
        assert 'deferred_jobs' in data
        assert isinstance(data.get('deferred_jobs'), list)
        # overtime_summary may be empty because no routes
        assert 'overtime_summary' in data


def test_overtime_fallback_applies_used_and_deferred():
    app = create_app()
    with TestClient(app) as client:
        d = today()
        # Create synthetic jobs likely to overflow base minutes: use many material items to inflate time
        # We'll add ~15 jobs quickly (service times default 15) to trigger overtime trimming
        for i in range(15):
            payload = {
                'location_name': f'Site {i}',
                'action': 'pickup',
                'items': 'rebar:10',
                'priority': 3 if i < 5 else 2 if i < 10 else 1,
                'date': d
            }
            r = client.post('/jobs/quick_add', json=payload)
            assert r.status_code == 200
        resp = client.post('/optimize', json={'date': d, 'auto': 'overtime'})
        assert resp.status_code == 200
        data = resp.json()
        # Expect overtime_summary present
        summary = data.get('overtime_summary') or []
        assert isinstance(summary, list)
        # At least one truck summary
        assert len(summary) >= 1
        # Each summary has required keys
        for s in summary:
            for k in ('truck_id', 'truck_name', 'total_minutes', 'overtime_minutes', 'overtime_minutes_used'):
                assert k in s
        # Deferred jobs list should not raise
        deferred = data.get('deferred_jobs') or []
        assert isinstance(deferred, list)
        for dj in deferred:
            assert {'id', 'priority', 'reason', 'suggested_date'} <= set(dj.keys())
        # If any route has overtime_minutes_used, it should be <= 60
        for s in summary:
            if s.get('overtime_minutes_used') is not None:
                assert s['overtime_minutes_used'] <= 60


def test_overtime_fallback_priority_respected_in_defer():
    app = create_app()
    with TestClient(app) as client:
        d = today()
        # Create high priority and low priority jobs; expect low priority more likely deferred
        high_ids = []
        low_ids = []
        for i in range(8):
            payload = {
                'location_name': f'HighP{i}',
                'action': 'pickup',
                'items': 'rebar:15',
                'priority': 0,  # highest
                'date': d
            }
            r = client.post('/jobs/quick_add', json=payload)
            high_ids.append(r.json()['stats']['jobs_created']) if r.status_code == 200 else None
        for i in range(12):
            payload = {
                'location_name': f'LowP{i}',
                'action': 'pickup',
                'items': 'rebar:15',
                'priority': 3,  # lowest
                'date': d
            }
            r = client.post('/jobs/quick_add', json=payload)
            low_ids.append(r.json()['stats']['jobs_created']) if r.status_code == 200 else None
        resp = client.post('/optimize', json={'date': d, 'auto': 'overtime'})
        assert resp.status_code == 200
        data = resp.json()
        deferred = data.get('deferred_jobs') or []
        # Heuristic: At least one deferred job should have priority 3 if any deferrals occurred
        if deferred:
            assert any(j['priority'] == 3 for j in deferred)
