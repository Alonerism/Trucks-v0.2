from fastapi.testclient import TestClient
try:
  from backend.api.main import create_app
except Exception:  # fallback for transition period
  from backend.api.routes import create_app

def test_optimize_and_dispatch_flow():
  app = create_app()
  c = TestClient(app)
  # Seed truck and jobs
  c.post("/trucks", json={"name": "T1"})
  for i in range(5):
    c.post("/jobs", json={"address":"A","lat":34.05,"lng":-118.25,"service_minutes":5,"priority":1})
  date = "2025-09-16"
  r = c.post("/optimize", json={"date": date})
  assert r.status_code == 200
  # next three
  r2 = c.get(f"/routes/1", params={"date": date})
  assert r2.status_code == 200
  data = r2.json()
  assert len(data.get("stops", [])) <= 3
  # done should advance and still succeed
  r3 = c.post(f"/routes/1/done", params={"date": date})
  assert r3.status_code == 200
  # reopt should also succeed
  r4 = c.post(f"/routes/1/reopt", params={"date": date})
  assert r4.status_code == 200
