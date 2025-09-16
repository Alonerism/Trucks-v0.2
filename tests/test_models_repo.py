from backend.models.repo import Repo

def test_crud_and_cascade():
  repo = Repo()
  t = repo.create_truck("T1")
  j = repo.create_job("A", 1.0, 1.0, 5.0, 1)
  r = repo.create_route(t.id, "2025-09-16")
  s = repo.add_route_stop(r.id, 0, j.id, None, None)
  # Cannot delete job without cascade
  assert not repo.delete_job(j.id, cascade=False)
  # Cascade works
  assert repo.delete_job(j.id, cascade=True)
  # Cannot delete truck if route exists without cascade
  assert not repo.delete_truck(t.id, cascade=False)
  # Cascade delete truck removes routes
  assert repo.delete_truck(t.id, cascade=True)
