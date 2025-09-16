from backend.core.solver_pyvrp import PyVRPSolver, SolveInput

def test_solver_returns_routes_and_matrix():
  coords = [(0,0),(0.01,0.01),(0.02,0.02)]
  req = SolveInput(coords=coords, service_minutes=[0,5,5], priorities=[1,1], trucks=[1])
  sol = PyVRPSolver().solve(req)
  assert sol.get("routes")
  assert sol.get("duration_matrix_seconds")
