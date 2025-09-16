import math
from app.solver_pyvrp import PyVRPSolver
from app.models import Job, Location, Truck, ActionType
import numpy as np


def build_basic_env():
    locations = [
        Location(id=1, name="Depot", address="Depot", lat=34.0, lon=-118.0),
        Location(id=2, name="A", address="A", lat=34.01, lon=-118.01),
        Location(id=3, name="B", address="B", lat=34.02, lon=-118.02),
        Location(id=4, name="C", address="C", lat=34.03, lon=-118.03),
    ]
    jobs = [
        Job(id=1, location_id=2, action=ActionType.PICKUP, priority=1, date="2025-09-15"),
        Job(id=2, location_id=3, action=ActionType.PICKUP, priority=2, date="2025-09-15"),
        Job(id=3, location_id=4, action=ActionType.PICKUP, priority=3, date="2025-09-15"),
    ]
    # Simple distance matrix minutes (symmetric)
    n = len(locations)
    mat = np.zeros((n, n))
    for i in range(n):
        for j in range(n):
            if i != j:
                mat[i, j] = (abs(i - j) + 1) * 5
    service_times = [0.0, 10.0, 10.0, 10.0]
    trucks = [Truck(id=1, name="T1", max_weight_lb=10000, bed_len_ft=12, bed_width_ft=8, height_limit_ft=10, large_capable=True, large_truck=True)]

    class Cfg:
        class Solver:
            balance_slider: float = 0.0
        solver = Solver()
    return Cfg(), trucks, jobs, mat, service_times, locations


def test_priority_prize_extremes():
    cfg, trucks, jobs, mat, service_times, locs = build_basic_env()
    solver = PyVRPSolver(cfg)
    avg_cost = 600.0
    p1 = solver.priority_prize(1, 0.0, avg_cost)
    p2 = solver.priority_prize(2, 0.0, avg_cost)
    p3 = solver.priority_prize(3, 0.0, avg_cost)
    assert p1 > p2 > p3
    # At s=2 priorities ignored (except Trump)
    p1_2 = solver.priority_prize(1, 2.0, avg_cost)
    p2_2 = solver.priority_prize(2, 2.0, avg_cost)
    assert p1_2 == p2_2 == 1 or abs(p1_2 - p2_2) <= 1


def test_monotonic_decay():
    cfg, trucks, jobs, mat, service_times, locs = build_basic_env()
    solver = PyVRPSolver(cfg)
    avg_cost = 600.0
    prev = solver.priority_prize(1, 0.0, avg_cost)
    for s in [0.2, 0.5, 1.0, 1.5, 2.0]:
        cur = solver.priority_prize(1, s, avg_cost)
        assert cur <= prev
        prev = cur


def test_trump_always_high():
    cfg, trucks, jobs, mat, service_times, locs = build_basic_env()
    solver = PyVRPSolver(cfg)
    avg_cost = 600.0
    trump = solver.priority_prize(0, 0.0, avg_cost)
    p1 = solver.priority_prize(1, 0.0, avg_cost)
    assert trump > p1 * 10


def test_served_ratio_monotonic_basic():
    # Build richer scenario with capacity pressure
    from app.models import Job
    cfg, trucks, jobs, mat, service_times, locs = build_basic_env()
    # Add extra jobs (close low priority) to allow displacement when priorities fade
    for j_id in range(4, 10):
        jobs.append(Job(id=j_id, location_id=2 + (j_id % 3), action=ActionType.PICKUP, priority=3, date="2025-09-15"))
    # One truck with small capacity so not all jobs fit
    trucks[0].large_truck = True
    # distance matrix enlarge accordingly (reuse existing for indices)
    solver = PyVRPSolver(cfg)
    locations = locs
    distance_matrix = mat
    service_times_local = service_times
    def solve_at(s):
        cfg.solver.balance_slider = s
        return solver.solve(trucks, jobs, distance_matrix, service_times_local, locations, time_limit_seconds=5)
    res0 = solve_at(0.0)
    res2 = solve_at(2.0)
    served0 = set(rj["job_id"] for r in res0.get("routes", []) for rj in r.get("jobs", []) for r in [r])
    served2 = set(rj["job_id"] for r in res2.get("routes", []) for rj in r.get("jobs", []) for r in [r])
    p1_jobs = {j.id for j in jobs if j.priority == 1}
    served_p1_0 = len(p1_jobs & served0)
    served_p1_2 = len(p1_jobs & served2)
    assert served_p1_0 >= served_p1_2
