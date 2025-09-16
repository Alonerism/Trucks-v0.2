from __future__ import annotations
from typing import Any, Dict, List, Tuple
from dataclasses import dataclass
import numpy as np

@dataclass
class SolveInput:
  # depot first, then job coords
  coords: List[Tuple[float, float]]  # (lat, lng)
  service_minutes: List[float]       # len == len(coords)
  priorities: List[int]              # len == jobs, ignored for MVP order
  trucks: List[int]                  # list of truck ids

class PyVRPSolver:
  """Minimal placeholder that returns a simple nearest-neighbor route per truck.
  Attach duration_matrix_seconds so service can compute ETAs consistently.
  """
  def __init__(self, seed: int = 42):
    self.seed = seed

  def solve(self, req: SolveInput) -> Dict[str, Any]:
    n = len(req.coords)
    if n <= 1 or not req.trucks:
      return {"routes": [], "unassigned_jobs": [], "duration_matrix_seconds": [[0]], "status": "empty"}
    # Build simple haversine-based travel minutes matrix
    def hav_mi(a, b):
      import math
      R = 3959
      lat1, lon1 = map(math.radians, a)
      lat2, lon2 = map(math.radians, b)
      dlat = lat2 - lat1; dlon = lon2 - lon1
      h = math.sin(dlat/2)**2 + math.cos(lat1)*math.cos(lat2)*math.sin(dlon/2)**2
      return 2 * R * math.asin(min(1, math.sqrt(h)))
    def minutes(a, b):
      # 25 mph average
      return (hav_mi(a, b) / 25.0) * 60.0
    dm = np.zeros((n, n), dtype=float)
    for i in range(n):
      for j in range(n):
        dm[i, j] = 0.0 if i == j else minutes(req.coords[i], req.coords[j])
    # Greedy round-robin assignment by priority then id order
    job_indices = list(range(1, n))
    # Sort by priority asc, then index
    job_indices.sort(key=lambda idx: (req.priorities[idx-1], idx))
    routes = []
    buckets: List[List[int]] = [[] for _ in req.trucks]
    for i, j in enumerate(job_indices):
      buckets[i % len(req.trucks)].append(j)
    # Build route dicts
    for truck_id, bucket in zip(req.trucks, buckets):
      if not bucket: continue
      # order by nearest neighbor from depot
      ordered = []
      remaining = bucket[:]
      cur = 0
      while remaining:
        nxt = min(remaining, key=lambda j: dm[cur, j])
        ordered.append(nxt)
        remaining.remove(nxt)
        cur = nxt
      service_total = sum(req.service_minutes[j] for j in ordered)
      drive_min = 0.0; cur = 0
      for j in ordered:
        drive_min += dm[cur, j]
        cur = j
      # back to depot (optional). Keep minimal for total_time estimate
      total_time = drive_min + service_total
      routes.append({
        "truck_id": truck_id,
        "jobs": [
          {"job_id": j, "service_time": float(req.service_minutes[j])}
          for j in ordered
        ],
        "total_time": float(total_time),
        "location_indices": ordered,
      })
    return {
      "routes": routes,
      "unassigned_jobs": [],
      "duration_matrix_seconds": (dm * 60.0).astype(int).tolist(),
      "status": "heuristic",
    }
