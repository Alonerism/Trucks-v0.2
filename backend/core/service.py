from __future__ import annotations
from typing import Dict, List, Any
from dataclasses import dataclass
from datetime import datetime, timedelta

from ..models.repo import Repo
from ..models.models import Truck, Job
from .solver_pyvrp import PyVRPSolver, SolveInput
from .ml_calibration import EtaCalibrator
from ..config import load_settings

@dataclass
class Config:
  workday_start: str = "07:00"
  batch_size: int = 3

class Service:
  def __init__(self, repo: Repo, config: Config | None = None):
    self.repo = repo
    if config is None:
      settings = load_settings()
      wd = settings.get("workday", {}) if isinstance(settings, dict) else {}
      dispatch = settings.get("dispatch", {}) if isinstance(settings, dict) else {}
      self.config = Config(
        workday_start=str(wd.get("start", "07:00")),
        batch_size=int(dispatch.get("batch_size", 3)),
      )
    else:
      self.config = config
    ml_settings = (settings.get("ml_calibration", {}) if config is None else {})
    self.cal = EtaCalibrator(enabled=bool(ml_settings.get("enabled", True)))
    self.solver = PyVRPSolver()

  def optimize(self, date: str) -> Dict[str, Any]:
    trucks = self.repo.list_trucks()
    jobs = self.repo.list_jobs()
    if not trucks or not jobs:
      return {"date": date, "routes": []}
    # Build coords: depot as avg of starts if available; else first job as depot for MVP
    if any(t.start_lat and t.start_lng for t in trucks):
      dep_lat = sum([t.start_lat or 0 for t in trucks]) / max(1, len(trucks))
      dep_lng = sum([t.start_lng or 0 for t in trucks]) / max(1, len(trucks))
    else:
      dep_lat, dep_lng = jobs[0].lat, jobs[0].lng
    coords = [(dep_lat, dep_lng)] + [(j.lat, j.lng) for j in jobs]
    service_min = [0.0] + [j.service_minutes for j in jobs]
    priorities = [j.priority for j in jobs]
    trucks_ids = [t.id for t in trucks]

    sol = self.solver.solve(SolveInput(coords=coords, service_minutes=service_min, priorities=priorities, trucks=trucks_ids))

    # Persist minimal route + ETAs
    # Clear existing
    for r in list(self.repo.routes.values()):
      if r.date == date:
        self.repo.delete_route(r.id)
    for rd in sol.get("routes", []):
      rid = self.repo.create_route(truck_id=rd["truck_id"], date=date).id
      duration = sol.get("duration_matrix_seconds", [])
      # schedule from workday start
      start = datetime.fromisoformat(f"{date}T{self.config.workday_start}:00")
      cum = 0.0; prev = 0
      for idx, jinfo in enumerate(rd["jobs"]):
        li = rd["location_indices"][idx]
        drive_s = float(duration[prev][li]) if duration else 0.0
        cum += drive_s
        arr = start + timedelta(seconds=cum)
        svc_m = float(jinfo.get("service_time", 0.0))
        cum += svc_m * 60.0
        dep = start + timedelta(seconds=cum)
        self.repo.add_route_stop(rid, idx, jinfo["job_id"], arr, dep)
        prev = li
    return {"date": date, "routes": list(self.repo.routes.values())}

  def next_three(self, truck_id: int, date: str) -> Dict[str, Any]:
    from .dispatch import Dispatcher
    return Dispatcher(self.repo, self.config.batch_size).next_three(truck_id, date)

  def done(self, truck_id: int, date: str) -> Dict[str, Any]:
    from .dispatch import Dispatcher
    # collect calibrator observation from last batch if possible
    st = self.repo.get_dispatch_state(truck_id, date)
    route = next((r for r in self.repo.routes.values() if r.truck_id == truck_id and r.date == date), None)
    if route and st.current_batch_index >= 0:
      idx = max(0, st.current_batch_index)
      stops = self.repo.list_route_stops(route.id)
      start = idx * self.config.batch_size
      batch = stops[start:start + self.config.batch_size]
      if batch and st.last_sent is not None:
        planned_start = batch[0].est_arrival
        planned_end = batch[-1].est_departure
        if planned_start and planned_end:
          est_sec = (planned_end - planned_start).total_seconds()
          actual_sec = (datetime.utcnow() - st.last_sent).total_seconds()
          now = datetime.utcnow()
          self.cal.add(est_seconds=est_sec, actual_seconds=actual_sec, hour=now.hour, dow=now.weekday())
    return Dispatcher(self.repo, self.config.batch_size).done_and_next(truck_id, date)

  def reopt(self, truck_id: int, date: str) -> Dict[str, Any]:
    # Re-solve remaining for this truck starting now
    route = next((r for r in self.repo.routes.values() if r.truck_id == truck_id and r.date == date), None)
    if not route:
      return {"message": "no route"}
    stops = self.repo.list_route_stops(route.id)
    st = self.repo.get_dispatch_state(truck_id, date)
    start_order = st.current_batch_index * self.config.batch_size
    # Remaining jobs
    jobs = [self.repo.jobs[s.job_id] for s in stops if s.order >= start_order]
    if not jobs:
      return {"message": "no remaining"}
    # Use current position as depot (last completed or depot at first stop)
    now = datetime.utcnow()
    if start_order > 0:
      pos_job = self.repo.jobs[stops[start_order-1].job_id]
      dep = (pos_job.lat, pos_job.lng)
    else:
      first = self.repo.jobs[stops[0].job_id]
      dep = (first.lat, first.lng)
    coords = [dep] + [(j.lat, j.lng) for j in jobs]
    service_min = [0.0] + [j.service_minutes for j in jobs]
    priorities = [j.priority for j in jobs]
    sol = self.solver.solve(SolveInput(coords=coords, service_minutes=service_min, priorities=priorities, trucks=[truck_id]))
    # Delete future stops and insert new plan
    self.repo.delete_route_stops_from_order(route.id, start_order)
    duration = sol.get("duration_matrix_seconds", [])
    cum = 0.0; prev = 0
    for idx, jinfo in enumerate(sol["routes"][0]["jobs"]):
      li = sol["routes"][0]["location_indices"][idx]
      drive_s = float(duration[prev][li]) if duration else 0.0
      cum += drive_s
      arr = now + timedelta(seconds=cum)
      svc_m = float(jinfo.get("service_time", 0.0))
      cum += svc_m * 60.0
      dep_dt = now + timedelta(seconds=cum)
      self.repo.add_route_stop(route.id, start_order + idx, jinfo["job_id"], arr, dep_dt)
      prev = li
    return {"message": "reoptimized", "remaining": len(sol["routes"][0]["jobs"]) }
