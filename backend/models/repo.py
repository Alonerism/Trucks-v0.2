from __future__ import annotations
from typing import Dict, List, Optional
from .models import Truck, Job, Route, RouteStop, DispatchState
from datetime import datetime

class Repo:
  """Minimal in-memory repository for quick rebuild.
  Replace with SQLModel/SQLAlchemy persistence later.
  """
  def __init__(self):
    self.trucks: Dict[int, Truck] = {}
    self.jobs: Dict[int, Job] = {}
    self.routes: Dict[int, Route] = {}
    self.route_stops: Dict[int, RouteStop] = {}
    self.dispatch: Dict[tuple[int,str], DispatchState] = {}
    self._ids = {"truck": 1, "job": 1, "route": 1, "stop": 1}

  # --- Trucks ---
  def list_trucks(self) -> List[Truck]: return list(self.trucks.values())
  def get_truck(self, tid: int) -> Optional[Truck]: return self.trucks.get(tid)
  def create_truck(self, name: str, capacity_opt: int | None = None) -> Truck:
    tid = self._ids["truck"]; self._ids["truck"] += 1
    t = Truck(id=tid, name=name, capacity_opt=capacity_opt)
    self.trucks[tid] = t
    return t
  def delete_truck(self, tid: int, cascade: bool = False) -> bool:
    if tid not in self.trucks: return False
    if cascade:
      # remove routes and their stops
      rids = [r.id for r in self.routes.values() if r.truck_id == tid]
      for rid in rids:
        self.delete_route(rid)
    else:
      if any(r.truck_id == tid for r in self.routes.values()):
        return False
    del self.trucks[tid]
    return True

  # --- Jobs ---
  def list_jobs(self) -> List[Job]: return list(self.jobs.values())
  def create_job(self, address: str, lat: float, lng: float, service_minutes: float, priority: int) -> Job:
    jid = self._ids["job"]; self._ids["job"] += 1
    j = Job(id=jid, address=address, lat=lat, lng=lng, service_minutes=service_minutes, priority=priority)
    self.jobs[jid] = j
    return j
  def delete_job(self, jid: int, cascade: bool = False) -> bool:
    if jid not in self.jobs: return False
    # Unassign from routes first if cascade
    if cascade:
      for s in list(self.route_stops.values()):
        if s.job_id == jid:
          self.delete_route_stop(s.id)
    else:
      if any(s.job_id == jid for s in self.route_stops.values()):
        return False
    del self.jobs[jid]
    return True

  # --- Routes ---
  def create_route(self, truck_id: int, date: str) -> Route:
    rid = self._ids["route"]; self._ids["route"] += 1
    r = Route(id=rid, truck_id=truck_id, date=date)
    self.routes[rid] = r
    return r
  def delete_route(self, rid: int) -> None:
    for s in [s for s in self.route_stops.values() if s.route_id == rid]:
      self.delete_route_stop(s.id)
    self.routes.pop(rid, None)
  def list_route_stops(self, rid: int) -> List[RouteStop]:
    return sorted([s for s in self.route_stops.values() if s.route_id == rid], key=lambda x: x.order)
  def add_route_stop(self, rid: int, order: int, job_id: int, est_arrival, est_departure) -> RouteStop:
    sid = self._ids["stop"]; self._ids["stop"] += 1
    s = RouteStop(id=sid, route_id=rid, order=order, job_id=job_id, est_arrival=est_arrival, est_departure=est_departure)
    self.route_stops[sid] = s
    return s
  def delete_route_stop(self, sid: int) -> None:
    self.route_stops.pop(sid, None)
  def delete_route_stops_from_order(self, rid: int, start_order: int) -> None:
    for s in list(self.route_stops.values()):
      if s.route_id == rid and s.order >= start_order:
        self.delete_route_stop(s.id)

  # --- Dispatch ---
  def get_dispatch_state(self, truck_id: int, date: str) -> DispatchState:
    key = (truck_id, date)
    if key not in self.dispatch:
      self.dispatch[key] = DispatchState(truck_id=truck_id, date=date, current_batch_index=0)
    return self.dispatch[key]
  def advance_batch(self, truck_id: int, date: str) -> int:
    st = self.get_dispatch_state(truck_id, date)
    st.current_batch_index += 1
    return st.current_batch_index

  # --- Helpers ---
  def now(self) -> datetime:
    return datetime.utcnow()
