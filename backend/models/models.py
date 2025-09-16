from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

# Minimal in-memory models for initial wiring; replace with SQLModel/SQLAlchemy if needed.

@dataclass
class Truck:
  id: int
  name: str
  capacity_opt: Optional[int] = None
  start_lat: Optional[float] = None
  start_lng: Optional[float] = None
  active: bool = True

@dataclass
class Job:
  id: int
  address: str
  lat: float
  lng: float
  service_minutes: float
  priority: int
  assigned_truck_opt: Optional[int] = None

@dataclass
class Route:
  id: int
  truck_id: int
  date: str

@dataclass
class RouteStop:
  id: int
  route_id: int
  order: int
  job_id: int
  est_arrival: Optional[datetime]
  est_departure: Optional[datetime]
  actual_arrival_opt: Optional[datetime] = None
  actual_departure_opt: Optional[datetime] = None

@dataclass
class DispatchState:
  truck_id: int
  date: str
  current_batch_index: int = 0
  last_sent: Optional[datetime] = None
