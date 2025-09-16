from __future__ import annotations
from typing import Dict, Any
from datetime import datetime
from ..models.repo import Repo


class Dispatcher:
  def __init__(self, repo: Repo, batch_size: int = 3):
    self.repo = repo
    self.batch_size = batch_size

  def next_three(self, truck_id: int, date: str) -> Dict[str, Any]:
    st = self.repo.get_dispatch_state(truck_id, date)
    route = next((r for r in self.repo.routes.values() if r.truck_id == truck_id and r.date == date), None)
    if not route:
      return {"message": "no route"}
    stops = self.repo.list_route_stops(route.id)
    start = st.current_batch_index * self.batch_size
    chunk = stops[start:start + self.batch_size]
    st.last_sent = datetime.utcnow()
    url = f"https://maps.google.com/?q=truck{truck_id}-{date}-{st.current_batch_index}"
    return {
      "truck_id": truck_id,
      "date": date,
      "batch_index": st.current_batch_index,
      "maps_url": url,
      "stops": [
        {
          "job_id": s.job_id,
          "eta": s.est_arrival.isoformat() if s.est_arrival else None,
          "eta_depart": s.est_departure.isoformat() if s.est_departure else None,
        } for s in chunk
      ],
    }

  def done_and_next(self, truck_id: int, date: str) -> Dict[str, Any]:
    self.repo.advance_batch(truck_id, date)
    return self.next_three(truck_id, date)
