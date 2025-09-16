from fastapi import APIRouter, HTTPException, FastAPI
from typing import Dict, Any

from ..models.repo import Repo
from ..core.service import Service

repo = Repo()
svc = Service(repo)

router = APIRouter()


@router.post("/optimize")
def optimize(payload: Dict[str, Any]) -> Dict[str, Any]:
    date = str(payload.get("date"))
    if not date:
        raise HTTPException(status_code=400, detail="date required")
    return svc.optimize(date)


@router.get("/routes/{truck_id}")
def next_three(truck_id: int, date: str) -> Dict[str, Any]:
    return svc.next_three(truck_id, date)


@router.post("/routes/{truck_id}/done")
def done(truck_id: int, date: str) -> Dict[str, Any]:
    return svc.done(truck_id, date)


@router.post("/routes/{truck_id}/reopt")
def reopt(truck_id: int, date: str) -> Dict[str, Any]:
    return svc.reopt(truck_id, date)


@router.get("/trucks")
def list_trucks():
    return [t.__dict__ for t in repo.list_trucks()]


@router.post("/trucks")
def add_truck(payload: Dict[str, Any]):
    t = repo.create_truck(name=payload.get("name", "Truck"))
    return t.__dict__


@router.delete("/trucks/{truck_id}")
def del_truck(truck_id: int, cascade: bool = False):
    ok = repo.delete_truck(truck_id, cascade=cascade)
    if not ok:
        raise HTTPException(status_code=409, detail="Truck still referenced in routes")
    return {"deleted": True}


@router.get("/jobs")
def list_jobs():
    return [j.__dict__ for j in repo.list_jobs()]


@router.post("/jobs")
def add_job(payload: Dict[str, Any]):
    j = repo.create_job(
        address=payload.get("address", "unknown"),
        lat=float(payload.get("lat", 0)),
        lng=float(payload.get("lng", 0)),
        service_minutes=float(payload.get("service_minutes", 10)),
        priority=int(payload.get("priority", 1)),
    )
    return j.__dict__


@router.delete("/jobs/{job_id}")
def del_job(job_id: int, cascade: bool = False):
    ok = repo.delete_job(job_id, cascade=cascade)
    if not ok:
        raise HTTPException(status_code=409, detail="Job still referenced in routes")
    return {"deleted": True}

def create_app() -> FastAPI:
    app = FastAPI(title="Fleet Optimizer")
    @app.get("/health")
    def health():
        return {"status": "ok"}
    app.include_router(router)
    return app

