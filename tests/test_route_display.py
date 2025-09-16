import pytest, asyncio
from datetime import datetime
from app.models import RouteResponse, RouteStopResponse, TruckResponse, JobResponse, OptimizationResult
from app.route_display import compute_display_durations, CACHE
from app.traffic_calibrator import get_calibrator

class DummyLoc(dict):
    @property
    def lat(self):
        return self['lat']
    @property
    def lon(self):
        return self['lon']

@pytest.mark.asyncio
async def test_compute_display_offline_fallback(monkeypatch):
    # Build minimal route with one stop
    truck = TruckResponse(id=1,name="T1",max_weight_lb=10000,bed_len_ft=10,bed_width_ft=5,height_limit_ft=None,large_capable=True,large_truck=False)
    job = JobResponse(
        id=1,
        location={"id":1,"name":"A","address":"A","lat":34.0,"lon":-118.0,"window_start":None,"window_end":None},
        action="drop",priority=1,earliest=None,latest=None,notes=None,items=[]
    )
    stop = RouteStopResponse(job=job,stop_order=0,position=0,estimated_arrival=datetime.utcnow(),service_start=datetime.utcnow(),estimated_departure=datetime.utcnow(),drive_minutes_from_previous=10.0,service_minutes=5.0)
    route = RouteResponse(truck=truck,date="2025-01-01",stops=[stop],total_drive_minutes=10.0,total_service_minutes=5.0,total_weight_lb=0.0,overtime_minutes=0.0,maps_url="")
    opt = OptimizationResult(date="2025-01-01",routes=[route],unassigned_jobs=[],total_cost=0.0,solver_used="greedy",computation_time_seconds=0.1)

    # Force offline fallback by monkeypatching leg fetchers to None
    async def no_osrm(*a,**k): return None
    async def no_google(*a,**k): return None
    from app import route_display
    monkeypatch.setattr(route_display, '_fetch_osrm_duration', no_osrm)
    monkeypatch.setattr(route_display, '_fetch_google_duration', no_google)

    workday = datetime.fromisoformat("2025-01-01T07:00:00")
    opt2 = await compute_display_durations(opt, workday)
    r = opt2.routes[0]
    assert r.display_drive_seconds is not None
    assert r.display_source == 'offline-fallback'
    assert r.stops[0].display_arrival_seconds is not None

@pytest.mark.asyncio
async def test_cache_15min_bucket(monkeypatch):
    from app import route_display
    calls = {'cnt':0}
    async def fake_osrm(o,d):
        calls['cnt'] += 1
        return 100
    monkeypatch.setattr(route_display,'_fetch_osrm_duration',fake_osrm)
    async def no_google(*a,**k): return None
    monkeypatch.setattr(route_display,'_fetch_google_duration',no_google)

    # Two legs identical within same 15m bucket should cache
    from app.distance import Coordinates
    from datetime import datetime
    base = datetime.fromisoformat("2025-01-01T08:05:00")
    c1 = Coordinates(lat=34.0,lon=-118.0); c2=Coordinates(lat=34.01,lon=-118.02)
    await route_display._leg_duration(c1,c2,base)
    await route_display._leg_duration(c1,c2,base.replace(minute=base.minute+10))
    assert calls['cnt'] == 1

def test_calibrator_learning():
    calib = get_calibrator()
    before = calib.factor(8)
    calib.ingest(100, 200, 8)
    after = calib.factor(8)
    assert after > before
