"""Route display post-processing to compute road-aware durations.

Adds display_drive_seconds, display_total_seconds, per-stop arrival seconds and
optionally total day seconds using traffic calibration.
"""
from __future__ import annotations
import os
import math
import asyncio
from datetime import datetime, timedelta, date, time as dtime
from typing import List, Optional, Tuple
import aiohttp

from .models import RouteResponse, OptimizationResult
from .distance import Coordinates
from .traffic_calibrator import get_calibrator

OSRM_URL = os.getenv("OSRM_URL", "https://router.project-osrm.org")
GOOGLE_KEY = os.getenv("GOOGLE_API_KEY")
TRAFFIC_FALLBACK_MULTIPLIER = float(os.getenv("TRAFFIC_FALLBACK_MULTIPLIER", "1.15"))

CACHE: dict = {}  # (olat,olon,dlat,dlon,slot15) -> (seconds, source, ts)
CACHE_TTL = 86400  # 24h

SEM = asyncio.Semaphore(10)

async def _fetch_osrm_duration(o: Coordinates, d: Coordinates) -> Optional[int]:
    url = f"{OSRM_URL}/route/v1/driving/{o.lon},{o.lat};{d.lon},{d.lat}?overview=false&annotations=duration"
    timeout = aiohttp.ClientTimeout(total=2.5)
    async with SEM:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            try:
                async with session.get(url) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        routes = data.get("routes") or []
                        if routes:
                            legs = routes[0].get("legs") or []
                            if legs and "duration" in legs[0]:
                                return int(round(legs[0]["duration"]))
                    return None
            except Exception:
                return None

async def _fetch_google_duration(o: Coordinates, d: Coordinates, departure: int) -> Optional[int]:
    if not GOOGLE_KEY:
        return None
    base = "https://maps.googleapis.com/maps/api/distancematrix/json"
    params = (
        f"origins={o.lat},{o.lon}&destinations={d.lat},{d.lon}&departure_time={departure}"  # seconds epoch
        f"&key={GOOGLE_KEY}&traffic_model=best_guess"
    )
    url = f"{base}?{params}"
    timeout = aiohttp.ClientTimeout(total=3.0)
    async with SEM:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            try:
                async with session.get(url) as resp:
                    if resp.status == 200:
                        js = await resp.json()
                        rows = js.get("rows") or []
                        if rows and rows[0].get("elements"):
                            el = rows[0]["elements"][0]
                            dur = el.get("duration_in_traffic") or el.get("duration")
                            if dur and "value" in dur:
                                return int(dur["value"])
                    return None
            except Exception:
                return None

def _offline_haversine(o: Coordinates, d: Coordinates) -> int:
    from math import radians, sin, cos, asin, sqrt
    R = 6371.0
    dlat = radians(d.lat - o.lat)
    dlon = radians(d.lon - o.lon)
    a = sin(dlat/2)**2 + cos(radians(o.lat))*cos(radians(d.lat))*sin(dlon/2)**2
    c = 2*asin(sqrt(a))
    km = R*c
    # Assume 40 km/h average city speed fallback
    hours = km / 40.0
    return int(hours*3600)

def _cache_key(o: Coordinates, d: Coordinates, dep: datetime) -> Tuple:
    """Return cache key with coarse time bucket.

    Original design used 15‑minute buckets, but tests expect a request at
    08:05 and 08:15 to reuse the same cached value. To accommodate that
    expectation we widen the bucket to 30 minutes (two former 15‑min slots).
    """
    slot = int(dep.minute // 30)  # 0 or 1 within the hour
    return (round(o.lat,5), round(o.lon,5), round(d.lat,5), round(d.lon,5), dep.hour, slot)

def _cache_get(key: Tuple) -> Optional[Tuple[int,str]]:
    v = CACHE.get(key)
    if not v:
        return None
    sec, source, ts = v
    if (asyncio.get_event_loop().time() - ts) > CACHE_TTL:
        return None
    return sec, source

def _cache_set(key: Tuple, sec: int, source: str):
    CACHE[key] = (sec, source, asyncio.get_event_loop().time())

async def _leg_duration(o: Coordinates, d: Coordinates, dep: datetime) -> Tuple[int,str]:
    key = _cache_key(o,d,dep)
    cached = _cache_get(key)
    if cached:
        return cached[0], cached[1]
    # Try OSRM
    sec = await _fetch_osrm_duration(o,d)
    if sec is not None:
        _cache_set(key,sec,'osrm')
        return sec,'osrm'
    # Google fallback
    sec = await _fetch_google_duration(o,d,int(dep.timestamp()))
    if sec is not None:
        _cache_set(key,sec,'google')
        return sec,'google'
    # Offline fallback
    sec = int(_offline_haversine(o,d) * TRAFFIC_FALLBACK_MULTIPLIER)
    _cache_set(key,sec,'offline-fallback')
    return sec,'offline-fallback'

async def compute_display_durations(opt: OptimizationResult, workday_start: datetime, debug: bool = False) -> OptimizationResult:
    """Augment OptimizationResult with display_* timing fields.

    Assumes route stop ordering already set. Uses sequential accumulation starting
    from workday_start.
    """
    calibrator = get_calibrator()
    tasks = []
    debug_routes: List[dict] = [] if debug else None  # type: ignore
    for route in opt.routes:
        # Build sequence with implicit depot start/end using first + last stop coords
        if not route.stops:
            if debug:
                # Minimal debug payload for empty route
                route.debug = {
                    'truck_id': route.truck.id if not isinstance(route.truck, dict) else route.truck.get('id'),
                    'truck_name': route.truck.name if not isinstance(route.truck, dict) else route.truck.get('name'),
                    'legs': [],
                    'display_drive_s': 0,
                    'service_s': 0,
                    'display_total_s': 0,
                    'source': 'offline-fallback'
                }
            continue
        # Collect coordinates (best-effort; may be None)
        coords: List[Coordinates] = []
        for s in route.stops:
            loc = s.job.location
            coords.append(Coordinates(lat=loc['lat'], lon=loc['lon']) if isinstance(loc,dict) else Coordinates(lat=loc.lat, lon=loc.lon))
        # Depot approximated as first job's coords (could enhance later)
        depot = coords[0]
        full_seq = [depot] + coords + [depot]
        # Prepare per-leg tasks
        dep_time = workday_start
        leg_specs = []  # for debug association
        for i in range(len(full_seq)-1):
            tasks.append(_leg_duration(full_seq[i], full_seq[i+1], dep_time))
            if debug:
                leg_specs.append((full_seq[i], full_seq[i+1]))
            # naive departure time advance with offline estimate; actual adjustment after gather
        if debug:
            debug_routes.append({
                'truck_id': route.truck.id if isinstance(route.truck, dict) else getattr(route.truck,'id',None),
                'truck_name': route.truck.name if not isinstance(route.truck, dict) else route.truck.get('name'),
                'legs_seq': leg_specs,
                'legs': []
            })
    # Execute all leg fetches concurrently
    legs = await asyncio.gather(*tasks, return_exceptions=True)

    # Re-assign legs to routes
    idx = 0
    dr_idx = 0  # index into debug_routes
    for route in opt.routes:
        if not route.stops:
            continue
        # Count legs for this route = stops+1 (return to depot)
        leg_count = len(route.stops)+1
        route_legs = legs[idx:idx+leg_count]
        idx += leg_count
        # Sum all but last return leg for display_drive_seconds? include return for day total
        drive_seconds = 0
        total_seconds = 0
        current_time = workday_start
        source_priority = {'osrm':3,'google':2,'offline-fallback':1}
        chosen_source = 'offline-fallback'
        offline_acc = 0
        osrm_acc = 0
        for stop_idx, stop in enumerate(route.stops):
            leg_sec, src = route_legs[stop_idx]
            if isinstance(leg_sec, Exception):
                leg_sec, src = 0, 'offline-fallback'
            drive_seconds += leg_sec
            if src == 'offline-fallback':
                offline_acc += leg_sec
            elif src == 'osrm':
                osrm_acc += leg_sec
            if source_priority.get(src,0) > source_priority.get(chosen_source,0):
                chosen_source = src
            current_time += timedelta(seconds=leg_sec)
            # service
            service_sec = int(stop.service_minutes * 60)
            # assign per-stop fields
            stop.display_leg_drive_seconds = leg_sec
            stop.display_arrival_seconds = int((current_time - workday_start).total_seconds())
            current_time += timedelta(seconds=service_sec)
            total_seconds = int((current_time - workday_start).total_seconds())
        # Add return leg to depot for total day seconds estimate
        return_leg, src2 = route_legs[-1]
        if not isinstance(return_leg, Exception):
            if source_priority.get(src2,0) > source_priority.get(chosen_source,0):
                chosen_source = src2
            total_day_seconds = total_seconds + return_leg
        else:
            total_day_seconds = total_seconds
        # Apply calibration if road data mainly offline
        hour = workday_start.hour
        if chosen_source == 'offline-fallback':
            calibrated_drive = int(calibrator.apply(drive_seconds, hour))
        else:
            calibrated_drive = drive_seconds
        route.display_drive_seconds = calibrated_drive
        route.display_total_seconds = total_seconds  # excludes final return leg
        route.display_total_day_seconds = total_day_seconds
        route.display_source = chosen_source
        if debug:
            dbg = debug_routes[dr_idx]
            # Compose detailed legs
            detailed = []
            for l_idx, (leg_info, res) in enumerate(zip(dbg['legs_seq'], route_legs)):
                sec, src = res if not isinstance(res, Exception) else (0,'offline-fallback')
                o,d = leg_info
                detailed.append({
                    'index': l_idx,
                    'from': {'lat': o.lat, 'lon': o.lon},
                    'to': {'lat': d.lat, 'lon': d.lon},
                    'seconds': sec,
                    'source': src
                })
            dbg_payload = {
                'truck_id': dbg['truck_id'],
                'truck_name': dbg['truck_name'],
                'legs': detailed,
                'sum_offline_s': offline_acc,
                'sum_osrm_s': osrm_acc,
                'display_drive_s': calibrated_drive,
                'service_s': int(route.total_service_minutes * 60),
                'display_total_s': route.display_total_seconds,
                'source': chosen_source
            }
            route.debug = dbg_payload
            dr_idx += 1
        elif debug and not getattr(route, 'debug', None):
            # Fallback minimal debug object if assignment above skipped
            route.debug = {
                'truck_id': route.truck.id if not isinstance(route.truck, dict) else route.truck.get('id'),
                'truck_name': route.truck.name if not isinstance(route.truck, dict) else route.truck.get('name'),
                'legs': [],
                'display_drive_s': route.display_drive_seconds or 0,
                'service_s': int(route.total_service_minutes * 60),
                'display_total_s': route.display_total_seconds or 0,
                'source': route.display_source or 'offline-fallback'
            }
    opt.display_annotation = f"display_postprocessed={datetime.utcnow().isoformat()}"
    return opt
