"""
FastAPI application for truck route optimization.
Provides REST API endpoints for import, optimization, and route visualization.
"""

import logging
import os
from datetime import datetime
from typing import List, Dict, Any, Optional

from fastapi import FastAPI, HTTPException, BackgroundTasks, Depends, Response, Request
from fastapi.encoders import jsonable_encoder
from fastapi.middleware.cors import CORSMiddleware
from sqlmodel import select

from .service import TruckOptimizerService
from .schemas import (
    ImportRequest, ImportStatsResponse, OptimizeRequest,
    ConfigUpdateRequest, HealthResponse, KPIResponse
)
from .models import OptimizationResult, DispatchMessage
from .messaging import get_whatsapp_client


logger = logging.getLogger(__name__)

# Global service instance
service: TruckOptimizerService = None


def get_service() -> TruckOptimizerService:
    """Dependency to get service instance."""
    global service
    if service is None:
        service = TruckOptimizerService()
    return service


def create_app() -> FastAPI:
    """Create and configure FastAPI application."""
    app = FastAPI(
        title="Truck Route Optimizer",
        description="Concrete truck route optimization with live traffic and priority handling",
        version="0.1.0",
        docs_url="/docs",
        redoc_url="/redoc"
    )
    
    # Add CORS middleware
    app.add_middleware(
        CORSMiddleware,
        allow_origins=[
            "http://localhost:5173",
            "http://127.0.0.1:5173",
            "http://localhost:5174",
            "http://127.0.0.1:5174",
            "http://localhost:5175",
            "http://127.0.0.1:5175",
            "http://localhost:5176",
            "http://127.0.0.1:5176",
            "http://localhost:8000",
            "http://127.0.0.1:8000",
        ],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    
    @app.on_event("startup")
    async def startup_event():
        """Initialize application on startup."""
        global service
        service = TruckOptimizerService()
        logger.info("Truck Optimizer API started")
    
    @app.on_event("shutdown")
    async def shutdown_event():
        """Clean up on shutdown."""
        if service:
            await service.close()
        logger.info("Truck Optimizer API stopped")
    
    @app.get("/health", response_model=HealthResponse)
    async def health_check(svc: TruckOptimizerService = Depends(get_service)):
        """Health check endpoint."""
        health_data = svc.health_check()
        return HealthResponse(
            status=health_data["status"],
            version="0.1.0",
            database_connected=health_data["database_connected"],
            google_api_configured=health_data["google_api_configured"],
            timestamp=health_data["timestamp"]
        )
    
    @app.post("/import", response_model=ImportStatsResponse)
    async def import_jobs(
        request: ImportRequest,
        svc: TruckOptimizerService = Depends(get_service)
    ):
        """
        Import jobs from CSV or JSON data.
        Upserts locations, items, and jobs; geocodes unknown addresses.
        """
        try:
            stats = await svc.import_jobs(request)
            return stats
        except Exception as e:
            logger.error(f"Import failed: {e}")
            raise HTTPException(status_code=400, detail=str(e))
    
    @app.post("/optimize")
    async def optimize_routes(
        request: OptimizeRequest,
        svc: TruckOptimizerService = Depends(get_service),
        req: Request = None
    ):
        """Run optimization with automatic fallback: base hours -> +1h overtime per truck -> defer remaining.

        Always returns 200 with structure:
        {
          date, routes:[{..., overtime_minutes_used}], deferred_jobs:[{id,priority,reason,suggested_date}],
          objective_breakdown: { balance: { balance_slider, f, g }, ... }
        }
        On bad input returns 400/422; unexpected failure 500.
        """
        try:
            # Detect debug flag via query (?debug=1) or header X-Debug: 1
            debug_flag = bool(getattr(request, 'debug', False))
            try:
                if req:
                    if req.query_params.get('debug') == '1':
                        debug_flag = True
                    if req.headers.get('X-Debug') == '1':
                        debug_flag = True
            except Exception:
                pass
            # Attach transient attribute to request for downstream service
            setattr(request, '_debug_flag', debug_flag)
            result = await svc.optimize_routes(request)
            return jsonable_encoder(result)
        except HTTPException:
            raise
        except ValueError as ve:
            raise HTTPException(status_code=400, detail=str(ve))
        except Exception as e:
            logger.error(f"Optimization failed: {e}")
            # Return minimal failure-style payload with 200 per contract comment
            try:
                return {
                    "date": request.date,
                    "routes": [],
                    "unassigned_jobs": [],
                    "deferred_jobs": [],
                    "objective_breakdown": {},
                    "solver_used": "pyvrp",
                    "status": "failed",
                    "error": str(e)
                }
            except Exception:
                raise HTTPException(status_code=500, detail=str(e))
    
    @app.get("/routes/{date}")
    async def get_routes(
        date: str,
        svc: TruckOptimizerService = Depends(get_service)
    ):
        """Get optimized routes for a specific date."""
        try:
            # This would load saved routes from database
            # For now, return a placeholder
            route_assignments = svc.repo.get_route_assignments_by_date(date)
            
            if not route_assignments:
                raise HTTPException(
                    status_code=404, 
                    detail=f"No routes found for date {date}"
                )
            
            # Convert to API response format
            # This is simplified - full implementation would reconstruct complete route data
            routes = []
            for assignment in route_assignments:
                routes.append({
                    "truck_id": assignment.truck_id,
                    "truck_name": assignment.truck.name,
                    "total_drive_minutes": assignment.total_drive_minutes,
                    "total_service_minutes": assignment.total_service_minutes,
                    "total_weight_lb": assignment.total_weight_lb,
                    "overtime_minutes": assignment.overtime_minutes,
                    "stops": []  # Would load route stops
                })
            
            unassigned_jobs = svc.repo.get_unassigned_jobs_by_date(date)
            
            return {
                "date": date,
                "routes": routes,
                "unassigned_jobs": [{"job_id": uj.job_id, "reason": uj.reason} for uj in unassigned_jobs],
                "total_routes": len(routes),
                "total_unassigned": len(unassigned_jobs)
            }
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Failed to get routes: {e}")
            raise HTTPException(status_code=500, detail=str(e))
    
    @app.get("/links/{date}")
    async def get_route_links(
        date: str,
        svc: TruckOptimizerService = Depends(get_service)
    ):
        """Get Google Maps URLs for routes on a specific date."""
        try:
            urls = await svc.get_route_urls(date)
            
            if not urls:
                raise HTTPException(
                    status_code=404,
                    detail=f"No route URLs found for date {date}"
                )
            
            return {
                "date": date,
                "truck_routes": urls
            }
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Failed to get route URLs: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    # --- Three-at-a-time dispatch endpoints ---
    @app.get("/routes/{truck_id}")
    async def get_next_three(
        truck_id: int,
        date: Optional[str] = None,
        svc: TruckOptimizerService = Depends(get_service),
    ):
        """Return the next up to 3 planned stops for a truck for the given date (default today)."""
        try:
            date = date or datetime.utcnow().date().isoformat()
            # Use DB-planned route ordering
            assignments = svc.repo.get_route_assignments_by_date(date)
            ra = next((a for a in assignments if a.truck_id == truck_id), None)
            if not ra:
                raise HTTPException(status_code=404, detail="No planned route for this truck/date")
            stops = svc.repo.get_route_stops_by_assignment(ra.id)
            stops = sorted(stops, key=lambda s: s.stop_order)
            # Find current dispatch index for this driver (assuming driver==truck for now)
            state = svc.repo.get_or_create_dispatch_state(truck_id, date)
            batch_idx = state.current_batch_index
            # Compute slice of 3 from ordered stops
            start = batch_idx * 3
            next_stops = stops[start:start+3]
            return {
                "truck_id": truck_id,
                "date": date,
                "batch_index": batch_idx,
                "count": len(next_stops),
                "stops": [
                    {
                        "job_id": s.job_id,
                        "order": s.stop_order,
                        "eta": s.estimated_arrival.isoformat() if s.estimated_arrival else None,
                        "eta_depart": s.estimated_departure.isoformat() if s.estimated_departure else None,
                        "drive_min_from_prev": s.drive_minutes_from_previous,
                        "service_min": s.service_minutes,
                    }
                    for s in next_stops
                ],
            }
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"get_next_three failed: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    @app.post("/routes/{truck_id}/done")
    async def mark_three_done(
        truck_id: int,
        date: Optional[str] = None,
        svc: TruckOptimizerService = Depends(get_service),
    ):
        """Advance to the next batch of three for a truck."""
        try:
            date = date or datetime.utcnow().date().isoformat()
            # Increment dispatch batch index
            state = svc.repo.get_or_create_dispatch_state(truck_id, date)
            next_idx = state.current_batch_index + 1
            svc.repo.set_dispatch_batch_index(truck_id, date, next_idx)
            return {"truck_id": truck_id, "date": date, "batch_index": next_idx}
        except Exception as e:
            logger.error(f"mark_three_done failed: {e}")
            raise HTTPException(status_code=500, detail=str(e))
    
    @app.get("/config")
    async def get_config(svc: TruckOptimizerService = Depends(get_service)):
        """Get current configuration (non-secret parameters)."""
        try:
            config = svc.get_config()
            
            # Remove sensitive information
            if "google" in config and "api_key" in config["google"]:
                del config["google"]["api_key"]
            
            return config
            
        except Exception as e:
            logger.error(f"Failed to get config: {e}")
            raise HTTPException(status_code=500, detail=str(e))
    
    @app.put("/config")
    async def update_config(
        request: ConfigUpdateRequest,
        svc: TruckOptimizerService = Depends(get_service)
    ):
        """Update configuration parameters."""
        try:
            svc.update_config(request.updates)
            return {"message": "Configuration updated successfully"}
            
        except NotImplementedError:
            raise HTTPException(
                status_code=501,
                detail="Configuration updates not yet implemented"
            )
        except Exception as e:
            logger.error(f"Failed to update config: {e}")
            raise HTTPException(status_code=400, detail=str(e))
    
    @app.get("/trucks")
    async def get_trucks(svc: TruckOptimizerService = Depends(get_service)):
        """Get all available trucks."""
        try:
            trucks = svc.repo.get_trucks()
            return [
                {
                    "id": truck.id,
                    "name": truck.name,
                    "max_weight_lb": truck.max_weight_lb,
                    "bed_len_ft": truck.bed_len_ft,
                    "bed_width_ft": truck.bed_width_ft,
                    "height_limit_ft": truck.height_limit_ft,
                    "large_capable": truck.large_capable
                }
                for truck in trucks
            ]
        except Exception as e:
            logger.error(f"Failed to get trucks: {e}")
            raise HTTPException(status_code=500, detail=str(e))
    
    @app.get("/locations")
    async def get_locations(svc: TruckOptimizerService = Depends(get_service)):
        """Get all locations."""
        try:
            locations = svc.repo.get_locations()
            return [
                {
                    "id": location.id,
                    "name": location.name,
                    "address": location.address,
                    "lat": location.lat,
                    "lon": location.lon,
                    "window_start": location.window_start.isoformat() if location.window_start else None,
                    "window_end": location.window_end.isoformat() if location.window_end else None
                }
                for location in locations
            ]
        except Exception as e:
            logger.error(f"Failed to get locations: {e}")
            raise HTTPException(status_code=500, detail=str(e))
    
    @app.get("/items")
    async def get_items(svc: TruckOptimizerService = Depends(get_service)):
        """Get all items in catalog."""
        try:
            import json
            items = svc.repo.get_items()
            result = []
            for item in items:
                dims = None
                if item.dims_lwh_ft:
                    try:
                        dims = json.loads(item.dims_lwh_ft)
                    except Exception:
                        dims = None
                result.append({
                    "id": item.id,
                    "name": item.name,
                    "category": item.category,
                    "weight_lb_per_unit": item.weight_lb_per_unit,
                    "volume_ft3_per_unit": item.volume_ft3_per_unit,
                    "dims_lwh_ft": dims,
                    "requires_large_truck": item.requires_large_truck,
                    "path": svc.repo.get_item_path(item.id)
                })
            return result
        except Exception as e:
            logger.error(f"Failed to get items: {e}")
            raise HTTPException(status_code=500, detail=str(e))
    
    @app.get("/kpis/{date}", response_model=KPIResponse)
    async def get_kpis(
        date: str,
        svc: TruckOptimizerService = Depends(get_service)
    ):
        """Get KPIs for routes on a specific date."""
        try:
            route_assignments = svc.repo.get_route_assignments_by_date(date)
            unassigned_jobs = svc.repo.get_unassigned_jobs_by_date(date)
            all_jobs = svc.repo.get_jobs_by_date(date)
            
            if not route_assignments and not unassigned_jobs:
                raise HTTPException(
                    status_code=404,
                    detail=f"No optimization results found for date {date}"
                )
            
            # Calculate KPIs
            total_drive_minutes = sum(ra.total_drive_minutes for ra in route_assignments)
            total_service_minutes = sum(ra.total_service_minutes for ra in route_assignments)
            total_overtime_minutes = sum(ra.overtime_minutes for ra in route_assignments)
            trucks_used = len([ra for ra in route_assignments if ra.total_drive_minutes > 0])
            
            # Simple efficiency score (lower is better)
            efficiency_score = total_drive_minutes + total_service_minutes + (total_overtime_minutes * 2)
            
            # Priority score (would need more complex calculation)
            priority_score = 100.0  # Placeholder
            
            return KPIResponse(
                total_drive_minutes=total_drive_minutes,
                total_service_minutes=total_service_minutes,
                total_overtime_minutes=total_overtime_minutes,
                trucks_used=trucks_used,
                jobs_assigned=len(all_jobs) - len(unassigned_jobs),
                jobs_unassigned=len(unassigned_jobs),
                efficiency_score=efficiency_score,
                priority_score=priority_score
            )
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Failed to calculate KPIs: {e}")
            raise HTTPException(status_code=500, detail=str(e))
    
    # Additional endpoints for Streamlit UI
    from fastapi.responses import JSONResponse
    @app.post("/jobs")
    async def create_job(job_payload: Dict[str, Any], svc: TruckOptimizerService = Depends(get_service)):
        """Create a single job (simple JSON format used by new tests).

        Expected minimal payload:
        {
          "date": "YYYY-MM-DD",
          "location": {"name": str, "address": str, "lat": float, "lon": float},
          "action": "drop"|"pickup",
          "priority": int,
          "items": []  # optional list (ignored for now)
        }
        """
        try:
            loc = job_payload.get("location") or {}
            date_val = job_payload.get("date")
            if not date_val:
                raise HTTPException(status_code=400, detail="Missing date")
            loc_name = loc.get("name") or loc.get("address")
            if not loc_name:
                raise HTTPException(status_code=400, detail="Missing location.name or address")
            # Build import-compatible single row
            row = {
                "location": loc_name,
                "action": job_payload.get("action", "drop"),
                "items": "misc:1",  # simplified
                "priority": job_payload.get("priority", 1),
                "notes": "",
                "earliest": None,
                "latest": None,
                "service_minutes_override": None
            }
            request = ImportRequest(data=[row], date=date_val, clear_existing=False)
            await svc.import_jobs(request)
            # Set coordinates if provided
            if loc.get("lat") is not None and loc.get("lon") is not None:
                lrec = svc.repo.get_location_by_name(loc_name)
                if lrec:
                    try:
                        svc.repo.update_location_coordinates(lrec.id, float(loc["lat"]), float(loc["lon"]))
                    except Exception:
                        pass
            # Find newest job id for date
            jobs_for_day = svc.repo.get_jobs_by_date(date_val)
            job_id = max(j.id for j in jobs_for_day) if jobs_for_day else None
            return JSONResponse(status_code=201, content={"job_id": job_id})
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Failed to create job: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    @app.get("/jobs")
    async def get_jobs(date: str = None, svc: TruckOptimizerService = Depends(get_service)):
        """Get jobs, optionally filtered by date."""
        try:
            if date:
                jobs = svc.repo.get_jobs_by_date(date)
            else:
                jobs = svc.repo.get_jobs()
            
            # Convert to dict format for easier JSON serialization
            result = []
            for job in jobs:
                job_dict = {
                    "id": job.id,
                    "date": job.date,
                    "locationName": job.location.name if job.location else "Unknown",
                    "address": job.location.address if job.location else None,
                    "action": job.action.value,
                    "priority": job.priority,
                    "earliest": job.earliest.isoformat() if job.earliest else None,
                    "latest": job.latest.isoformat() if job.latest else None,
                    "notes": job.notes,
                    "items": [{"name": item.item.name, "quantity": item.qty} for item in job.job_items]
                }
                
                # Add coordinates if available
                if job.location and job.location.lat is not None and job.location.lon is not None:
                    job_dict["coordinates"] = {
                        "lat": job.location.lat,
                        "lng": job.location.lon
                    }
                
                result.append(job_dict)
            
            return result
        except Exception as e:
            logger.error(f"Failed to get jobs: {e}")
            raise HTTPException(status_code=500, detail=str(e))
    
    @app.post("/jobs/quick_add")
    @app.post("/jobs/quick-add")  # alias used by tests
    async def quick_add_job(job_data: Dict[str, Any], svc: TruckOptimizerService = Depends(get_service)):
        """Quickly add a single job with items.

        Accepts both legacy payload (location_name + date) and test payload (location only, date omitted).
        Returns created job_id for test compatibility.
        """
        try:
            from datetime import datetime as _dt
            # Support both keys
            location_name = job_data.get("location_name") or job_data.get("location")
            if not location_name:
                raise HTTPException(status_code=400, detail="Missing location or location_name")
            date_val = job_data.get("date") or _dt.now().date().isoformat()

            # Capture existing job ids to detect new one
            existing_ids = {j.id for j in svc.repo.get_jobs_by_date(date_val)}

            job_row = {
                "location": location_name,
                "action": job_data.get("action", "pickup"),
                "items": job_data.get("items", "misc:1"),
                "priority": job_data.get("priority", 1),
                "notes": job_data.get("notes", ""),
                "earliest": job_data.get("earliest"),
                "latest": job_data.get("latest"),
                "service_minutes_override": None
            }
            # If address provided, append for uniqueness
            if job_data.get("address"):
                job_row["location"] = f"{location_name} ({job_data['address']})"

            request = ImportRequest(
                data=[job_row],
                date=date_val,
                clear_existing=False
            )
            await svc.import_jobs(request)

            # Coordinates update
            if job_data.get("lat") and job_data.get("lon"):
                loc = svc.repo.get_location_by_name(location_name)
                if loc:
                    try:
                        svc.repo.update_location_coordinates(loc.id, float(job_data["lat"]), float(job_data["lon"]))
                    except Exception:
                        pass

            # Find newly created job id
            new_jobs = [j for j in svc.repo.get_jobs_by_date(date_val) if j.id not in existing_ids]
            job_id = new_jobs[0].id if new_jobs else None
            if job_id is None:
                raise HTTPException(status_code=500, detail="Failed to determine created job id")
            # Tests expect a structure with stats.jobs_created counting this creation
            return {"job_id": job_id, "success": True, "stats": {"jobs_created": 1}}
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Failed to add job: {e}")
            raise HTTPException(status_code=500, detail=str(e))
    
    @app.delete("/jobs/{job_id}")
    async def delete_job(job_id: int, svc: TruckOptimizerService = Depends(get_service)):
        """Soft delete a job by ID (idempotent)."""
        try:
            success = svc.repo.delete_job_by_id(job_id)
            if success:
                return {"message": f"Job {job_id} deleted successfully"}
            else:
                return {"message": f"Job {job_id} not found or already deleted"}
        except Exception as e:
            logger.error(f"Failed to delete job {job_id}: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    @app.post("/jobs/{job_id}/defer")
    async def defer_job(
        job_id: int, 
        defer_data: Optional[Dict[str, Any]] = None,
        svc: TruckOptimizerService = Depends(get_service)
    ):
        """Defer a job to the next day with optional priority update."""
        try:
            new_priority = None
            if defer_data and 'new_priority' in defer_data:
                new_priority = defer_data['new_priority']
            
            success = svc.repo.defer_job_to_next_day(job_id, new_priority)
            if success:
                return {"message": f"Job {job_id} deferred to next day"}
            else:
                raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Failed to defer job {job_id}: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    @app.post("/jobs/defer")
    async def bulk_defer_jobs(
        defer_request: Dict[str, Any],
        svc: TruckOptimizerService = Depends(get_service)
    ):
        """Defer multiple jobs to the next day with optional priority updates."""
        try:
            jobs = defer_request.get('jobs', [])
            if not jobs:
                return {"message": "No jobs provided", "deferred_count": 0}
            
            job_ids = [job['job_id'] for job in jobs]
            priority_updates = {job['job_id']: job.get('new_priority') for job in jobs if 'new_priority' in job}
            
            count = svc.repo.bulk_defer_jobs(job_ids, priority_updates)
            return {"message": f"{count} jobs deferred successfully", "deferred_count": count}
        except Exception as e:
            logger.error(f"Failed to bulk defer jobs: {e}")
            raise HTTPException(status_code=500, detail=str(e))
    
    @app.get("/catalog/trucks")
    async def get_catalog_trucks(svc: TruckOptimizerService = Depends(get_service)):
        """Get all trucks in catalog format."""
        try:
            trucks = svc.repo.get_trucks()
            return [
                {
                    "id": truck.id,
                    "name": truck.name,
                    "max_weight_lb": truck.max_weight_lb,
                    "bed_len_ft": truck.bed_len_ft,
                    "bed_width_ft": truck.bed_width_ft,
                    "height_limit_ft": truck.height_limit_ft,
                    "large_capable": truck.large_capable
                }
                for truck in trucks
            ]
        except Exception as e:
            logger.error(f"Failed to get trucks: {e}")
            raise HTTPException(status_code=500, detail=str(e))
    
    @app.post("/catalog/trucks")
    async def add_catalog_truck(truck_data: Dict[str, Any], svc: TruckOptimizerService = Depends(get_service)):
        """Add a truck to the catalog."""
        try:
            required = ["name", "max_weight_lb", "bed_len_ft", "bed_width_ft"]
            for r in required:
                if r not in truck_data:
                    raise HTTPException(status_code=400, detail=f"Missing field: {r}")
            try:
                created = svc.repo.create_truck({
                    "name": truck_data["name"],
                    "max_weight_lb": float(truck_data["max_weight_lb"]),
                    "bed_len_ft": float(truck_data["bed_len_ft"]),
                    "bed_width_ft": float(truck_data["bed_width_ft"]),
                    "height_limit_ft": float(truck_data.get("height_limit_ft", 0) or 0) or None,
                    "large_capable": bool(truck_data.get("large_capable", False))
                })
            except Exception as e:
                # If unique constraint on name, return the existing truck (idempotent by name)
                msg = str(e)
                if "UNIQUE constraint failed" in msg or "IntegrityError" in msg:
                    existing = svc.repo.get_truck_by_name(truck_data["name"])
                    if existing:
                        created = existing
                    else:
                        raise
                else:
                    raise
            return {"success": True, "truck": {
                "id": created.id,
                "name": created.name,
                "max_weight_lb": created.max_weight_lb,
                "bed_len_ft": created.bed_len_ft,
                "bed_width_ft": created.bed_width_ft,
                "height_limit_ft": created.height_limit_ft,
                "large_capable": created.large_capable
            }}
        except Exception as e:
            logger.error(f"Failed to add truck: {e}")
            raise HTTPException(status_code=500, detail=str(e))
    
    @app.delete("/catalog/trucks/{truck_id}")
    async def delete_catalog_truck(
        truck_id: int,
        force: bool = False,
        svc: TruckOptimizerService = Depends(get_service)
    ):
        """Delete a truck from the catalog."""
        try:
            # Check truck existence
            truck = svc.repo.get_truck_by_id(truck_id)
            if not truck:
                raise HTTPException(status_code=404, detail="Truck not found")

            # Classify assignments by date relative to today
            from datetime import datetime as _dt
            today = _dt.now().date()
            assignments = svc.repo.get_route_assignments_for_truck(truck_id)

            has_active_or_future = False
            for a in assignments:
                try:
                    a_date = _dt.fromisoformat(f"{a.date}T00:00:00").date()
                except Exception:
                    # If parsing fails, treat as active to be safe
                    a_date = today
                if a_date >= today:
                    has_active_or_future = True
                    break

            if has_active_or_future:
                # Active/future assignments block deletion regardless of force flag
                raise HTTPException(status_code=409, detail="Truck still referenced in routes")

            # Only past assignments exist (or none) → allow deletion. If force=true, also OK.
            deleted = svc.repo.delete_truck_and_assignments(truck_id)
            if not deleted:
                # Shouldn't happen because we found it, but guard
                raise HTTPException(status_code=404, detail="Truck not found")
            return Response(status_code=204)
        except HTTPException:
            # Preserve raised HTTP errors (e.g., 404)
            raise
        except Exception as e:
            logger.error(f"Failed to delete truck: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    @app.post("/admin/reset")
    async def admin_reset(payload: Optional[Dict[str, Any]] = None, svc: TruckOptimizerService = Depends(get_service)):
        """Admin: Full reset of optimization + dispatch state.

        Body (all optional):
        {
          "drop_trucks": true|false (default true),
          "drop_jobs": true|false (default true),
          "seed_demo": true|false (default false)
        }
        If seed_demo=true will insert 20 LA test jobs for today.
        """
        try:
            cfg = payload or {}
            drop_trucks = bool(cfg.get("drop_trucks", True))
            drop_jobs = bool(cfg.get("drop_jobs", True))
            seed_demo = bool(cfg.get("seed_demo", False))
            summary = svc.repo.full_reset(drop_trucks=drop_trucks, drop_jobs=drop_jobs)
            seeded: Optional[Dict[str, Any]] = None
            if seed_demo:
                from datetime import datetime as _dt
                today = _dt.utcnow().date().isoformat()
                seeded = await _seed_la_jobs(svc, today)
            return {"reset": summary, "seeded": seeded}
        except Exception as e:
            logger.error(f"Admin reset failed: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    async def _seed_la_jobs(svc: TruckOptimizerService, date: str) -> Dict[str, Any]:
        """Seed 20 deterministic LA/SFV test jobs (idempotent by location name+date)."""
        # Provided valley / city addresses (name, address). Coordinates left None for geocoder/ manual updates.
        jobs = [
            ("Test Job 1", "Sherman Oaks Galleria, 15301 Ventura Blvd, Sherman Oaks, CA 91403", None, None),
            ("Test Job 2", "818 Coffee, 11855 Ventura Blvd, Studio City, CA 91604", None, None),
            ("Test Job 3", "Westfield Topanga, 6600 Topanga Canyon Blvd, Canoga Park, CA 91303", None, None),
            ("Test Job 4", "CSUN, 18111 Nordhoff St, Northridge, CA 91330", None, None),
            ("Test Job 5", "Burbank Airport, 2627 N Hollywood Way, Burbank, CA 91505", None, None),
            ("Test Job 6", "Universal Studios, 100 Universal City Plaza, Universal City, CA 91608", None, None),
            ("Test Job 7", "NoHo Arts District, 11136 Magnolia Blvd, North Hollywood, CA 91601", None, None),
            ("Test Job 8", "Lake Balboa Park, 6300 Balboa Blvd, Van Nuys, CA 91406", None, None),
            ("Test Job 9", "Sepulveda Basin, 6350 Woodley Ave, Van Nuys, CA 91406", None, None),
            ("Test Job 10", "Pierce College, 6201 Winnetka Ave, Woodland Hills, CA 91371", None, None),
            ("Test Job 11", "Encino Hospital, 16237 Ventura Blvd, Encino, CA 91436", None, None),
            ("Test Job 12", "Porter Ranch Town Center, 19700 Rinaldi St, Porter Ranch, CA 91326", None, None),
            ("Test Job 13", "Granada Hills High, 10535 Zelzah Ave, Granada Hills, CA 91344", None, None),
            ("Test Job 14", "Warner Center Park, 5800 Topanga Canyon Blvd, Woodland Hills, CA 91367", None, None),
            ("Test Job 15", "Panorama Mall, 8401 Van Nuys Blvd, Panorama City, CA 91402", None, None),
            ("Test Job 16", "Hollywood Bowl, 2301 N Highland Ave, Los Angeles, CA 90068", None, None),
            ("Test Job 17", "Dodger Stadium, 1000 Vin Scully Ave, Los Angeles, CA 90012", None, None),
            ("Test Job 18", "Glendale Galleria, 100 W Broadway, Glendale, CA 91210", None, None),
            ("Test Job 19", "Van Nuys Airport, 16461 Sherman Way, Van Nuys, CA 91406", None, None),
            ("Test Job 20", "Reseda Park, 18411 Victory Blvd, Reseda, CA 91335", None, None),
        ]
        created = 0
        for name, address, lat, lon in jobs:
            # If location already exists for name, skip job creation duplication for same date
            if any(j.location and j.location.name == name and j.date == date for j in svc.repo.get_jobs_by_date(date)):
                continue
            row = {"location": name, "action": "drop", "items": "misc:1", "priority": 1, "notes": "seed"}
            from .schemas import ImportRequest as _ImportRequest
            req = _ImportRequest(data=[row], date=date, clear_existing=False)
            await svc.import_jobs(req)
            loc = svc.repo.get_location_by_name(name)
            if loc:
                try:
                    if lat is not None and lon is not None:
                        svc.repo.update_location_coordinates(loc.id, lat, lon)
                except Exception:
                    pass
            created += 1
        return {"jobs_attempted": len(jobs), "jobs_created": created, "date": date}

    @app.patch("/catalog/trucks/{truck_id}")
    async def update_catalog_truck(truck_id: int, truck_data: Dict[str, Any], svc: TruckOptimizerService = Depends(get_service)):
        """Update a truck in the catalog."""
        try:
            # Map frontend keys to backend model keys if needed
            mapping = {
                "max_weight_lb": "max_weight_lb",
                "bed_len_ft": "bed_len_ft",
                "bed_width_ft": "bed_width_ft",
                "height_limit_ft": "height_limit_ft",
                "large_capable": "large_capable",
                "name": "name",
            }
            updates: Dict[str, Any] = {}
            for k, v in truck_data.items():
                key = mapping.get(k)
                if key is None:
                    continue
                updates[key] = v

            updated = svc.repo.update_truck(truck_id, updates)
            if not updated:
                raise HTTPException(status_code=404, detail="Truck not found")
            return {"success": True}
        except HTTPException:
            # Propagate explicit HTTP errors
            raise
        except Exception as e:
            logger.error(f"Failed to update truck: {e}")
            raise HTTPException(status_code=500, detail=str(e))
    
    @app.get("/catalog/items")
    async def get_catalog_items(svc: TruckOptimizerService = Depends(get_service)):
        """Get all items in catalog format."""
        try:
            import json
            items = svc.repo.get_items()
            result = []
            for item in items:
                dims = None
                if item.dims_lwh_ft:
                    try:
                        dims = json.loads(item.dims_lwh_ft)
                    except Exception:
                        dims = None
                result.append({
                    "id": item.id,
                    "name": item.name,
                    "category": item.category.value,
                    "weight_lb_per_unit": item.weight_lb_per_unit,
                    "volume_ft3_per_unit": item.volume_ft3_per_unit,
                    "dims_lwh_ft": dims,
                    "requires_large_truck": item.requires_large_truck,
                    "path": svc.repo.get_item_path(item.id)
                })
            return result
        except Exception as e:
            logger.error(f"Failed to get items: {e}")
            raise HTTPException(status_code=500, detail=str(e))
    
    @app.post("/catalog/items")
    async def add_catalog_item(item_data: Dict[str, Any], svc: TruckOptimizerService = Depends(get_service)):
        """Add an item to the catalog."""
        try:
            required = ["name", "category", "weight_lb_per_unit"]
            for r in required:
                if r not in item_data:
                    raise HTTPException(status_code=400, detail=f"Missing field: {r}")

            # Normalize and create
            payload: Dict[str, Any] = {
                "name": item_data["name"],
                "category": item_data["category"],
                "weight_lb_per_unit": float(item_data["weight_lb_per_unit"]),
                "requires_large_truck": bool(item_data.get("requires_large_truck", False)),
            }
            if item_data.get("volume_ft3_per_unit") is not None:
                payload["volume_ft3_per_unit"] = float(item_data["volume_ft3_per_unit"])  
            if item_data.get("dims_lwh_ft") is not None:
                payload["dims_lwh_ft"] = item_data["dims_lwh_ft"]
            if item_data.get("path") is not None:
                payload["path"] = item_data["path"]

            try:
                created = svc.repo.create_item(payload)
            except Exception as e:
                msg = str(e)
                # If unique constraint on name, return existing item (idempotent by name)
                if "UNIQUE constraint failed" in msg or "IntegrityError" in msg:
                    existing = svc.repo.get_item_by_name(item_data["name"])
                    if existing:
                        # If caller supplied a path, ensure metadata aligns with requested path
                        if "path" in item_data:
                            try:
                                svc.repo.set_item_path(existing.id, item_data.get("path"))
                            except Exception:
                                pass
                        created = existing
                    else:
                        raise
                else:
                    raise

            # Build a fully serialized response without returning a live ORM object
            # Re-fetch in a fresh session to avoid detached/expired attribute access
            fetched = svc.repo.get_item_by_id(created.id)
            import json as _json
            dims = None
            if getattr(fetched, 'dims_lwh_ft', None):
                try:
                    dims = _json.loads(fetched.dims_lwh_ft) if isinstance(fetched.dims_lwh_ft, str) else fetched.dims_lwh_ft
                except Exception:
                    dims = None
            item_resp = {
                "id": fetched.id,
                "name": fetched.name,
                "category": getattr(fetched, 'category', None).value if getattr(fetched, 'category', None) is not None else None,
                "weight_lb_per_unit": fetched.weight_lb_per_unit,
                "volume_ft3_per_unit": fetched.volume_ft3_per_unit,
                "dims_lwh_ft": dims,
                "requires_large_truck": fetched.requires_large_truck,
                "path": svc.repo.get_item_path(fetched.id),
            }
            return {"success": True, "item": item_resp}
        except Exception as e:
            logger.error(f"Failed to add item: {e}")
            raise HTTPException(status_code=500, detail=str(e))
    
    @app.delete("/catalog/items/{item_id}")
    async def delete_catalog_item(item_id: int, svc: TruckOptimizerService = Depends(get_service)):
        """Delete an item from the catalog."""
        try:
            # Check if item exists
            # Lightweight existence check via get by name isn't ideal; let's fetch all and match id
            items = svc.repo.get_items()
            exists = any(i.id == item_id for i in items)
            if not exists:
                raise HTTPException(status_code=404, detail="Item not found")

            deleted = svc.repo.delete_item(item_id)
            if not deleted:
                raise HTTPException(status_code=409, detail="Item is in use and cannot be deleted")
            return Response(status_code=204)
        except Exception as e:
            logger.error(f"Failed to delete item: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    @app.patch("/catalog/items/{item_id}")
    async def update_catalog_item(item_id: int, item_data: Dict[str, Any], svc: TruckOptimizerService = Depends(get_service)):
        """Update an item in the catalog (including optional path)."""
        try:
            # Load existing
            items = svc.repo.get_items()
            item = next((i for i in items if i.id == item_id), None)
            if not item:
                raise HTTPException(status_code=404, detail="Item not found")

            updates: Dict[str, Any] = {}
            mapping = {
                "name": "name",
                "category": "category",
                "weight_lb_per_unit": "weight_lb_per_unit",
                "volume_ft3_per_unit": "volume_ft3_per_unit",
                "requires_large_truck": "requires_large_truck",
                "dims_lwh_ft": "dims_lwh_ft",
            }
            for k, v in item_data.items():
                key = mapping.get(k)
                if key is None:
                    continue
                updates[key] = v

            # Apply updates
            if updates:
                # Reuse upsert flow by name if changed, else direct session update through repo
                # Implement a simple direct update here
                with svc.repo.get_session() as session:
                    existing = session.get(type(item), item_id)
                    if not existing:
                        raise HTTPException(status_code=404, detail="Item not found")
                    import json
                    for key, val in updates.items():
                        if key == "dims_lwh_ft" and isinstance(val, (list, tuple)):
                            setattr(existing, key, json.dumps(list(val)))
                        else:
                            setattr(existing, key, val)
                    session.add(existing)
                    session.commit()

            # Path update handled via meta
            if "path" in item_data:
                svc.repo.set_item_path(item_id, item_data.get("path"))

            return {"success": True}
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Failed to update item: {e}")
            raise HTTPException(status_code=500, detail=str(e))
    
    @app.get("/site_materials")
    async def get_site_materials(svc: TruckOptimizerService = Depends(get_service)):
        """Get all site materials."""
        try:
            # TODO: Implement site materials in repository
            # For now, return empty list
            return []
        except Exception as e:
            logger.error(f"Failed to get site materials: {e}")
            raise HTTPException(status_code=500, detail=str(e))
    
    @app.post("/site_materials")
    async def add_site_material(material_data: Dict[str, Any], svc: TruckOptimizerService = Depends(get_service)):
        """Add or update site material."""
        try:
            # TODO: Implement site materials in repository
            return {"success": True, "message": "Site materials not yet implemented"}
        except Exception as e:
            logger.error(f"Failed to add site material: {e}")
            raise HTTPException(status_code=500, detail=str(e))
    
    @app.delete("/site_materials/{material_id}")
    async def delete_site_material(material_id: int, svc: TruckOptimizerService = Depends(get_service)):
        """Delete site material."""
        try:
            # TODO: Implement site materials deletion in repository
            return {"success": True, "message": "Site material deletion not yet implemented"}
        except Exception as e:
            logger.error(f"Failed to delete site material: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    # =========================
    # Dispatch APIs (Tab #2)
    # =========================
    @app.get("/dispatch/drivers")
    async def list_drivers(svc: TruckOptimizerService = Depends(get_service)):
        try:
            drivers = svc.repo.get_drivers()
            return [
                {
                    "id": d.id,
                    "name": d.name,
                    "phone_e164": d.phone_e164,
                    "assigned_truck_id": d.assigned_truck_id,
                    "active": d.active,
                }
                for d in drivers
            ]
        except Exception as e:
            logger.error(f"Failed to list drivers: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    @app.post("/dispatch/setup/{date}")
    async def setup_dispatch(date: str, svc: TruckOptimizerService = Depends(get_service)):
        """Create/refresh 3-stop batches for each driver based on latest routes."""
        try:
            summary = await svc.setup_dispatch_batches(date)
            return summary
        except Exception as e:
            logger.error(f"Failed to setup dispatch: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    @app.post("/dispatch/next/{driver_id}/{date}")
    async def send_next_batch(driver_id: int, date: str, svc: TruckOptimizerService = Depends(get_service)):
        """Manually send the next 3 stops to a driver (fallback when WA fails)."""
        try:
            res = await svc.send_next_batch(driver_id, date)
            return res
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Failed to send next batch: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    @app.get("/dispatch/state/{driver_id}/{date}")
    async def get_driver_dispatch_state(driver_id: int, date: str, svc: TruckOptimizerService = Depends(get_service)):
        """Get driver's current batch and stops with job details for a given date."""
        try:
            state = svc.repo.get_or_create_dispatch_state(driver_id, date)
            batches = svc.repo.list_dispatch_batches(driver_id, date)
            current_batch = batches.get(state.current_batch_index, [])
            # Expand job details
            stops = []
            for s in current_batch:
                job = svc.repo.get_job_by_id(s.job_id)
                stops.append({
                    "batch_stop_id": s.id,
                    "job_id": s.job_id,
                    "expected_arrival": s.expected_arrival.isoformat() if s.expected_arrival else None,
                    "expected_departure": s.expected_departure.isoformat() if s.expected_departure else None,
                    "completed_at": s.completed_at.isoformat() if s.completed_at else None,
                    "job": {
                        "id": job.id if job else s.job_id,
                        "priority": getattr(job, 'priority', None) if job else None,
                        "notes": getattr(job, 'notes', None) if job else None,
                        "action": getattr(getattr(job, 'action', None), 'value', None) if job else None,
                        "location": {
                            "name": getattr(getattr(job, 'location', None), 'name', None) if job else None,
                            "address": getattr(getattr(job, 'location', None), 'address', None) if job else None,
                            "lat": getattr(getattr(job, 'location', None), 'lat', None) if job else None,
                            "lon": getattr(getattr(job, 'location', None), 'lon', None) if job else None,
                        } if job else None,
                        "items": (
                            [{"name": ji.item.name, "quantity": ji.qty} for ji in job.job_items]
                            if job and getattr(job, 'job_items', None) else []
                        ),
                    }
                })
            return {
                "driver_id": driver_id,
                "date": date,
                "current_batch_index": state.current_batch_index,
                "total_batches": len(batches),
                "stops": stops,
            }
        except Exception as e:
            logger.error(f"Failed to get dispatch state: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    @app.post("/dispatch/insert_stop")
    async def insert_stop(payload: Dict[str, Any], svc: TruckOptimizerService = Depends(get_service)):
        """Insert a new stop into a driver's remaining route and rebuild batches."""
        try:
            required = ["driver_id", "date", "job_data"]
            for r in required:
                if r not in payload:
                    raise HTTPException(status_code=400, detail=f"Missing field: {r}")
            info = await svc.insert_stop_into_driver_route(
                driver_id=int(payload["driver_id"]),
                date=str(payload["date"]),
                job_data=payload["job_data"],
            )
            return info
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Failed to insert stop: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    @app.get("/dispatch/logs")
    async def get_dispatch_logs(date: Optional[str] = None, driver_id: Optional[int] = None, 
                               svc: TruckOptimizerService = Depends(get_service)):
        """Get expected vs actual time logs for analytics."""
        try:
            logs = svc.repo.list_time_logs(date=date, driver_id=driver_id)
            
            # Enrich with job/driver details
            result = []
            for log in logs:
                job = svc.repo.get_job_by_id(log.job_id) if log.job_id else None
                driver = svc.repo.get_driver_by_id(log.driver_id) if log.driver_id else None
                
                result.append({
                    "id": log.id,
                    "job_id": log.job_id,
                    "driver_id": log.driver_id,
                    "truck_id": log.truck_id,
                    "date": log.date,
                    "planned_start": log.planned_start.isoformat() if log.planned_start else None,
                    "actual_start": log.actual_start.isoformat() if log.actual_start else None,
                    "planned_end": log.planned_end.isoformat() if log.planned_end else None,
                    "actual_end": log.actual_end.isoformat() if log.actual_end else None,
                    "delta_start_minutes": log.delta_start_minutes,
                    "delta_end_minutes": log.delta_end_minutes,
                    "notes": log.notes,
                    "created_at": log.created_at.isoformat(),
                    "job_info": {
                        "location_name": job.location.name if job and job.location else None,
                        "action": job.action if job else None,
                        "priority": job.priority if job else None
                    } if job else None,
                    "driver_name": driver.name if driver else None
                })
                
            return result
            
        except Exception as e:
            logger.error(f"Failed to get dispatch logs: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    @app.post("/dispatch/whatsapp/webhook")
    async def whatsapp_webhook(request: Request, svc: TruckOptimizerService = Depends(get_service)):
        """Inbound webhook for WhatsApp provider (Twilio or others) with HMAC signature validation and idempotency.

        Expects text messages from drivers. When the body equals 'done' (case-insensitive),
        we will mark the current batch as complete and send the next 3 stops.
        """
        try:
            # Read raw body for signature validation
            body_bytes = await request.body()
            
            # HMAC signature validation (optional but recommended)
            webhook_secret = os.getenv("WEBHOOK_SECRET")
            if webhook_secret:
                signature = request.headers.get("X-Webhook-Signature") or request.headers.get("X-Twilio-Signature")
                if signature:
                    import hmac
                    import hashlib
                    expected = hmac.new(
                        webhook_secret.encode('utf-8'),
                        body_bytes,
                        hashlib.sha256
                    ).hexdigest()
                    
                    # Handle different signature formats
                    signature_value = signature.replace("sha256=", "") if signature.startswith("sha256=") else signature
                    
                    if not hmac.compare_digest(expected, signature_value):
                        logger.warning(f"Invalid webhook signature: {signature}")
                        raise HTTPException(status_code=401, detail="Invalid signature")
            
            # Parse payload
            content_type = request.headers.get("content-type", "")
            if "application/json" in content_type:
                payload = await request.json()
            else:
                # Form-encoded (common for Twilio)
                from urllib.parse import parse_qs
                form_data = parse_qs(body_bytes.decode('utf-8'))
                payload = {k: v[0] if v else "" for k, v in form_data.items()}
            
            # Extract message details
            body = str(payload.get("Body") or payload.get("body") or "").strip()
            from_e164 = str(payload.get("From") or payload.get("from") or "").replace("whatsapp:", "")
            message_id = str(payload.get("MessageSid") or payload.get("id") or "")
            date = str(payload.get("date") or datetime.utcnow().strftime("%Y-%m-%d"))
            
            # Idempotency check using message_id
            if message_id:
                existing_messages = svc.repo.session.exec(
                    select(DispatchMessage).where(DispatchMessage.provider_message_id == message_id)
                ).all()
                if existing_messages:
                    logger.info(f"Duplicate message ignored: {message_id}")
                    return {"status": "duplicate_ignored", "message_id": message_id}
            
            # Find driver by phone
            drivers = svc.repo.get_drivers()
            driver = next((d for d in drivers if (d.phone_e164 and d.phone_e164.endswith(from_e164))), None)
            
            # Log the message (with idempotency key)
            svc.repo.log_message({
                "driver_id": driver.id if driver else None,
                "date": date,
                "direction": "inbound",
                "content": body,
                "provider_message_id": message_id or None,
            })
            
            if not driver:
                logger.warning(f"Unknown driver from {from_e164}: {body}")
                return {"status": "unknown_driver"}

            if body.lower() == "done":
                # Handle completion and advance to next batch
                completion_result = await svc.handle_driver_completion(driver.id, date, body)
                next_result = await svc.send_next_batch(driver.id, date)
                return {
                    "status": "processed", 
                    "completion": completion_result,
                    "next_batch": next_result
                }
            else:
                return {"status": "ignored", "reason": "not_done_message"}
                
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Webhook handling failed: {e}")
            raise HTTPException(status_code=500, detail=str(e))
    
    return app


# Create the app instance
app = create_app()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
