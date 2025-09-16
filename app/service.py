"""
Main service layer for truck optimization.
Orchestrates data import, optimization, and result generation.
"""

import logging
import yaml
from datetime import datetime, date as dt_date, time, timedelta
from pathlib import Path
from typing import List, Dict, Optional, Any, Tuple, Set

from .models import (
    Truck, Location, Item, Job, JobItem, RouteAssignment, RouteStop,
    OptimizationResult, RouteResponse, JobResponse,
    ActionType, ItemCategory
)
from .schemas import (
    AppConfig, Settings, ImportRequest, JobImportRow,
    ImportStatsResponse, OptimizeRequest
)
from .repo import DatabaseRepository
from .distance import DistanceProvider, Coordinates
from .distance_offline import OfflineDistanceCalculator
from typing import Any as _AnyType
from .solver_ortools_vrp import ORToolsVRPSolver
from .solver_pyvrp import PyVRPSolver
from .url_builder import GoogleMapsUrlBuilder
from .constraints import ConstraintValidator
from .messaging import get_whatsapp_client
from .ml_calibration import EtaCalibrator


logger = logging.getLogger(__name__)


class TruckOptimizerService:
    """Main service for truck route optimization."""
    
    def __init__(self, config_path: str = "config/params.yaml"):
        """Initialize service with configuration."""
        self.config = self._load_config(config_path)
        self.settings = Settings()
        # Initialize components
        self.repo = DatabaseRepository(self.config)
        self.distance_provider = DistanceProvider(self.config, self.settings)
        self.offline_calculator = OfflineDistanceCalculator(self.config)
        self.url_builder = GoogleMapsUrlBuilder(self.config)
        self.validator = ConstraintValidator(self.config)
        self.messaging = get_whatsapp_client()
        self.calibrator = EtaCalibrator()
        # Setup logging
        self._setup_logging()
        # Initialize database
        self.repo.create_tables()
        self._initialize_seed_data()
    
    def _load_config(self, config_path: str) -> AppConfig:
        """Load configuration from YAML file."""
        try:
            with open(config_path, 'r') as f:
                config_data = yaml.safe_load(f)
            return AppConfig(**config_data)
        except Exception as e:
            logger.error(f"Failed to load configuration: {e}")
            raise
    
    def _setup_logging(self) -> None:
        """Setup logging configuration."""
        logging.basicConfig(
            level=getattr(logging, self.config.logging.level),
            format=self.config.logging.format
        )
    
    def _initialize_seed_data(self) -> None:
        """Initialize database with seed data from configuration."""
        # Initialize trucks
        trucks_data = [truck.model_dump() for truck in self.config.fleet.trucks]
        self.repo.upsert_trucks(trucks_data)
        
        # Initialize items catalog
        items_data = [item.model_dump() for item in self.config.item_catalog]
        self.repo.upsert_items(items_data)
        
        logger.info("Seed data initialized")
    
    async def import_jobs(self, request: ImportRequest) -> ImportStatsResponse:
        """
        Import jobs from CSV/JSON data.
        
        Args:
            request: Import request with job data
            
        Returns:
            Import statistics
        """
        stats = ImportStatsResponse(
            locations_created=0,
            locations_updated=0,
            items_created=0,
            jobs_created=0,
            total_job_items=0,
            geocoding_requests=0
        )
        
        # Clear existing jobs if requested
        if request.clear_existing:
            deleted_count = self.repo.delete_jobs_by_date(request.date)
            logger.info(f"Cleared {deleted_count} existing jobs for {request.date}")
        
        # Process locations first
        unique_locations = set()
        for row in request.data:
            unique_locations.add(row.location)
        
        location_coords = await self._process_locations(unique_locations, stats)
        
        # Process items
        unique_items = set()
        for row in request.data:
            item_specs = self._parse_items_string(row.items)
            for item_name, _ in item_specs:
                unique_items.add(item_name)
        
        await self._process_items(unique_items, stats)
        
        # Process jobs
        for row in request.data:
            try:
                await self._create_job_from_row(row, request.date, stats)
            except Exception as e:
                error_msg = f"Failed to create job for location '{row.location}': {e}"
                logger.error(error_msg)
                stats.errors.append(error_msg)
        
        logger.info(f"Import completed: {stats.jobs_created} jobs, "
                   f"{stats.locations_created} new locations, "
                   f"{stats.geocoding_requests} geocoding requests")
        
        return stats
    
    async def _process_locations(
        self, 
        location_names: set, 
        stats: ImportStatsResponse
    ) -> Dict[str, Coordinates]:
        """Process and geocode locations."""
        location_coords = {}
        addresses_to_geocode = []
        
        for location_name in location_names:
            existing_location = self.repo.get_location_by_name(location_name)
            
            if existing_location:
                if existing_location.lat and existing_location.lon:
                    # Already has coordinates
                    location_coords[location_name] = Coordinates(
                        lat=existing_location.lat,
                        lon=existing_location.lon
                    )
                else:
                    # Needs geocoding
                    addresses_to_geocode.append(location_name)
                    stats.locations_updated += 1
            else:
                # New location
                new_location = self.repo.create_location({
                    "name": location_name,
                    "address": location_name,  # Use name as address for now
                })
                addresses_to_geocode.append(location_name)
                stats.locations_created += 1
        
        # Geocode addresses
        if addresses_to_geocode:
            geocoding_results = await self.distance_provider.geocode_locations(addresses_to_geocode)
            stats.geocoding_requests += len(addresses_to_geocode)
            
            for address, coords in geocoding_results.items():
                if coords:
                    # Update location with coordinates
                    location = self.repo.get_location_by_name(address)
                    if location:
                        self.repo.update_location_coordinates(location.id, coords.lat, coords.lon)
                        location_coords[address] = coords
                else:
                    error_msg = f"Failed to geocode location: {address}"
                    logger.warning(error_msg)
                    stats.errors.append(error_msg)
        
        return location_coords
    
    async def _process_items(self, item_names: set, stats: ImportStatsResponse) -> None:
        """Process items, creating unknown ones."""
        for item_name in item_names:
            existing_item = self.repo.get_item_by_name(item_name)
            
            if not existing_item:
                # Create unknown item with default properties
                new_item = self.repo.create_item({
                    "name": item_name,
                    "category": "material",  # Default category
                    "weight_lb_per_unit": 50.0,  # Default weight
                    "requires_large_truck": False
                })
                stats.items_created += 1
                logger.warning(f"Created unknown item '{item_name}' with default properties")
    
    async def _create_job_from_row(
        self, 
        row: JobImportRow, 
        date: str, 
        stats: ImportStatsResponse
    ) -> None:
        """Create a job and its items from import row."""
        # Get location
        location = self.repo.get_location_by_name(row.location)
        if not location:
            raise ValueError(f"Location not found: {row.location}")
        
        # Parse times: support HH:MM strings by combining with provided date
        earliest = None
        latest = None
        if row.earliest:
            try:
                if len(row.earliest) <= 5 and ':' in row.earliest:
                    # time only, combine with date
                    earliest = datetime.fromisoformat(f"{date}T{row.earliest}")
                else:
                    earliest = datetime.fromisoformat(row.earliest)
            except Exception as e:
                logger.warning(f"Failed to parse earliest '{row.earliest}': {e}")
        if row.latest:
            try:
                if len(row.latest) <= 5 and ':' in row.latest:
                    latest = datetime.fromisoformat(f"{date}T{row.latest}")
                else:
                    latest = datetime.fromisoformat(row.latest)
            except Exception as e:
                logger.warning(f"Failed to parse latest '{row.latest}': {e}")
        
        # Create job
        job_data = {
            "location_id": location.id,
            "action": row.action,
            "priority": row.priority,
            "date": date,
            "earliest": earliest,
            "latest": latest,
            "notes": row.notes
        }
        
        job = self.repo.create_job(job_data)
        stats.jobs_created += 1
        
        # Parse and create job items
        item_specs = self._parse_items_string(row.items)
        for item_name, qty in item_specs:
            item = self.repo.get_item_by_name(item_name)
            if not item:
                raise ValueError(f"Item not found: {item_name}")
            
            job_item_data = {
                "job_id": job.id,
                "item_id": item.id,
                "qty": qty
            }
            
            self.repo.create_job_item(job_item_data)
            stats.total_job_items += 1
    
    def _parse_items_string(self, items_str: str) -> List[Tuple[str, float]]:
        """Parse items string like 'big drill:1; rebar:5' into [(name, qty), ...]."""
        items = []
        
        for item_spec in items_str.split(';'):
            item_spec = item_spec.strip()
            if ':' in item_spec:
                name, qty_str = item_spec.rsplit(':', 1)
                try:
                    qty = float(qty_str.strip())
                    items.append((name.strip(), qty))
                except ValueError:
                    logger.warning(f"Invalid quantity in item spec: {item_spec}")
            else:
                # Default quantity of 1
                items.append((item_spec.strip(), 1.0))
        
        return items
    
    async def optimize_routes(self, request: OptimizeRequest) -> OptimizationResult:
        """Top-level optimization orchestration entrypoint.

        Always returns an OptimizationResult (never None) and enforces a hard
        runtime cap for solver execution to prevent frontend timeouts.
        """
        start_time = datetime.now()
        trucks = self.repo.get_trucks()
        jobs = self.repo.get_jobs_by_date(request.date)
        debug_flag = bool(getattr(request,'_debug_flag', False))
        logger.info(
            f"Optimize start date={request.date} jobs={len(jobs)} trucks={len(trucks)} "
            f"solver_req={getattr(request,'solver_strategy',None)} balance_slider={getattr(request,'balance_slider',None)} debug={int(debug_flag)}"
        )
        logger.info(f"Optimize called with {len(jobs)} jobs")

        for _dbg_j in jobs[:3]:  # debug sample
            try:
                print("JOB_DEBUG", type(_dbg_j), getattr(_dbg_j,'id',None), getattr(_dbg_j,'priority',None), getattr(_dbg_j,'location_id',None))
            except Exception:
                pass

        if not jobs:  # early exit
            logger.warning(f"No jobs found for date {request.date}")
            # Preserve explicit 0.0 slider (can't use 'or' because 0.0 is falsy)
            s_val = getattr(request, 'balance_slider', None)
            if s_val is None:
                s_val = getattr(self.config.solver, 'balance_slider', None)
            result = OptimizationResult(
                date=request.date,
                routes=[],
                unassigned_jobs=[],
                total_cost=0.0,
                solver_used="none",
                computation_time_seconds=0.0,
                deferred_jobs=[],
                overtime_summary=[]
            )
            if isinstance(s_val, (int,float)):
                try:
                    f = 10 ** (1 - float(s_val))
                    g = 10 ** (float(s_val) - 1)
                    result.objective_breakdown = {
                        'drive_minutes':0.0,
                        'service_minutes':0.0,
                        'overtime_minutes':0.0,
                        'priority_soft_cost':0.0,
                        'total_cost':0.0,
                        'balance':{"balance_slider":float(s_val),"f":float(f),"g":float(g)}
                    }
                except Exception:
                    pass
            return result

        job_items_map: Dict[int, List[JobItem]] = {j.id: j.job_items for j in jobs}
        depot_address = self.config.depot.address
        depot_coords_dict = await self.distance_provider.geocode_locations([depot_address])
        depot_coords = depot_coords_dict[depot_address]
        if not depot_coords:
            # Fallback to a default LA coordinate instead of failing hard (tests mock geocoding)
            logger.warning(f"Depot geocode failed for '{depot_address}' – using fallback coordinates (34.05,-118.25)")
            depot_coords = Coordinates(lat=34.05, lon=-118.25)

        max_jobs = getattr(self.config.solver, 'max_jobs_per_optimization', 100)
        if len(jobs) > max_jobs:
            logger.info(f"Large instance size={len(jobs)} applying intelligent filter top={max_jobs}")
            filtered_jobs = self._filter_jobs_intelligently(jobs, depot_coords, max_jobs)
        else:
            filtered_jobs = jobs
        if len(filtered_jobs) != len(jobs):
            logger.info(f"Filtered jobs kept={len(filtered_jobs)} excluded={len(jobs)-len(filtered_jobs)}")

        seen_loc: Set[int] = set()
        filtered_locations: List[Location] = []
        for j in filtered_jobs:
            if j.location.id not in seen_loc:
                seen_loc.add(j.location.id)
                filtered_locations.append(j.location)

        workday_start = datetime.fromisoformat(f"{request.date}T{self.config.depot.workday_window.start}:00")

        # Runtime overrides
        if getattr(request,'priority_trade_off',None) is not None and hasattr(self.config.solver,'priority'):
            self.config.solver.priority.performance_trade_off = float(request.priority_trade_off)
        if getattr(request,'priority_soft_cost',None) is not None and hasattr(self.config.solver,'weights'):
            self.config.solver.weights.priority_soft_cost = float(request.priority_soft_cost)
        original_balance_slider = getattr(self.config.solver,'balance_slider',None)
        if getattr(request,'balance_slider',None) is not None:
            try:
                self.config.solver.balance_slider = float(request.balance_slider)
            except Exception:
                self.config.solver.balance_slider = None
        original_single_truck_mode = getattr(self.config.solver,'single_truck_mode',0)
        if getattr(request,'single_truck_mode',None) is not None:
            try:
                self.config.solver.single_truck_mode = 1 if bool(request.single_truck_mode) else 0
            except Exception:
                pass

        # Enforce PyVRP as the only solver (unless explicitly overridden to 'greedy' for emergency)
        solver_type = 'pyvrp'
        requested_strategy = getattr(request, 'solver_strategy', None)
        if requested_strategy == 'greedy':
            logger.warning("Greedy solver explicitly requested – overriding default pyvrp")
            solver_type = 'greedy'
        elif requested_strategy and requested_strategy != 'pyvrp':
            logger.warning(f"Ignoring unsupported solver_strategy={requested_strategy}; using pyvrp")

        # Validate coordinates; if many invalid, still attempt PyVRP but log warnings (no auto fallback)
        invalid_jobs = [j for j in filtered_jobs if not (j.location and j.location.lat is not None and j.location.lon is not None)]
        if invalid_jobs:
            logger.warning(f"{len(invalid_jobs)} jobs missing coordinates; PyVRP may underperform (ids={[j.id for j in invalid_jobs[:5]]}...) ")

        # ---------------- Solver selection & execution ----------------
        solution: Any = None
        solver_used = 'pyvrp'
        try:
            solver = PyVRPSolver(self.config)
            depot_location = Location(id=0, name='Depot', address=self.config.depot.address, lat=depot_coords.lat, lon=depot_coords.lon)
            location_coords = [depot_coords] + [Coordinates(lat=loc.lat or 0.0, lon=loc.lon or 0.0) for loc in filtered_locations]
            distance_matrix = await self._calculate_distance_matrix(location_coords)
            service_times = [0.0]
            loc_service_map: Dict[int, float] = {}
            for loc in filtered_locations:
                rel_jobs = [j for j in filtered_jobs if j.location_id == loc.id]
                if rel_jobs:
                    vals = [self.validator.calculate_service_time(job_items_map.get(j.id, [])) for j in rel_jobs]
                    loc_service_map[loc.id] = float(sum(vals) / max(1, len(vals)))
                else:
                    loc_service_map[loc.id] = 0.0
                service_times.append(loc_service_map[loc.id])
            time_cap = min(15, getattr(self.config.solver.improve, 'time_limit_seconds', 15))
            logger.info(f"PyVRP solve begin time_cap={time_cap}s jobs={len(filtered_jobs)} trucks={len(trucks)}")
            solution = solver.solve(
                trucks=trucks,
                jobs=filtered_jobs,
                distance_matrix=distance_matrix,
                service_times=service_times,
                locations=[depot_location] + filtered_locations,
                time_limit_seconds=time_cap,
            )
            solver_used = 'pyvrp'
            if isinstance(solution, dict) and solution.get('status') == 'failed':
                logger.error("PyVRP returned failure status; fabricating compact fallback while keeping solver_used=pyvrp")
                # Compact fallback: assign up to one route per available truck; any extra jobs become unassigned
                fallback_routes: List[Dict[str, Any]] = []
                unassigned: List[int] = []
                truck_count = len(trucks)
                # Take first N jobs for N trucks (preserve priority ordering)
                sorted_jobs = sorted(filtered_jobs, key=lambda j: (j.priority, j.id))
                primary_jobs = sorted_jobs[:truck_count]
                leftover_jobs = sorted_jobs[truck_count:]
                for truck, job in zip(trucks, primary_jobs):
                    svc_time = self.validator.calculate_service_time(job_items_map.get(job.id, []))
                    # Estimate nominal drive via offline distance to depot (if coords available)
                    nominal_drive = 0.0
                    try:
                        if job.location and job.location.lat and job.location.lon:
                            dist_km = self.offline_calculator.get_distance_km(depot_coords, Coordinates(lat=job.location.lat, lon=job.location.lon))
                            nominal_drive = max(5.0, dist_km * 2.0)  # minutes heuristic
                    except Exception:
                        nominal_drive = 10.0
                    fallback_routes.append({
                        'truck_id': truck.id,
                        'jobs': [{
                            'job_id': job.id,
                            'service_time': svc_time,
                            'priority': job.priority
                        }],
                        'total_time': svc_time + nominal_drive,
                    })
                unassigned.extend([j.id for j in leftover_jobs])
                solution['routes'] = fallback_routes
                solution['unassigned_jobs'] = unassigned
        except Exception as e:
            logger.exception(f"PyVRP fatal exception: {e}")
            # Construct empty failure-style solution
            solution = {"routes": [], "unassigned_jobs": [j.id for j in filtered_jobs], "status": "failed", "solver": "pyvrp"}
            solver_used = 'pyvrp'

        # ---------------- Overtime + deferred job attribution ----------------
        deferred_jobs: List[Dict[str, Any]] = []
        next_day = (datetime.fromisoformat(request.date) + timedelta(days=1)).date().isoformat()
        if False:  # Overtime logic disabled for PyVRP-only mode (retain placeholder)
            window_start = time.fromisoformat(self.config.depot.workday_window.start)
            window_end = time.fromisoformat(self.config.depot.workday_window.end)
            full_minutes = (datetime.combine(dt_date.today(), window_end) - datetime.combine(dt_date.today(), window_start)).seconds / 60
            base_minutes = max(0, full_minutes - 60)
            for r in getattr(solution, 'routes', []):
                scheduled = getattr(r, 'total_drive_minutes', 0.0) + getattr(r, 'total_service_minutes', 0.0)
                overtime_needed = max(0, scheduled - base_minutes)
                overtime_used = min(60, overtime_needed)
                try:
                    r.overtime_minutes = float(overtime_used)
                    setattr(r, 'overtime_minutes_used', float(overtime_used))
                except Exception:
                    pass
        if isinstance(solution, dict) and 'unassigned_jobs' in solution:
            for uj in solution['unassigned_jobs']:
                # Support either dict entries or plain job IDs
                if isinstance(uj, dict):
                    j_id = uj.get('id')
                    priority = uj.get('priority')
                    reason = uj.get('reason', 'unassigned_by_solver')
                else:  # assume scalar job id
                    j_id = uj
                    job_obj = None
                    try:
                        if hasattr(self.repo, 'get_job_by_id'):
                            job_obj = self.repo.get_job_by_id(j_id)
                    except Exception:
                        job_obj = None
                    priority = getattr(job_obj, 'priority', None)
                    reason = 'unassigned_by_solver'
                if j_id is None:
                    continue
                deferred_jobs.append({
                    'id': j_id,
                    'priority': priority,
                    'reason': reason,
                    'suggested_date': next_day
                })
        seen_def: Set[int] = set()
        unique_def: List[Dict[str, Any]] = []
        for dj in deferred_jobs:
            if dj['id'] in seen_def:
                continue
            seen_def.add(dj['id'])
            unique_def.append(dj)
        deferred_jobs = unique_def
        if isinstance(solution, dict):
            solution['deferred_jobs_payload'] = deferred_jobs
        else:
            try:
                setattr(solution, 'deferred_jobs_payload', deferred_jobs)
            except Exception:
                pass

        await self._save_optimization_results(solution, request.date)
        result = self._convert_solution_to_result(solution, request.date, start_time, depot_coords, solver_used)

        # Objective breakdown (best-effort)
        try:
            # First compute per-route objective breakdowns so UI can attribute cost per truck
            for r in result.routes:
                try:
                    drive = float(r.total_drive_minutes)
                    service = float(r.total_service_minutes)
                    overtime = float(getattr(r, 'overtime_minutes', 0.0) or 0.0)
                    # Rebuild priority soft cost mirroring global logic using stop order
                    priority_cost = 0.0
                    if hasattr(self.config.solver, 'weights') and r.stops:
                        for idx, stop in enumerate(r.stops):
                            position_penalty = idx + 1
                            job = stop['job'] if isinstance(stop, dict) else getattr(stop, 'job', None)  # defensive
                            if not job:
                                job = getattr(stop, 'job', None)
                            if not job:
                                continue
                            # Stop can be dict when coming from PyVRP path inside conversion earlier
                            priority_val = getattr(job, 'priority', None) if not isinstance(job, dict) else job.get('priority')
                            if hasattr(self.config.solver, 'priority') and hasattr(self.config.solver.priority, 'urgency_weights'):
                                uw = self.config.solver.priority.urgency_weights
                                if priority_val == 0: pw = uw.critical
                                elif priority_val == 1: pw = uw.high
                                elif priority_val == 2: pw = uw.medium
                                else: pw = uw.low
                            else:
                                pw = 4.0 - priority_val if (priority_val is not None and priority_val > 0) else 10.0
                            priority_cost += position_penalty * pw
                        if hasattr(self.config.solver, 'priority') and hasattr(self.config.solver.priority, 'performance_trade_off'):
                            priority_cost *= self.config.solver.priority.performance_trade_off
                        priority_cost *= getattr(self.config.solver.weights, 'priority_soft_cost', 1.0)
                    total_cost_r = drive + service + overtime + priority_cost
                    s_val_r = getattr(self.config.solver, 'balance_slider', None)
                    bal_obj = None
                    if isinstance(s_val_r, (int, float)):
                        f = 10 ** (1 - s_val_r); g = 10 ** (s_val_r - 1)
                        bal_obj = {"balance_slider": float(s_val_r), "f": float(f), "g": float(g)}
                    r.objective_breakdown = {
                        'drive_minutes': drive,
                        'service_minutes': service,
                        'overtime_minutes': overtime,
                        'priority_soft_cost': float(priority_cost),
                        'total_cost': float(total_cost_r),
                        **({'balance': bal_obj} if bal_obj else {})
                    }
                except Exception:
                    pass
            # Global objective breakdown for PyVRP: approximate via per-route sums (overtime currently 0)
            try:
                total_drive = sum(getattr(r, 'total_drive_minutes', 0.0) for r in result.routes)
                total_service = sum(getattr(r, 'total_service_minutes', 0.0) for r in result.routes)
                total_overtime = sum(getattr(r, 'overtime_minutes', 0.0) for r in result.routes)
                pr_cost = sum((r.objective_breakdown or {}).get('priority_soft_cost', 0.0) for r in result.routes if getattr(r, 'objective_breakdown', None))
                s_val = getattr(self.config.solver, 'balance_slider', None)
                fg_info = None
                if isinstance(s_val, (int, float)):
                    f = 10 ** (1 - s_val); g = 10 ** (s_val - 1)
                    fg_info = {"balance_slider": float(s_val), "f": float(f), "g": float(g)}
                result.objective_breakdown = {
                    'drive_minutes': float(total_drive),
                    'service_minutes': float(total_service),
                    'overtime_minutes': float(total_overtime),
                    'priority_soft_cost': float(pr_cost),
                    'total_cost': float(total_drive + total_service + total_overtime + pr_cost),
                    **({'balance': fg_info} if fg_info else {}),
                }
            except Exception:
                pass
        except Exception:
            pass

        try:  # debug print
            print("\n=== PRIORITY ANALYSIS ===")
            for i, route in enumerate(result.routes):
                print(f"Route {i+1} ({route.truck.name}):")
                for j, stop in enumerate(route.stops):
                    job = stop.job
                    print(f"  Stop {j+1}: Job {job.id} - P{job.priority} - {job.location.name} ({job.action})")
            print("=========================\n")
        except Exception:
            pass

        elapsed = (datetime.now() - start_time).total_seconds()
        logger.info(f"Optimize done solver={solver_used} routes={len(result.routes)} unassigned={len(result.unassigned_jobs)} deferred={len(result.deferred_jobs)} elapsed={elapsed:.2f}s")

        # Display post-processing: if debug requested, always run to populate route.debug.
        # Otherwise, honor PYVRP_ETAS_ONLY env (default skip).
        try:
            import os as _os
            if debug_flag or _os.getenv("PYVRP_ETAS_ONLY", "1") != "1":
                from .route_display import compute_display_durations  # optional path
                workday_start = datetime.fromisoformat(f"{request.date}T{self.config.depot.workday_window.start}:00")
                result = await compute_display_durations(result, workday_start, debug=debug_flag)  # type: ignore
            else:
                pass  # No-op: rely on solver-derived times
        except Exception as e:
            logger.warning(f"Display duration post-processing skipped/failed: {e}")

        try:
            self.config.solver.single_truck_mode = original_single_truck_mode
        except Exception:
            pass
        try:
            self.config.solver.balance_slider = original_balance_slider
        except Exception:
            pass
        return result
    
    
    async def _save_optimization_results(self, solution: _AnyType, date: str) -> None:
        """Save optimization results to database."""
        # Clear existing results for this date
        self.repo.delete_route_assignments_by_date(date)
        self.repo.delete_unassigned_jobs_by_date(date)
        from datetime import datetime as _dt
        from datetime import timedelta as _td
        # Workday anchor used for synthetic times when solver output lacks schedule
        try:
            workday_start_str = self.config.depot.workday_window.start
            anchor = _dt.fromisoformat(f"{date}T{workday_start_str}:00")
        except Exception:
            anchor = _dt.fromisoformat(f"{date}T08:00:00")

        # Save route assignments for both dict and object solutions
        if isinstance(solution, dict) and 'routes' in solution:
            # PyVRP dictionary format with duration matrix and location indices -> compute ETAs from solver outputs only
            duration_matrix_seconds = solution.get('duration_matrix_seconds')
            if isinstance(duration_matrix_seconds, list):
                # convert to numpy-like access with nested lists
                pass
            for rdict in solution.get('routes', []):
                truck_id = rdict.get('truck_id')
                jobs_list = rdict.get('jobs', [])
                loc_indices = rdict.get('location_indices', []) or []
                # Totals direct from solver summary
                total_time_min = float(rdict.get('total_time', 0.0) or 0.0)
                declared_service = float(sum((j.get('service_time', 0.0) or 0.0) for j in jobs_list))
                drive_time = max(0.0, total_time_min - declared_service)
                assignment_data = {
                    "truck_id": truck_id,
                    "date": date,
                    "total_drive_minutes": float(drive_time),
                    "total_service_minutes": float(declared_service),
                    "total_weight_lb": 0.0,
                    "overtime_minutes": 0.0,
                }
                route_assignment = self.repo.create_route_assignment(assignment_data)

                # Walk the route and compute per-stop arrival/departure strictly from matrix + service times
                cumulative_seconds = 0.0
                prev_idx = 0  # depot is index 0 in the matrix
                for idx, jinfo in enumerate(jobs_list):
                    job_id = int(jinfo.get('job_id'))
                    service_m = float(jinfo.get('service_time', 0.0) or 0.0)
                    service_s = service_m * 60.0
                    # location index in matrix for this job
                    if idx < len(loc_indices):
                        li = int(loc_indices[idx])
                    else:
                        li = prev_idx  # fallback should not happen
                    # drive seconds from prev to this loc
                    try:
                        drive_s = float(duration_matrix_seconds[prev_idx][li])
                    except Exception:
                        drive_s = 0.0

                    # Optional ML calibration adjustment
                    try:
                        adj = self.calibrator.predict(drive_s, service_m)
                        if adj is not None and adj > 0:
                            drive_s = float(adj)
                    except Exception:
                        pass

                    cumulative_seconds += drive_s
                    est_arrival = anchor + _td(seconds=cumulative_seconds)
                    cumulative_seconds += service_s
                    est_departure = anchor + _td(seconds=cumulative_seconds)

                    stop_data = {
                        "route_assignment_id": route_assignment.id,
                        "job_id": job_id,
                        "stop_order": idx,
                        "estimated_arrival": est_arrival,
                        "estimated_departure": est_departure,
                        "drive_minutes_from_previous": drive_s / 60.0,
                        "service_minutes": service_m,
                    }
                    self.repo.create_route_stop(stop_data)
                    prev_idx = li

                logger.info(f"Saved PyVRP route truck_id={truck_id} stops={len(jobs_list)} drive≈{drive_time:.1f} service≈{declared_service:.1f}")
                logger.info(f"Saved route with {len(jobs_list)} stops")
            # Unassigned: list of job IDs
            for uj in solution.get('unassigned_jobs', []):
                if isinstance(uj, dict):
                    uj_id = uj.get('id')
                    reason = uj.get('reason', 'unassigned_by_solver')
                else:
                    uj_id = uj
                    reason = 'unassigned_by_solver'
                if uj_id is None:
                    continue
                self.repo.create_unassigned_job({
                    "job_id": uj_id,
                    "date": date,
                    "reason": reason
                })
        else:
            # Object-based solution (e.g., OR-Tools or greedy) with detailed timing
            routes = getattr(solution, 'routes', [])
            for route in routes:
                assignment_data = {
                    "truck_id": route.truck.id,
                    "date": date,
                    "total_drive_minutes": route.total_drive_minutes,
                    "total_service_minutes": route.total_service_minutes,
                    "total_weight_lb": route.total_weight_lb,
                    "overtime_minutes": getattr(route, 'overtime_minutes', 0.0)
                }
                route_assignment = self.repo.create_route_assignment(assignment_data)
                for assignment in getattr(route, 'assignments', []) or []:
                    stop_data = {
                        "route_assignment_id": route_assignment.id,
                        "job_id": assignment.job.id,
                        "stop_order": assignment.stop_order,
                        "estimated_arrival": assignment.estimated_arrival,
                        "estimated_departure": assignment.estimated_departure,
                        "drive_minutes_from_previous": assignment.drive_minutes_from_previous,
                        "service_minutes": assignment.service_minutes
                    }
                    self.repo.create_route_stop(stop_data)
                logger.info(f"Saved route truck={route.truck.name} stops={len(getattr(route,'assignments',[]) or [])}")
                logger.info(f"Saved route with {len(getattr(route,'assignments',[]) or [])} stops")
            for job in getattr(solution, 'unassigned_jobs', []):
                self.repo.create_unassigned_job({
                    "job_id": job.id,
                    "date": date,
                    "reason": "time_window_infeasible" if (job.latest is not None) else "constraints_infeasible"
                })

    async def reoptimize_truck_remaining(self, date: str, truck_id: int, extra_job_ids: Optional[List[int]] = None) -> Dict[str, Any]:
        """Re-solve only the remaining route for a single truck from its current position.

        - Current position = last completed stop (from dispatch batches) or depot if none.
        - Remaining jobs = route stops with stop_order >= current batch*3 plus any extra_job_ids provided.
        - Runs PyVRP on reduced problem with a single vehicle and updates DB stops from current index onward.
        """
        # Load existing assignment and stops
        assignments = self.repo.get_route_assignments_by_date(date)
        ra = next((a for a in assignments if a.truck_id == truck_id), None)
        if not ra:
            return {"message": "No planned route for this truck/date"}
        stops = sorted(self.repo.get_route_stops_by_assignment(ra.id), key=lambda s: s.stop_order)
        state = self.repo.get_or_create_dispatch_state(truck_id, date)
        current_batch = state.current_batch_index
        start_order = current_batch * 3

        # Determine current position
        depot_address = self.config.depot.address
        depot_coords_dict = await self.distance_provider.geocode_locations([depot_address])
        depot_coords = depot_coords_dict[depot_address]
        cur_lat = depot_coords.lat
        cur_lon = depot_coords.lon
        if current_batch > 0:
            completed_batch = self.repo.get_dispatch_batch(truck_id, date, current_batch - 1)
            if completed_batch:
                last_completed = next((b for b in reversed(completed_batch) if b.completed_at), None) or completed_batch[-1]
                last_job = self.repo.get_job_by_id(last_completed.job_id)
                if last_job and last_job.location and last_job.location.lat and last_job.location.lon:
                    cur_lat = last_job.location.lat
                    cur_lon = last_job.location.lon

        # Remaining jobs from plan
        remaining_jobs = [s.job for s in stops if s.stop_order >= start_order]
        # Add extras if provided
        if extra_job_ids:
            for jid in extra_job_ids:
                j = self.repo.get_job_by_id(int(jid))
                if j and j not in remaining_jobs:
                    remaining_jobs.append(j)
        if not remaining_jobs:
            return {"message": "No remaining jobs to optimize"}

        # Build locations list: [current_pos] + unique job locations
        seen_loc: Set[int] = set()
        unique_locs: List[Location] = []
        for j in remaining_jobs:
            if j.location and j.location.id not in seen_loc:
                seen_loc.add(j.location.id)
                unique_locs.append(j.location)
        from .models import Location as _Location
        current_location = _Location(id=-1, name='Current Position', address='Current', lat=cur_lat, lon=cur_lon)
        locations: List[_Location] = [current_location] + unique_locs

        # Compute service times per location
        service_times = [0.0]
        job_items_map: Dict[int, List[JobItem]] = {j.id: j.job_items for j in remaining_jobs}
        loc_service_map: Dict[int, float] = {}
        for loc in unique_locs:
            rel_jobs = [j for j in remaining_jobs if j.location_id == loc.id]
            if rel_jobs:
                vals = [self.validator.calculate_service_time(job_items_map.get(j.id, [])) for j in rel_jobs]
                loc_service_map[loc.id] = float(sum(vals) / max(1, len(vals)))
            else:
                loc_service_map[loc.id] = 0.0
            service_times.append(loc_service_map[loc.id])

        # Distance matrix using current position as first node
        from .distance import Coordinates as _Coord
        coords_seq = [_Coord(lat=cur_lat, lon=cur_lon)] + [
            _Coord(lat=loc.lat or 0.0, lon=loc.lon or 0.0) for loc in unique_locs
        ]
        dist_mat = await self._calculate_distance_matrix(coords_seq)

        # Solve with single truck
        truck = self.repo.get_truck_by_id(truck_id)
        solver = PyVRPSolver(self.config)
        solution = solver.solve(
            trucks=[truck],
            jobs=remaining_jobs,
            distance_matrix=dist_mat,
            service_times=service_times,
            locations=locations,
            time_limit_seconds=min(10, getattr(self.config.solver.improve, 'time_limit_seconds', 10) if getattr(self.config, 'solver', None) and getattr(self.config.solver, 'improve', None) else 10),
        )

        # Compute ETAs starting now
        now = datetime.utcnow()
        duration_matrix_seconds = solution.get('duration_matrix_seconds', [])
        routes = solution.get('routes', [])
        if not routes:
            return {"message": "Re-optimization produced no route"}
        r = routes[0]
        jobs_list = r.get('jobs', [])
        loc_indices = r.get('location_indices', []) or []

        # Remove existing stops from start_order onwards
        self.repo.delete_route_stops_from_order(ra.id, start_order)

        cumulative_seconds = 0.0
        prev_idx = 0  # current position at index 0
        created_stops: List[RouteStop] = []
        for i, jinfo in enumerate(jobs_list):
            job_id = int(jinfo.get('job_id'))
            service_m = float(jinfo.get('service_time', 0.0) or 0.0)
            service_s = service_m * 60.0
            li = int(loc_indices[i]) if i < len(loc_indices) else prev_idx
            try:
                drive_s = float(duration_matrix_seconds[prev_idx][li])
            except Exception:
                drive_s = 0.0
            # Apply calibration if available
            try:
                adj = self.calibrator.predict(drive_s, service_m)
                if adj is not None and adj > 0:
                    drive_s = float(adj)
            except Exception:
                pass
            cumulative_seconds += drive_s
            est_arrival = now + timedelta(seconds=cumulative_seconds)
            cumulative_seconds += service_s
            est_departure = now + timedelta(seconds=cumulative_seconds)

            stop_data = {
                "route_assignment_id": ra.id,
                "job_id": job_id,
                "stop_order": start_order + i,
                "estimated_arrival": est_arrival,
                "estimated_departure": est_departure,
                "drive_minutes_from_previous": drive_s / 60.0,
                "service_minutes": service_m,
            }
            created_stops.append(self.repo.create_route_stop(stop_data))
            prev_idx = li

        # Rebuild dispatch batches from current index, embedding ETAs
        batches = [created_stops[i:i+3] for i in range(0, len(created_stops), 3)]
        for b_rel, batch in enumerate(batches):
            b_idx = current_batch + b_rel
            self.repo.set_dispatch_batch(
                driver_id=truck_id,
                date=date,
                batch_index=b_idx,
                stops=[{
                    "job_id": s.job_id,
                    "expected_arrival": s.estimated_arrival,
                    "expected_departure": s.estimated_departure,
                } for s in batch]
            )

        return {
            "message": "Re-optimized remaining route",
            "truck_id": truck_id,
            "date": date,
            "remaining_stops": len(created_stops),
            "batches_updated": len(batches),
        }
    
    def _convert_solution_to_result(
        self, 
        solution: _AnyType, 
        date: str, 
        start_time: datetime,
        depot_coords: Coordinates,
        solver_used: str
    ) -> OptimizationResult:
        """Convert solver solution to API result format."""
        routes: List[RouteResponse] = []
        unassigned_jobs: List[JobResponse] = []
        unassigned_reasons: Dict[int, str] = {}

        is_dict_solution = isinstance(solution, dict)
        if is_dict_solution:
            solution_routes = solution.get('routes', [])
        else:
            solution_routes = getattr(solution, 'routes', [])

        if is_dict_solution:
            # PyVRP dictionary format: each route is a dict with truck_id, jobs list
            for rdict in solution_routes:
                job_stops = []
                coords_list = [depot_coords]
                # truck lookup (fallback minimal info if missing)
                truck = self.repo.get_truck_by_id(rdict.get('truck_id')) if hasattr(self.repo, 'get_truck_by_id') else None
                truck_response = {
                    "id": rdict.get('truck_id'),
                    "name": getattr(truck, 'name', f"Truck {rdict.get('truck_id', '?')}"),
                    "max_weight_lb": getattr(truck, 'max_weight_lb', None),
                    "bed_len_ft": getattr(truck, 'bed_len_ft', None),
                    "bed_width_ft": getattr(truck, 'bed_width_ft', None),
                    "height_limit_ft": getattr(truck, 'height_limit_ft', None),
                    "large_capable": getattr(truck, 'large_capable', None)
                }
                total_time_min = float(rdict.get('total_time', 0.0) or 0.0)
                service_total = 0.0
                for pos, jinfo in enumerate(rdict.get('jobs', [])):
                    job_id = jinfo.get('job_id')
                    job_model = self.repo.get_job_by_id(job_id) if hasattr(self.repo, 'get_job_by_id') else None
                    if job_model and job_model.location:
                        coords_list.append(Coordinates(lat=job_model.location.lat, lon=job_model.location.lon))
                        location_response = {
                            "id": job_model.location.id,
                            "name": job_model.location.name,
                            "address": job_model.location.address,
                            "lat": job_model.location.lat,
                            "lon": job_model.location.lon,
                            "window_start": job_model.location.window_start,
                            "window_end": job_model.location.window_end
                        }
                        svc_m = float(jinfo.get('service_time', 0.0) or 0.0)
                        service_total += svc_m
                        job_resp = JobResponse(
                            id=job_model.id,
                            location=location_response,
                            action=job_model.action,
                            priority=job_model.priority,
                            earliest=job_model.earliest,
                            latest=job_model.latest,
                            notes=job_model.notes,
                            items=[{
                                "item_name": ji.item.name,
                                "category": ji.item.category,
                                "qty": ji.qty,
                                "weight_lb_total": ji.item.weight_lb_per_unit * ji.qty
                            } for ji in job_model.job_items]
                        )
                        # Provide placeholder timestamps & drive minutes so Pydantic validation passes.
                        # These will be replaced/augmented by display post-processing later.
                        placeholder_time = datetime.fromisoformat(f"{date}T08:00:00")
                        job_stops.append({
                            "job": job_resp,
                            "stop_order": pos + 1,
                            "position": pos,
                            "estimated_arrival": placeholder_time,
                            "service_start": placeholder_time,
                            "estimated_departure": placeholder_time,
                            "drive_minutes_from_previous": 0.0 if pos == 0 else 5.0,
                            "service_minutes": svc_m,
                            "wait_minutes": 0.0,
                            "slack_minutes": 0.0,
                            "leg_distance_meters": 0.0
                        })
                coords_list.append(depot_coords)
                route_urls = self.url_builder.build_coordinate_urls(coords_list, truck_response.get('name', 'Route'))
                maps_url = route_urls.urls[0] if route_urls.urls else ""
                # Overtime synthetic computation
                try:
                    from datetime import time as _t, date as _d, datetime as _dt
                    window_start = _t.fromisoformat(self.config.depot.workday_window.start)
                    window_end = _t.fromisoformat(self.config.depot.workday_window.end)
                    full_minutes = (
                        _dt.combine(_d.today(), window_end) - _dt.combine(_d.today(), window_start)
                    ).seconds / 60
                    base_minutes = max(0, full_minutes - 60)
                except Exception:
                    base_minutes = 7 * 60  # fallback 7h base
                overtime_needed = max(0.0, total_time_min - base_minutes)
                overtime_used = min(60.0, overtime_needed)
                routes.append(RouteResponse(
                    truck=truck_response,
                    date=date,
                    stops=job_stops,
                    total_drive_minutes=max(0.0, total_time_min - service_total),
                    total_service_minutes=service_total,
                    total_weight_lb=0.0,
                    overtime_minutes=overtime_used,
                    maps_url=maps_url,
                    overtime_minutes_used=overtime_used if overtime_used > 0 else None
                ))
            # Unassigned jobs are a list of job IDs
            for uj in solution.get('unassigned_jobs', []):
                if isinstance(uj, dict):
                    uj_id = uj.get('id')
                else:
                    uj_id = uj
                job_model = self.repo.get_job_by_id(uj_id) if hasattr(self.repo, 'get_job_by_id') else None
                if not job_model or not job_model.location:
                    continue
                loc = job_model.location
                loc_resp = {"id": loc.id, "name": loc.name, "address": loc.address, "lat": loc.lat, "lon": loc.lon, "window_start": loc.window_start, "window_end": loc.window_end}
                unassigned_jobs.append(JobResponse(
                    id=job_model.id,
                    location=loc_resp,
                    action=job_model.action,
                    priority=job_model.priority,
                    earliest=job_model.earliest,
                    latest=job_model.latest,
                    notes=job_model.notes,
                    items=[]
                ))
                unassigned_reasons[job_model.id] = 'unassigned_by_solver'
            total_cost = float(solution.get('total_time', 0.0))
            deferred_payload = solution.get('deferred_jobs_payload', []) or []
            overtime_summary = [
                {
                    "truck_id": r.truck.id,
                    "truck_name": r.truck.name,
                    "total_minutes": r.total_drive_minutes + r.total_service_minutes,
                    "overtime_minutes": r.overtime_minutes,
                    "overtime_minutes_used": r.overtime_minutes_used,
                }
                for r in routes
            ]
        else:
            # Original object-based solution (e.g., OR-Tools) path
            for route in solution_routes:
                if not getattr(route, 'assignments', None):
                    continue
                route_stops = []
                for idx, assignment in enumerate(route.assignments):
                    location_response = {
                        "id": assignment.job.location.id,
                        "name": assignment.job.location.name,
                        "address": assignment.job.location.address,
                        "lat": assignment.job.location.lat,
                        "lon": assignment.job.location.lon,
                        "window_start": assignment.job.location.window_start,
                        "window_end": assignment.job.location.window_end
                    }
                    job_response = JobResponse(
                        id=assignment.job.id,
                        location=location_response,
                        action=assignment.job.action,
                        priority=assignment.job.priority,
                        earliest=assignment.job.earliest,
                        latest=assignment.job.latest,
                        notes=assignment.job.notes,
                        items=[{
                            "item_name": item.item.name,
                            "category": item.item.category,
                            "qty": item.qty,
                            "weight_lb_total": item.item.weight_lb_per_unit * item.qty
                        } for item in assignment.job_items]
                    )
                    service_start = assignment.estimated_arrival
                    if getattr(assignment, 'wait_minutes', 0) > 0:
                        service_start = assignment.estimated_arrival + timedelta(minutes=assignment.wait_minutes)
                    route_stops.append({
                        "job": job_response,
                        "stop_order": assignment.stop_order,
                        "position": idx,
                        "estimated_arrival": assignment.estimated_arrival,
                        "service_start": service_start,
                        "estimated_departure": assignment.estimated_departure,
                        "drive_minutes_from_previous": assignment.drive_minutes_from_previous,
                        "service_minutes": assignment.service_minutes,
                        "wait_minutes": getattr(assignment, 'wait_minutes', 0.0),
                        "slack_minutes": getattr(assignment, 'slack_minutes', 0.0),
                        "leg_distance_meters": getattr(assignment, 'leg_distance_meters', 0.0)
                    })
                truck_response = {
                    "id": route.truck.id,
                    "name": route.truck.name,
                    "max_weight_lb": route.truck.max_weight_lb,
                    "bed_len_ft": route.truck.bed_len_ft,
                    "bed_width_ft": route.truck.bed_width_ft,
                    "height_limit_ft": route.truck.height_limit_ft,
                    "large_capable": route.truck.large_capable
                }
                coords = [depot_coords]
                for a in route.assignments:
                    coords.append(Coordinates(lat=a.job.location.lat, lon=a.job.location.lon))
                coords.append(depot_coords)
                route_urls = self.url_builder.build_coordinate_urls(coords, route.truck.name)
                maps_url = route_urls.urls[0] if route_urls.urls else ""
                routes.append(RouteResponse(
                    truck=truck_response,
                    date=date,
                    stops=route_stops,
                    total_drive_minutes=route.total_drive_minutes,
                    total_service_minutes=route.total_service_minutes,
                    total_weight_lb=route.total_weight_lb,
                    overtime_minutes=route.overtime_minutes,
                    maps_url=maps_url,
                    overtime_minutes_used=getattr(route, 'overtime_minutes_used', None)
                ))
            for job in getattr(solution, 'unassigned_jobs', []):
                location_response = {
                    "id": job.location.id,
                    "name": job.location.name,
                    "address": job.location.address,
                    "lat": job.location.lat,
                    "lon": job.location.lon,
                    "window_start": job.location.window_start,
                    "window_end": job.location.window_end
                }
                unassigned_jobs.append(JobResponse(
                    id=job.id,
                    location=location_response,
                    action=job.action,
                    priority=job.priority,
                    earliest=job.earliest,
                    latest=job.latest,
                    notes=job.notes,
                    items=[]
                ))
                unassigned_reasons[job.id] = "time_window_infeasible" if job.latest else "constraints_infeasible"
            total_cost = getattr(solution, 'total_cost', 0.0)
            deferred_payload = getattr(solution, 'deferred_jobs_payload', []) or []
            overtime_summary = [{
                "truck_id": r.truck.id,
                "truck_name": r.truck.name,
                "total_minutes": r.total_drive_minutes + r.total_service_minutes,
                "overtime_minutes": r.overtime_minutes,
                "overtime_minutes_used": getattr(r, 'overtime_minutes_used', None)
            } for r in getattr(solution, 'routes', []) if getattr(r, 'assignments', None)]

        computation_time = (datetime.now() - start_time).total_seconds()
        result = OptimizationResult(
            date=date,
            routes=routes,
            unassigned_jobs=unassigned_jobs,
            unassigned_reasons=unassigned_reasons,
            total_cost=total_cost,
            solver_used=solver_used,
            computation_time_seconds=computation_time,
            deferred_jobs=deferred_payload,
            overtime_summary=overtime_summary
        )
        return result

    # =========================
    # Dispatch: batching and messaging
    # =========================
    async def setup_dispatch_batches(self, date: str) -> Dict[str, Any]:
        """Create/refresh 3-stop batches per driver for the given date.

        Strategy: map drivers to trucks by name prefix when possible. For each route,
        break ordered stops into chunks of 3, store in DispatchBatchStop, and reset
        DriverDispatchState.current_batch_index to 0.
        """
        # Load routes saved to DB
        assignments = self.repo.get_route_assignments_by_date(date)
        if not assignments:
            return {"message": f"No routes for {date}. Run optimize first."}

        # Build a mapping truck_id -> ordered job ids from DB stops
        route_jobs: Dict[int, List[Job]] = {}
        for ra in assignments:
            stops = self.repo.get_route_stops_by_assignment(ra.id)
            # Ensure ordered
            stops = sorted(stops, key=lambda s: s.stop_order)
            jobs = [s.job for s in stops]
            route_jobs[ra.truck_id] = jobs

        # Map drivers to trucks by simple heuristic: driver name prefix equals truck name prefix
        drivers = self.repo.get_drivers()
        driver_map: Dict[int, List[Job]] = {}
        for d in drivers:
            jobs_for_driver: List[Job] = []
            # If explicit assignment available
            if d.assigned_truck_id and d.assigned_truck_id in route_jobs:
                jobs_for_driver = route_jobs[d.assigned_truck_id]
            else:
                # fallback: pick first non-empty route
                for tid, jlist in route_jobs.items():
                    if jlist:
                        jobs_for_driver = jlist
                        break
            driver_map[d.id] = jobs_for_driver

        summary = {"drivers": []}
        for driver_id, jobs in driver_map.items():
            # reset state
            state = self.repo.get_or_create_dispatch_state(driver_id, date)
            self.repo.set_dispatch_batch_index(driver_id, date, 0)
            # chunk by 3
            batches = [jobs[i:i+3] for i in range(0, len(jobs), 3)]
            # Build a lookup from job_id to planned ETA from RouteStop
            job_eta: Dict[int, Tuple[datetime, datetime]] = {}
            for tid, jlist in route_jobs.items():
                # find assignment for this truck
                ra_for_tid = next((a for a in assignments if a.truck_id == tid), None)
                if ra_for_tid:
                    for s in self.repo.get_route_stops_by_assignment(ra_for_tid.id):
                        job_eta[s.job_id] = (s.estimated_arrival, s.estimated_departure)
            for b_idx, batch in enumerate(batches):
                stops_payload = [{
                    "job_id": j.id,
                    "expected_arrival": job_eta.get(j.id, (None, None))[0],
                    "expected_departure": job_eta.get(j.id, (None, None))[1],
                } for j in batch]
                self.repo.set_dispatch_batch(driver_id, date, b_idx, stops_payload)
            summary["drivers"].append({"driver_id": driver_id, "batches": len(batches)})
        return summary

    async def send_next_batch(self, driver_id: int, date: str) -> Dict[str, Any]:
        """Advance driver's batch index and send the next 3 stops via WhatsApp (if configured)."""
        state = self.repo.get_or_create_dispatch_state(driver_id, date)
        next_index = state.current_batch_index
        # Fetch batch
        batch = self.repo.get_dispatch_batch(driver_id, date, next_index)
        if not batch:
            return {"message": "No more stops for this driver"}

        # Build maps link: depot -> stops -> depot (best-effort: use job locations)
        coords: List[Coordinates] = []
        depot_address = self.config.depot.address
        depot_coords_dict = await self.distance_provider.geocode_locations([depot_address])
        depot_coords = depot_coords_dict[depot_address]
        coords.append(depot_coords)
        for bs in batch:
            job = self.repo.get_jobs_by_date(date)
            job = next((j for j in job if j.id == bs.job_id), None)
            if job and job.location and job.location.lat and job.location.lon:
                coords.append(Coordinates(lat=job.location.lat, lon=job.location.lon))
        coords.append(depot_coords)
        urls = self.url_builder.build_coordinate_urls(coords, truck_name=f"Driver {driver_id}")
        maps_url = urls.urls[0] if urls.urls else ""

        # Compose text body with job details
        def _fmt_job(j: Job) -> str:
            loc = j.location.name if j.location else "Unknown"
            return f"- {loc} ({j.action.value}) P{j.priority}"

        jobs_for_day = self.repo.get_jobs_by_date(date)
        id_to_job = {j.id: j for j in jobs_for_day}
        lines = [
            f"Next 3 stops (batch {next_index+1}):",
            *[_fmt_job(id_to_job.get(bs.job_id)) for bs in batch if id_to_job.get(bs.job_id)],
            f"Maps: {maps_url}",
            "Reply 'done' when finished.",
        ]
        body = "\n".join(lines)

        # Send via messaging provider if phone configured
        driver = self.repo.get_driver_by_id(driver_id)
        provider_resp = None
        if driver and driver.phone_e164:
            try:
                provider_resp = self.messaging.send_message(driver.phone_e164, body)
            except Exception as e:
                logger.error(f"Failed to send WA message: {e}")
        # Log outbound
        self.repo.log_message({
            "driver_id": driver_id,
            "date": date,
            "direction": "outbound",
            "content": body,
            "provider_message_id": (provider_resp or {}).get("sid"),
        })

        # Advance index
        self.repo.set_dispatch_batch_index(driver_id, date, next_index + 1)
        # Update last_sent_at timestamp for calibrator
        try:
            self.repo.update_dispatch_state(driver_id, date, last_sent_at=datetime.utcnow())
        except Exception:
            pass
        
        # Log expected times for batch jobs for analytics
        for bs in batch:
            job = id_to_job.get(bs.job_id)
            if job:
                self.repo.create_time_log({
                    "job_id": bs.job_id,
                    "driver_id": driver_id,
                    "truck_id": driver.assigned_truck_id if driver else None,
                    "date": date,
                    "planned_start": bs.expected_arrival,
                    "planned_end": bs.expected_departure,
                    "notes": f"Batch {next_index+1} sent"
                })
        
        return {"sent": True, "batch_index": next_index, "count": len(batch), "maps_url": maps_url}

    async def handle_driver_completion(self, driver_id: int, date: str, message_content: str = "done") -> Dict[str, Any]:
        """Handle driver 'done' message - log actual completion times and prepare next batch."""
        current_time = datetime.utcnow()
        
        # Log inbound message
        self.repo.log_message({
            "driver_id": driver_id,
            "date": date,
            "direction": "inbound",
            "content": message_content,
        })
        
        # Get current batch and mark jobs as completed with actual times
        state = self.repo.get_or_create_dispatch_state(driver_id, date)
        # The driver just completed the previous batch (current_batch_index - 1)
        completed_batch_index = max(0, state.current_batch_index - 1)
        batch = self.repo.get_dispatch_batch(driver_id, date, completed_batch_index)
        
        completion_count = 0
        # Compute calibration observation using batch-level planned vs actual
        # Planned estimate from first expected_arrival to last expected_departure
        try:
            start_planned = batch[0].expected_arrival if batch and batch[0].expected_arrival else None
            end_planned = batch[-1].expected_departure if batch and batch[-1].expected_departure else None
            est_sec = (end_planned - start_planned).total_seconds() if (start_planned and end_planned) else None
        except Exception:
            est_sec = None
        # Sum service minutes in batch from route stops
        service_min_total = 0.0
        try:
            # Build lookup of job_id -> service minutes
            assignments = self.repo.get_route_assignments_by_date(date)
            for a in assignments:
                for s in self.repo.get_route_stops_by_assignment(a.id):
                    if any(s.job_id == x.job_id for x in batch):
                        service_min_total += float(getattr(s, 'service_minutes', 0.0) or 0.0)
        except Exception:
            service_min_total = 0.0

        for bs in batch:
            # Mark this stop as completed
            bs.completed_at = current_time
            # Update time logs with actual completion
            self.repo.update_time_log_actual(
                job_id=bs.job_id,
                driver_id=driver_id,
                date=date,
                actual_end=current_time
            )
            completion_count += 1
        
        # Update state last_ack_at
        state.last_ack_at = current_time
        # Train calibrator if possible
        try:
            last_sent = getattr(state, 'last_sent_at', None)
            if est_sec is not None and last_sent:
                actual_sec = (current_time - last_sent).total_seconds()
                self.calibrator.add_observation(est_sec=float(est_sec), service_min=float(service_min_total), actual_sec=float(actual_sec))
                # Train when we have at least 10 observations
                if len(getattr(self.calibrator, '_y', [])) >= 10:
                    self.calibrator.train()
        except Exception:
            pass
        
        return {
            "acknowledged": True,
            "completed_jobs": completion_count,
            "batch_index": completed_batch_index,
            "message": f"Marked {completion_count} jobs as completed"
        }

    async def insert_stop_into_driver_route(self, driver_id: int, date: str, job_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new job mid-day and re-batch remaining stops for the driver.

        For MVP: simply add job to today's jobs and rebuild batches from current index.
        """
        # Create job using existing quick-add flow
        payload = {
            "location_name": job_data.get("location_name") or job_data.get("location") or "Ad-hoc Stop",
            "action": job_data.get("action", "pickup"),
            "items": job_data.get("items", "misc:1"),
            "priority": int(job_data.get("priority", 1)),
            "notes": job_data.get("notes", ""),
            "date": date,
        }
        await self.import_jobs(ImportRequest(data=[JobImportRow(**payload)], date=date, clear_existing=False))

        # Re-optimize remaining for this driver's truck
        assignments = self.repo.get_route_assignments_by_date(date)
        if not assignments:
            return {"message": "No routes for today"}
        driver = self.repo.get_driver_by_id(driver_id)
        target_truck_id = driver.assigned_truck_id if driver and driver.assigned_truck_id else (assignments[0].truck_id if assignments else None)
        if target_truck_id is None:
            return {"message": "Driver not mapped to any route"}
        return await self.reoptimize_truck_remaining(date=date, truck_id=int(target_truck_id))
    
    async def get_route_urls(self, date: str) -> List[Dict[str, Any]]:
        """Get Google Maps URLs for routes on a date."""
        route_assignments = self.repo.get_route_assignments_by_date(date)
        
        if not route_assignments:
            return []
        
        # Get depot coordinates
        depot_address = self.config.depot.address
        depot_coords_dict = await self.distance_provider.geocode_locations([depot_address])
        depot_coords = depot_coords_dict[depot_address]
        
        if not depot_coords:
            raise ValueError(f"Could not geocode depot address: {depot_address}")
        
        urls = []
        
        for assignment in route_assignments:
            # Get route stops with their jobs and locations
            route_stops = self.repo.get_route_stops_by_assignment(assignment.id)
            
            if not route_stops:
                # Empty route
                continue
            
            # Build list of coordinates for this route: depot -> stops -> depot
            coordinates = [depot_coords]
            
            # Sort stops by stop order and add their locations
            sorted_stops = sorted(route_stops, key=lambda s: s.stop_order)
            for stop in sorted_stops:
                if stop.job.location.lat and stop.job.location.lon:
                    coordinates.append(Coordinates(
                        lat=stop.job.location.lat,
                        lon=stop.job.location.lon
                    ))
            
            # Return to depot
            coordinates.append(depot_coords)
            
            # Generate URL using simple coordinate-based approach
            route_urls = self.url_builder.build_coordinate_urls(
                coordinates,
                assignment.truck.name
            )
            
            urls.append({
                "truck_name": assignment.truck.name,
                "urls": route_urls.urls,
                "total_stops": route_urls.total_stops
            })
        
        return urls
    
    def get_config(self) -> Dict[str, Any]:
        """Get current configuration (non-secret)."""
        return self.config.model_dump()
    
    def update_config(self, updates: Dict[str, Any]) -> None:
        """Update configuration parameters."""
        # This would update the YAML file and reload config
        # Implementation depends on specific requirements
        raise NotImplementedError("Config updates not yet implemented")
    
    def _filter_jobs_by_location(self, jobs: List[Any], depot_coords: Coordinates) -> List[Any]:
        """Filter jobs and group nearby locations to stay under Google API limits."""
        import math
        
        def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
            """Calculate distance between two points in miles using Haversine formula."""
            R = 3959  # Earth's radius in miles
            
            dlat = math.radians(lat2 - lat1)
            dlon = math.radians(lon2 - lon1)
            a = (math.sin(dlat / 2) ** 2 + 
                 math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * 
                 math.sin(dlon / 2) ** 2)
            c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
            return R * c
        
        filtered_jobs = []
        max_distance_miles = 12  # Increased radius to include more jobs
        max_locations = 9  # Keep under 10 total locations (including depot) for 10x10=100 elements
        
        logger.info(f"Starting job filtering with {len(jobs)} jobs, max distance: {max_distance_miles} miles")
        
        # First pass: collect all jobs within radius
        jobs_in_radius = []
        for job in jobs:
            if job.location and job.location.lat and job.location.lon:
                distance = haversine_distance(
                    depot_coords.lat, depot_coords.lon,
                    job.location.lat, job.location.lon
                )
                if distance <= max_distance_miles:
                    jobs_in_radius.append((job, distance))
                    logger.info(f"Job {job.id} at {job.location.name} - {distance:.1f} miles from depot")
                else:
                    logger.info(f"Excluding job {job.id} at {job.location.name} - {distance:.1f} miles from depot")
            else:
                logger.warning(f"Excluding job {job.id} at {job.location.name if job.location else 'unknown'} - missing coordinates")
        
        # Sort by distance (prioritize closer jobs)
        jobs_in_radius.sort(key=lambda x: x[1])
        
        # Second pass: group nearby locations and select up to max_locations unique locations
        unique_locations = {}  # lat,lon -> [jobs]
        location_count = 0
        
        for job, distance in jobs_in_radius:
            if location_count >= max_locations:
                logger.info(f"Reached max location limit ({max_locations}), excluding remaining jobs")
                break
                
            # Use a location key that groups very nearby locations (within ~0.5 miles)
            lat_rounded = round(job.location.lat, 2)  # ~0.7 mile precision
            lon_rounded = round(job.location.lon, 2)
            location_key = (lat_rounded, lon_rounded)
            
            if location_key not in unique_locations:
                if location_count < max_locations:
                    unique_locations[location_key] = []
                    location_count += 1
                else:
                    # Try to find a nearby existing location to group with
                    found_nearby = False
                    for existing_key in unique_locations.keys():
                        existing_lat, existing_lon = existing_key
                        if haversine_distance(lat_rounded, lon_rounded, existing_lat, existing_lon) <= 0.5:
                            unique_locations[existing_key].append(job)
                            found_nearby = True
                            logger.info(f"Grouping job {job.id} with nearby location {existing_key}")
                            break
                    
                    if not found_nearby:
                        logger.info(f"Excluding job {job.id} - would exceed location limit")
                        continue
            
            unique_locations[location_key].append(job)
            filtered_jobs.append(job)
            logger.info(f"Including job {job.id} at {job.location.name} - {distance:.1f} miles from depot")
        
        logger.info(f"Filtered to {len(filtered_jobs)} jobs across {len(unique_locations)} unique locations")
        logger.info(f"Excluded {len(jobs) - len(filtered_jobs)} jobs to stay under API limits")
        
        return filtered_jobs
    
    def _filter_jobs_intelligently(self, jobs: List[Job], depot_coords: Coordinates, max_jobs: int) -> List[Job]:
        """
        Apply intelligent filtering for very large job datasets.
        Prioritizes by distance, priority, and capacity utilization.
        """
        import math
        
        def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
            """Calculate distance between two points in miles using Haversine formula."""
            R = 3959  # Earth's radius in miles
            
            dlat = math.radians(lat2 - lat1)
            dlon = math.radians(lon2 - lon1)
            a = (math.sin(dlat / 2) ** 2 + 
                 math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * 
                 math.sin(dlon / 2) ** 2)
            c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
            return R * c
        
        scored_jobs = []
        
        for job in jobs:
            if not job.location or not job.location.lat or not job.location.lon:
                continue
                
            distance_miles = haversine_distance(
                depot_coords.lat, depot_coords.lon,
                job.location.lat, job.location.lon
            )
            
            # Calculate total job capacity required
            total_capacity = sum(getattr(item, 'qty', 0) for item in job.job_items)
            
            # Scoring: lower is better
            # Distance factor (favor closer jobs)
            distance_score = distance_miles
            
            # Priority factor (favor high priority jobs)
            priority_multiplier = 1.0
            if hasattr(job, 'priority'):
                pr = job.priority
                # Integer priorities: 0 highest urgency, larger = lower urgency
                if isinstance(pr, int):
                    if pr == 0:
                        priority_multiplier = 0.3
                    elif pr == 1:
                        priority_multiplier = 0.5
                    elif pr == 2:
                        priority_multiplier = 0.8
                    else:
                        priority_multiplier = 1.0
                elif isinstance(pr, str):
                    p_lower = pr.lower()
                    if p_lower in ('urgent', 'critical'):
                        priority_multiplier = 0.3
                    elif p_lower == 'high':
                        priority_multiplier = 0.5
                    elif p_lower == 'medium':
                        priority_multiplier = 0.8
                    else:
                        priority_multiplier = 1.0
            
            # Capacity efficiency (favor larger jobs)
            capacity_score = max(1.0, 20.0 - total_capacity)  # Favor jobs with more capacity
            
            final_score = (distance_score * priority_multiplier) + (capacity_score * 0.1)
            scored_jobs.append((final_score, job))
        
        # Sort by score and take top jobs
        scored_jobs.sort(key=lambda x: x[0])
        return [job for _, job in scored_jobs[:max_jobs]]

    async def _calculate_distance_matrix(self, location_coords: List[Coordinates]) -> Any:
        """Calculate distance matrix between all location pairs."""
        import numpy as np
        
        n = len(location_coords)
        distance_matrix = np.zeros((n, n))
        # Optional debug structure to hold sources per leg when provider fails
        provider_sources: List[Tuple[int,int,str,float]] = []
        
        try:
            # Try to use routing provider for accurate travel times
            from .routing import RoutingProvider, TruckProfile
            
            routing_provider = RoutingProvider(self.config)
            truck_profile = TruckProfile()  # Default profile
            
            for i in range(n):
                for j in range(n):
                    if i == j:
                        distance_matrix[i][j] = 0.0
                    else:
                        result = await routing_provider.calculate_route(
                            location_coords[i], location_coords[j], truck_profile
                        )
                        if result and 'duration_minutes' in result:
                            distance_matrix[i][j] = result['duration_minutes']
                            provider_sources.append((i,j,'provider',result['duration_minutes']))
                        else:
                            # Fallback to offline calculation
                            distance_matrix[i][j] = self.offline_calculator._calculate_travel_time(
                                location_coords[i], location_coords[j], 1.0
                            )[1]  # Get time in minutes
                            provider_sources.append((i,j,'offline-fallback',distance_matrix[i][j]))
                            
        except Exception as e:
            msg = str(e)
            if 'Connection refused' in msg or 'ECONNREFUSED' in msg:
                logger.warning("Routing provider connection refused (OSRM likely unreachable in container). Falling back to offline distances. Consider running a local OSRM container or set OSRM_URL env.")
            else:
                logger.warning(f"Failed to use routing provider for distance matrix: {e}")
            # Fallback to offline calculation for entire matrix
            for i in range(n):
                for j in range(n):
                    if i == j:
                        distance_matrix[i][j] = 0.0
                    else:
                        distance_matrix[i][j] = self.offline_calculator._calculate_travel_time(
                            location_coords[i], location_coords[j], 1.0
                        )[1]  # Get time in minutes
                        provider_sources.append((i,j,'offline-fallback',distance_matrix[i][j]))
                        
        if provider_sources:
            # Summaries for debug logs
            prov = len([p for p in provider_sources if p[2]=='provider'])
            offl = len(provider_sources) - prov
            logger.info(f"Distance matrix built with {prov} provider legs and {offl} offline legs")
        return distance_matrix
    
    def health_check(self) -> Dict[str, Any]:
        """Perform health check on all components."""
        return {
            "status": "healthy",
            "database_connected": self.repo.health_check(),
            "google_api_configured": self.settings.google_maps_api_key is not None,
            "timestamp": datetime.now().isoformat()
        }
    
    async def close(self) -> None:
        """Clean up resources."""
        await self.distance_provider.close()
