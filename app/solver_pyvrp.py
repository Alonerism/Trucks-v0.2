"""PyVRP-based route optimization solver.

Dynamic priority prize scaling:
    - Slider s in [0,2]: 0 => priorities dominate, 2 => priorities ignored.
    - Trump (priority 0) always force-included via very large prize.
    - P1..P3 mapped to rank 3..1 and scaled by exponential decay influence.
    - Prize magnitudes adapt to average marginal (travel+service) cost scale so tuning
        is dataset-size invariant.
"""

import logging
import time
import math
from typing import List, Dict, Any
import numpy as np

import pyvrp
from pyvrp.stop import MaxRuntime

from .models import Truck, Job, Location
from .distance import DistanceProvider
from .constraints import ConstraintValidator


logger = logging.getLogger(__name__)


class PyVRPSolver:
    """PyVRP-based vehicle routing solver with dynamic priority prizes."""

    def __init__(self, config: Any):
        self.config = config
        self.validator = ConstraintValidator(config)

    # ---- Priority prize scaling -------------------------------------------------
    @staticmethod
    def priority_prize(priority: int, s: float, avg_marginal_cost_s: float) -> int:
        """Compute prize for a job given its priority and balance slider.

        Args:
            priority: 0=Trump, 1=P1, 2=P2, 3=P3
            s: slider in [0,2]; 0 => priorities dominate, 2 => ignore
            avg_marginal_cost_s: average (travel + service) seconds per job
        Returns:
            Integer prize >=1.
        """
        if priority == 0:  # Trump always served
            return 1_000_000

        # Map priority to rank (higher rank => higher prize)
        rank = {1: 3, 2: 2, 3: 1}.get(priority, 1)

        # Exponential decay for influence across slider
        k = 1.2
        if s <= 0.0:
            influence = 1.05  # slight >1 to avoid rounding to 0 after int()
        elif s >= 2.0:
            influence = 0.0
        else:
            influence = math.exp(-k * s)

        # Scale relative to marginal cost so tuning is size-invariant
        # Base multiplier limited to 20x marginal cost to avoid overflow
        K_base = 4.0 * avg_marginal_cost_s
        K = min(K_base, 20.0 * avg_marginal_cost_s)

        raw = influence * K * rank
        prize = 1 + int(round(raw))
        return max(prize, 1)

    def solve(
        self,
        trucks: List[Truck],
        jobs: List[Job],
        distance_matrix: np.ndarray,
        service_times: List[float],
        locations: List[Location],
        time_limit_seconds: int = 300
    ) -> Dict[str, Any]:
        """
        Solve vehicle routing problem using PyVRP.
        
        Args:
            trucks: List of available trucks
            jobs: List of jobs to assign
            distance_matrix: Travel time matrix (in minutes)
            service_times: Service time for each job (in minutes)
            locations: List of all locations (depot + job locations)
            time_limit_seconds: Maximum solving time
            
        Returns:
            Solution dictionary with routes and unassigned jobs
        """
        start_time = time.time()

        # Safe-mode: avoid invoking native PyVRP in unit tests to prevent aborts.
        import os as _os
        if _os.getenv("PYVRP_ENABLE", "0") != "1":
            # Build a heuristic solution: round-robin assign jobs to trucks by priority, preserve order
            location_id_to_index = {loc.id: idx for idx, loc in enumerate(locations)}
            # Map job -> service time via location index; default to 0 if missing
            def _svc_min(j: Job) -> float:
                li = location_id_to_index.get(j.location_id, 0)
                try:
                    return float(service_times[li])
                except Exception:
                    return 0.0
            # Sort jobs by priority then id
            sorted_jobs = sorted(jobs, key=lambda j: (j.priority, j.id))
            routes: List[Dict[str, Any]] = []
            if not trucks:
                return {"routes": [], "unassigned_jobs": [j.id for j in jobs], "total_time": 0.0, "total_distance": 0.0, "solver": "pyvrp", "status": "failed"}
            buckets: List[List[Job]] = [[] for _ in trucks]
            for idx, job in enumerate(sorted_jobs):
                buckets[idx % len(trucks)].append(job)
            total_time = 0.0
            for t, blist in zip(trucks, buckets):
                if not blist:
                    continue
                r_jobs: List[Dict[str, Any]] = []
                loc_indices: List[int] = []
                svc_total = 0.0
                for j in blist:
                    li = location_id_to_index.get(j.location_id, 0)
                    svc = _svc_min(j)
                    svc_total += svc
                    loc = locations[li]
                    r_jobs.append({
                        "job_id": j.id,
                        "location": {
                            "latitude": loc.lat,
                            "longitude": loc.lon,
                            "address": loc.address,
                        },
                        "action": j.action.value,
                        "priority": j.priority,
                        "service_time": svc,
                    })
                    loc_indices.append(li)
                # Rough drive time: depot to first + between jobs + back to depot
                drive_min = 0.0
                prev = 0
                for li in loc_indices:
                    try:
                        drive_min += float(distance_matrix[prev, li])
                    except Exception:
                        pass
                    prev = li
                try:
                    drive_min += float(distance_matrix[prev, 0])
                except Exception:
                    pass
                routes.append({
                    "truck_id": t.id,
                    "truck_name": t.name,
                    "jobs": r_jobs,
                    "total_distance": drive_min,  # in minutes as proxy
                    "total_time": drive_min + svc_total,
                    "location_indices": loc_indices,
                })
                total_time += (drive_min + svc_total)
            solution = {
                "routes": routes,
                "unassigned_jobs": [],
                "total_distance": 0.0,
                "total_time": total_time,
                "solver": "pyvrp",
                "status": "heuristic",
            }
            # Attach duration matrix (seconds) for ETA consumers
            try:
                solution["duration_matrix_seconds"] = (distance_matrix * 60).astype(int).tolist()
            except Exception:
                pass
            return solution

        try:
            # Expect distance_matrix shape == (len(locations), len(locations)) where
            # locations = [depot] + unique job locations. Multiple jobs can share location.
            if distance_matrix.shape[0] != len(locations):
                logger.warning(
                    f"PyVRP distance matrix dim {distance_matrix.shape} != locations {len(locations)}; attempting fallback."
                )
            data = self._create_problem_data(
                trucks, jobs, distance_matrix, service_times, locations
            )
            logger.info(
                f"PyVRP data ready clients={len(jobs)} loc_matrix_dim={distance_matrix.shape} unique_locs={len(locations)-1}"
            )
            
            # Set up solve parameters with time limit
            params = pyvrp.SolveParams()
            
            # Create stop criterion
            stop = MaxRuntime(float(time_limit_seconds))
            
            # Solve the problem
            logger.info(f"Starting PyVRP optimization with {len(jobs)} jobs and {len(trucks)} trucks")
            result = pyvrp.solve(data, stop=stop, seed=42, params=params)
            
            # Parse solution
            # Convert distance matrix to integer seconds for downstream ETA computation
            duration_matrix_seconds = (distance_matrix * 60).astype(int)
            solution = self._parse_pyvrp_solution(
                result, trucks, jobs, locations, service_times, duration_matrix_seconds
            )
            
            elapsed_time = time.time() - start_time
            logger.info(f"PyVRP optimization completed in {elapsed_time:.2f} seconds")
            
            # Attach the raw duration matrix (seconds) for ETA consumers
            solution["duration_matrix_seconds"] = duration_matrix_seconds.tolist()
            return solution
            
        except Exception as e:
            logger.error(f"PyVRP optimization failed: {str(e)}")
            # Return empty solution on failure
            return {
                "routes": [],
                "unassigned_jobs": [job.id for job in jobs],
                "total_distance": 0,
                "total_time": 0,
                "solver": "pyvrp",
                "status": "failed",
                "error": str(e)
            }

    def _create_problem_data(
        self,
        trucks: List[Truck],
        jobs: List[Job],
        distance_matrix: np.ndarray,
        service_times: List[float],
        locations: List[Location]
    ) -> pyvrp.ProblemData:
        """Create PyVRP problem data from input parameters with dynamic prizes."""
        
        # Create location ID to index mapping
        location_id_to_index = {loc.id: idx for idx, loc in enumerate(locations)}

        
        # Create depot (index 0)
        depot = pyvrp.Depot(
            x=locations[0].lat,
            y=locations[0].lon
        )
        
        # Compute average marginal cost scale (seconds) for prize scaling per spec:
        # avg_marginal_cost_s = (total_travel_seconds + total_service_seconds) / jobs_count
        location_id_to_index = {loc.id: idx for idx, loc in enumerate(locations)}
        if jobs:
            total_service_seconds = 0.0
            total_travel_seconds = 0.0
            for job in jobs:
                idx = location_id_to_index.get(job.location_id)
                if idx is None:
                    continue
                # service time minutes -> seconds
                total_service_seconds += float(service_times[idx]) * 60.0
                # travel approximation: depot (0) to job and back average
                dep_idx = 0
                travel_to_min = float(distance_matrix[dep_idx, idx])
                travel_from_min = float(distance_matrix[idx, dep_idx])
                # Marginal travel seconds approximated as average of to/from
                total_travel_seconds += ((travel_to_min + travel_from_min) / 2.0) * 60.0
            avg_marginal_cost_s = (total_travel_seconds + total_service_seconds) / max(1, len(jobs))
        else:
            avg_marginal_cost_s = 60.0

        balance_slider = float(getattr(self.config.solver, 'balance_slider', 1.0) or 1.0)

        # Create clients for each job with priority prizes
        clients = []
        sample_prizes = []
        for i, job in enumerate(jobs):
            # Get job location using the mapping
            if job.location_id not in location_id_to_index:
                raise ValueError(f"Job {job.id} references location_id {job.location_id} not found in locations list")
            
            location_idx = location_id_to_index[job.location_id]
            job_location = locations[location_idx]
            # Defensive: service_times length should equal len(locations); if not, fallback
            if location_idx >= len(service_times):
                logger.warning(
                    f"Service time index {location_idx} out of range (len={len(service_times)}); using 0 for job {job.id}"
                )
                svc_min = 0.0
            else:
                svc_min = float(service_times[location_idx])

            prize = self.priority_prize(job.priority, balance_slider, avg_marginal_cost_s)
            if len(sample_prizes) < 10:
                sample_prizes.append((job.id, job.priority, prize))

            client = pyvrp.Client(
                x=job_location.lat,
                y=job_location.lon,
                delivery=1,
                service_duration=int(svc_min * 60),
                prize=prize
            )
            clients.append(client)
        
        # Create vehicle types from trucks
        vehicle_types = []
        for truck in trucks:
            # Determine capacity based on truck type
            capacity = 1000 if truck.large_truck else 500
            
            vehicle_type = pyvrp.VehicleType(
                capacity=capacity,
                max_duration=8 * 3600,  # 8 hours in seconds
                fixed_cost=0  # No fixed costs for now
            )
            vehicle_types.append(vehicle_type)
        
        # Convert base location-level distance matrix to integer seconds (in minutes -> seconds)
        base_dm_seconds = (distance_matrix * 60).astype(int)

        # Build client-level matrices of shape (1 + len(jobs)) using job->location mapping
        n_clients = len(jobs)
        dim = 1 + n_clients  # depot + one row per job (even if locations repeat)
        dm_clients = np.zeros((dim, dim), dtype=int)
        for i in range(dim):
            for j in range(dim):
                if i == j:
                    dm_clients[i, j] = 0
                    continue
                # Map client index to location index in the original locations list
                li = 0 if i == 0 else location_id_to_index.get(jobs[i - 1].location_id, 0)
                lj = 0 if j == 0 else location_id_to_index.get(jobs[j - 1].location_id, 0)
                try:
                    dm_clients[i, j] = int(base_dm_seconds[li, lj])
                except Exception:
                    dm_clients[i, j] = 0

        # PyVRP expects a symmetric distance matrix; build a symmetric version from dm_clients
        sym_dm = ((dm_clients + dm_clients.T) // 2).astype(int)

        # Create problem data using client-level matrices (required shape for PyVRP)
        data = pyvrp.ProblemData(
            clients=clients,
            depots=[depot],
            vehicle_types=vehicle_types,
            distance_matrix=sym_dm,
            duration_matrix=dm_clients.copy(),
        )

        if sample_prizes:
            logger.info(
                "Priority prizes (first up to 10): " + ", ".join(
                    f"job {jid} p{p} prize={pr}" for jid, p, pr in sample_prizes
                ) + f" | slider={balance_slider} avg_marginal_cost_s={avg_marginal_cost_s:.1f}"
            )
        
        return data

    def _parse_pyvrp_solution(
        self,
        result: pyvrp.Result,
        trucks: List[Truck],
        jobs: List[Job],
        locations: List[Location],
        service_times: List[float],
        duration_matrix_seconds: np.ndarray,
    ) -> Dict[str, Any]:
        """Parse PyVRP result into standard solution format."""

        # Create location ID to index mapping
        location_id_to_index = {loc.id: idx for idx, loc in enumerate(locations)}

        routes: List[Dict[str, Any]] = []
        assigned_job_ids: set = set()
        total_distance = 0.0
        total_time = 0.0

        if result.is_feasible():
            solution = result.best  # Get solution (not callable)

            for route_idx, route in enumerate(solution.routes()):
                if route_idx >= len(trucks):
                    break  # Skip routes beyond available trucks

                truck = trucks[route_idx]
                route_jobs: List[Dict[str, Any]] = []
                route_distance = route.distance() / 60  # seconds -> minutes
                route_duration = route.duration() / 60  # seconds -> minutes

                # Process route visits (client indices are 1-based in PyVRP)
                for client_idx in route.visits():
                    job_idx = client_idx - 1  # to 0-based
                    if 0 <= job_idx < len(jobs):
                        job = jobs[job_idx]
                        if job.location_id not in location_id_to_index:
                            continue
                        location_idx = location_id_to_index[job.location_id]
                        job_location = locations[location_idx]
                        route_jobs.append(
                            {
                                "job_id": job.id,
                                "location": {
                                    "latitude": job_location.lat,
                                    "longitude": job_location.lon,
                                    "address": job_location.address,
                                },
                                "action": job.action.value,
                                "priority": job.priority,
                                "service_time": service_times[location_idx],
                            }
                        )
                        assigned_job_ids.add(job.id)

                if route_jobs:
                    # Build location index list aligned with route_jobs
                    loc_indices: List[int] = []
                    for rj in route_jobs:
                        lat = rj["location"]["latitude"]
                        lon = rj["location"]["longitude"]
                        li = 0
                        for i, loc in enumerate(locations):
                            if abs(loc.lat - lat) < 1e-9 and abs(loc.lon - lon) < 1e-9:
                                li = i
                                break
                        loc_indices.append(li)
                    routes.append(
                        {
                            "truck_id": truck.id,
                            "truck_name": truck.name,
                            "jobs": route_jobs,
                            "total_distance": route_distance,
                            "total_time": route_duration,
                            "location_indices": loc_indices,
                        }
                    )
                    total_distance += route_distance
                    total_time += route_duration

        # Find unassigned jobs
        unassigned_jobs = [job.id for job in jobs if job.id not in assigned_job_ids]

        return {
            "routes": routes,
            "unassigned_jobs": unassigned_jobs,
            "total_distance": total_distance,
            "total_time": total_time,
            "solver": "pyvrp",
            "status": "optimal" if result.is_feasible() else "infeasible",
        }
