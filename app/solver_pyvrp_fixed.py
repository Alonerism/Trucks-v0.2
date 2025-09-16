"""
PyVRP-based route optimization solver.
Implements priority-based penalty system with Trump 0, P1-P3 levels.
"""

import logging
import time
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple, Any
import numpy as np

import pyvrp
from pyvrp.stop import MaxRuntime

from .models import Truck, Job, Location, ActionType
from .distance import DistanceProvider
from .constraints import ConstraintValidator


logger = logging.getLogger(__name__)


class PyVRPSolver:
    """PyVRP-based vehicle routing solver with priority penalties."""
    
    def __init__(self, config: Any):
        """Initialize solver with configuration."""
        self.config = config
        self.validator = ConstraintValidator(config)
        
        # Priority penalty weights based on slider settings
        self._setup_priority_penalties()
        
    def _setup_priority_penalties(self):
        """Setup priority penalty weights based on balance slider."""
        # Get balance slider value (0=Optimal, 1=Balanced, 2=Priority)
        balance_slider = getattr(self.config.solver, 'balance_slider', 1.0)
        
        # Base penalties for P1, P2, P3 (Trump 0 handled separately)
        if balance_slider <= 0.5:  # Optimal mode
            # Low penalties, solver focuses on cost minimization
            self.p1_penalty = 1000
            self.p2_penalty = 500
            self.p3_penalty = 100
        elif balance_slider <= 1.5:  # Balanced mode
            # Moderate penalties, reasonable priority differentiation
            self.p1_penalty = 5000
            self.p2_penalty = 2000
            self.p3_penalty = 500
        else:  # Priority mode (1.5-2.0)
            # High penalties, strong priority preference
            self.p1_penalty = 20000
            self.p2_penalty = 5000
            self.p3_penalty = 1000
            
        # Trump 0 always gets highest penalty (practically infinite)
        self.trump_penalty = 100000
        
        logger.info(f"Priority penalties - Trump:0:{self.trump_penalty}, P1:{self.p1_penalty}, P2:{self.p2_penalty}, P3:{self.p3_penalty}")

    def _get_priority_penalty(self, priority: int) -> float:
        """Get penalty for given priority level."""
        if priority == 0:  # Trump priority
            return self.trump_penalty
        elif priority == 1:
            return self.p1_penalty
        elif priority == 2:
            return self.p2_penalty
        elif priority == 3:
            return self.p3_penalty
        else:
            return self.p3_penalty  # Default to lowest priority

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
        
        try:
            # Create problem data
            data = self._create_problem_data(
                trucks, jobs, distance_matrix, service_times, locations
            )
            
            # Set up solve parameters with time limit
            params = pyvrp.SolveParams()
            
            # Create stop criterion
            stop = MaxRuntime(float(time_limit_seconds))
            
            # Solve the problem
            logger.info(f"Starting PyVRP optimization with {len(jobs)} jobs and {len(trucks)} trucks")
            result = pyvrp.solve(data, stop=stop, seed=42, params=params)
            
            # Parse solution
            solution = self._parse_pyvrp_solution(
                result, trucks, jobs, locations
            )
            
            elapsed_time = time.time() - start_time
            logger.info(f"PyVRP optimization completed in {elapsed_time:.2f} seconds")
            
            return solution
            
        except Exception as e:
            logger.error(f"PyVRP optimization failed: {str(e)}")
            # Return empty solution on failure
            return {
                "routes": [],
                "unassigned_jobs": [job.id for job in jobs],
                "total_distance": 0,
                "total_time": 0,
                "solver": "PyVRP",
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
        """Create PyVRP problem data from input parameters."""
        
        # Create depot (index 0)
        depot = pyvrp.Depot(
            x=locations[0].latitude,
            y=locations[0].longitude
        )
        
        # Create clients for each job with priority penalties
        clients = []
        for i, job in enumerate(jobs):
            # Get job location (job locations start at index 1)
            job_location = locations[job.location_id]
            
            # Calculate priority penalty
            priority_penalty = self._get_priority_penalty(job.priority)
            
            # Create client with penalty as prize (negative for penalty)
            client = pyvrp.Client(
                x=job_location.latitude,
                y=job_location.longitude,
                delivery=1,  # Each job is a delivery
                service_duration=int(service_times[i+1] * 60),  # Convert to seconds
                prize=-priority_penalty  # Negative penalty as prize
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
        
        # Convert distance matrix to integer seconds (PyVRP expects int)
        # distance_matrix is in minutes, convert to seconds
        distance_matrix_seconds = (distance_matrix * 60).astype(int)
        duration_matrix_seconds = distance_matrix_seconds.copy()
        
        # Create problem data
        data = pyvrp.ProblemData(
            clients=clients,
            depots=[depot],
            vehicle_types=vehicle_types,
            distance_matrix=distance_matrix_seconds,
            duration_matrix=duration_matrix_seconds
        )
        
        return data

    def _parse_pyvrp_solution(
        self,
        result: pyvrp.Result,
        trucks: List[Truck],
        jobs: List[Job],
        locations: List[Location]
    ) -> Dict[str, Any]:
        """Parse PyVRP result into standard solution format."""
        
        routes = []
        assigned_job_ids = set()
        total_distance = 0
        total_time = 0
        
        if result.is_feasible():
            solution = result.best  # Get solution (not callable)
            
            for route_idx, route in enumerate(solution.routes()):
                if route_idx >= len(trucks):
                    break  # Skip routes beyond available trucks
                    
                truck = trucks[route_idx]
                route_jobs = []
                route_distance = route.distance() / 60  # Convert from seconds to minutes
                route_duration = route.duration() / 60  # Convert from seconds to minutes
                
                # Process route visits (excluding depot visits)
                for visit in route.visits():
                    client_id = visit.client()
                    if client_id < len(jobs):  # Valid job index
                        job = jobs[client_id]
                        job_location = locations[job.location_id]
                        
                        route_jobs.append({
                            "job_id": job.id,
                            "location": {
                                "latitude": job_location.latitude,
                                "longitude": job_location.longitude,
                                "address": job_location.address
                            },
                            "action_type": job.action_type.value,
                            "priority": job.priority,
                            "service_time": visit.service_duration() / 60  # Convert to minutes
                        })
                        assigned_job_ids.add(job.id)
                
                if route_jobs:  # Only include routes with jobs
                    routes.append({
                        "truck_id": truck.id,
                        "truck_name": truck.name,
                        "jobs": route_jobs,
                        "total_distance": route_distance,
                        "total_time": route_duration
                    })
                    
                    total_distance += route_distance
                    total_time += route_duration
        
        # Find unassigned jobs
        unassigned_jobs = [job.id for job in jobs if job.id not in assigned_job_ids]
        
        return {
            "routes": routes,
            "unassigned_jobs": unassigned_jobs,
            "total_distance": total_distance,
            "total_time": total_time,
            "solver": "PyVRP",
            "status": "optimal" if result.is_feasible() else "infeasible"
        }
