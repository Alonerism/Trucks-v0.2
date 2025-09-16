"""
PyVRP-based route optimization solver.
Implements priority-based penalty system with Trump 0, P1-P3 le            # Create stop criterion using the correct import
            from pyvrp.stop import MaxRuntime
            stop = MaxRuntime(float(time_limit_seconds))
            
            # Solve the problem
            logger.info(f"Starting PyVRP optimization with {len(jobs)} jobs and {len(trucks)} trucks")
            result = pyvrp.solve(data, stop=stop, seed=42, params=params)t logging
import time
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple, Any
import numpy as np

import pyvrp

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
            
            # Create stop criterion using the correct import
            from pyvrp.stop import MaxRuntime
            stop = MaxRuntime(float(time_limit_seconds))
                    params.time_limit = time_limit_seconds
                    stop = None
                else:
                    # Use simple iteration limit as fallback
                    stop = pyvrp.MaxIterations(1000)
            
            # Solve the problem
            logger.info(f"Starting PyVRP optimization with {len(jobs)} jobs and {len(trucks)} trucks")
            
            if stop is not None:
                result = pyvrp.solve(data, stop=stop, seed=42, params=params)
            else:
                result = pyvrp.solve(data, seed=42, params=params)
            
            # Parse solution
            solution = self._parse_pyvrp_solution(
                result, trucks, jobs, locations, distance_matrix, service_times
            )
            
            solution['computation_time_seconds'] = time.time() - start_time
            solution['solver_used'] = 'pyvrp'
            
            logger.info(f"PyVRP optimization completed in {solution['computation_time_seconds']:.2f}s")
            return solution
            
        except Exception as e:
            logger.error(f"PyVRP solver failed: {e}")
            return self._create_empty_result(trucks, jobs, time.time() - start_time)
            
    def _create_problem_data(
        self,
        trucks: List[Truck],
        jobs: List[Job],
        distance_matrix: np.ndarray,
        service_times: List[float],
        locations: List[Location]
    ) -> pyvrp.ProblemData:
        """Create PyVRP problem data from input."""
        
        # PyVRP expects distance matrix to match: 1 depot + number of jobs
        # But our distance_matrix is based on unique locations
        # We need to expand it to map each job to the matrix
        
        # Create mapping from job location to matrix index
        location_to_matrix_idx = {}
        for i, location in enumerate(locations):
            location_to_matrix_idx[location.id] = i
            
        # Create expanded distance matrix: depot + each job
        n_nodes = 1 + len(jobs)  # depot + jobs
        expanded_matrix = np.zeros((n_nodes, n_nodes), dtype=int)
        
        # Fill depot row/column (index 0)
        for job_idx, job in enumerate(jobs):
            matrix_idx = location_to_matrix_idx[job.location_id]
            # Depot to job
            expanded_matrix[0][job_idx + 1] = int(distance_matrix[0][matrix_idx] * 100)
            # Job to depot  
            expanded_matrix[job_idx + 1][0] = int(distance_matrix[matrix_idx][0] * 100)
            
        # Fill job-to-job entries
        for i, job_i in enumerate(jobs):
            matrix_idx_i = location_to_matrix_idx[job_i.location_id]
            for j, job_j in enumerate(jobs):
                if i == j:
                    expanded_matrix[i + 1][j + 1] = 0
                else:
                    matrix_idx_j = location_to_matrix_idx[job_j.location_id]
                    expanded_matrix[i + 1][j + 1] = int(distance_matrix[matrix_idx_i][matrix_idx_j] * 100)
        
        # Debug logging
        logger.info(f"Original distance matrix shape: {distance_matrix.shape}")
        logger.info(f"Expanded distance matrix shape: {expanded_matrix.shape}")
        logger.info(f"Number of jobs: {len(jobs)}")
        logger.info(f"Number of locations: {len(locations)}")
        
        # Create client data (jobs)
        clients = []
        clients = []
        clients = []
        for i, job in enumerate(jobs):
            demand = max(1, int(self._calculate_job_weight(job) * 0.453592))  # lb to kg
            penalty = int(self._get_priority_penalty(job.priority))
            
            # Time windows (convert to seconds)
            tw_start, tw_end = self._get_job_time_window(job)
            service_duration = int(service_times[i+1] * 60)  # minutes to seconds
            
            client = pyvrp.Client(
                x=0,  # Using distance matrix, coordinates don't matter
                y=0,
                delivery=demand,  # Changed from demand to delivery
                service_duration=service_duration,
                tw_early=tw_start,
                tw_late=tw_end,
                prize=penalty  # Changed from penalty to prize
            )
            clients.append(client)
            
        # Create depot
        depot = pyvrp.Depot(x=0, y=0)
        
        # Create vehicle types
        vehicle_types = []
        single_truck_mode = getattr(self.config.solver, 'single_truck_mode', 0)
        
        for truck in trucks:
            capacity = max(1, int(truck.max_weight_lb * 0.453592))  # lb to kg
            max_duration = int(8 * 60 * 60)  # 8 hours in seconds
            
            vehicle_type = pyvrp.VehicleType(
                capacity=capacity,
                depot=0,  # Changed from start_depot/end_depot to single depot
                num_available=1,
                max_duration=max_duration,
                fixed_cost=1  # Changed from unit_cost to fixed_cost
            )
            vehicle_types.append(vehicle_type)
            
            if single_truck_mode:
                break
                
        # Create problem data - use expanded matrix
        data = pyvrp.ProblemData(
            clients=clients,
            depots=[depot],
            vehicle_types=vehicle_types,
            distance_matrix=expanded_matrix,  # Use expanded matrix
            duration_matrix=expanded_matrix   # Use expanded matrix
        )
        
        return data
        
    def _calculate_job_weight(self, job: Job) -> float:
        """Calculate total weight for a job based on items."""
        total_weight = 0.0
        
        for job_item in job.job_items:
            item_weight = job_item.item.weight_lb_per_unit * job_item.qty
            total_weight += item_weight
            
        return max(1.0, total_weight)  # Minimum 1 lb
        
    def _get_job_time_window(self, job: Job) -> Tuple[int, int]:
        """Get time window for job in seconds from midnight."""
        # Default: 8 AM to 6 PM
        default_start = 8 * 3600  # 8 AM in seconds
        default_end = 18 * 3600   # 6 PM in seconds
        
        if job.earliest:
            start_dt = job.earliest
            start_seconds = start_dt.hour * 3600 + start_dt.minute * 60 + start_dt.second
        else:
            start_seconds = default_start
            
        if job.latest:
            end_dt = job.latest
            end_seconds = end_dt.hour * 3600 + end_dt.minute * 60 + end_dt.second
        else:
            end_seconds = default_end
            
        # Apply truck restrictions for large truck jobs
        if hasattr(job, 'requires_large_truck') and job.requires_large_truck:
            # Check if location is in restricted area (Santa Monica)
            restricted_start = self._check_restricted_area_time_window(job)
            if restricted_start:
                start_seconds = max(start_seconds, restricted_start)
                
        return start_seconds, end_seconds
        
    def _check_restricted_area_time_window(self, job: Job) -> Optional[int]:
        """Check if job location has truck restrictions."""
        try:
            # Santa Monica restriction: large trucks cannot enter before 8 AM
            # This is a simplified check - in production, would use geofencing
            if 'santa monica' in job.location.name.lower():
                return 8 * 3600  # 8 AM in seconds
                
            return None
            
        except Exception as e:
            logger.warning(f"Could not check truck restrictions for job {job.id}: {e}")
            return None
            
    def _parse_pyvrp_solution(
        self,
        result: Any,
        trucks: List[Truck],
        jobs: List[Job],
        locations: List[Location],
        distance_matrix: np.ndarray,
        service_times: List[float]
    ) -> Dict[str, Any]:
        """Parse PyVRP solution into standard format."""
        routes = []
        assigned_job_ids = set()
        
        # Get best solution
        solution = result.best
        
        # Parse routes
        for route_idx, route in enumerate(solution.routes()):
            if route_idx >= len(trucks):
                break  # More routes than trucks
                
            truck = trucks[route_idx]
            route_jobs = []
            
            # Calculate route timing
            current_time = 8 * 60  # Start at 8 AM (minutes)
            total_drive_time = 0.0
            total_service_time = 0.0
            
            # Get route visits (excluding depot)
            visits = [visit for visit in route.visits() if visit != 0]
            
            for i, customer_idx in enumerate(visits):
                job_idx = customer_idx - 1  # Adjust for depot offset
                if job_idx >= len(jobs):
                    continue
                    
                job = jobs[job_idx]
                assigned_job_ids.add(job.id)
                
                # Calculate travel time from previous location
                prev_location_idx = visits[i-1] if i > 0 else 0  # Previous location or depot
                travel_time = distance_matrix[prev_location_idx][customer_idx]
                
                current_time += travel_time
                total_drive_time += travel_time
                
                # Service time
                service_time = service_times[customer_idx]
                total_service_time += service_time
                
                # Create route stop
                route_stop = {
                    'job_id': job.id,
                    'location_name': job.location.name,
                    'action': job.action.value,
                    'priority': job.priority,
                    'estimated_arrival': current_time,
                    'service_minutes': service_time,
                    'drive_minutes_from_previous': travel_time
                }
                
                route_jobs.append(route_stop)
                current_time += service_time
                
            if route_jobs:  # Only add routes with jobs
                total_minutes = total_drive_time + total_service_time
                overtime_minutes = max(0, total_minutes - 480)  # 8 hours = 480 minutes
                
                route_data = {
                    'truck_name': truck.name,
                    'truck_id': truck.id,
                    'ordered_stops': route_jobs,
                    'drive_minutes': total_drive_time,
                    'service_minutes': total_service_time,
                    'total_minutes': total_minutes,
                    'overtime_minutes': overtime_minutes
                }
                routes.append(route_data)
                
        # Find unassigned jobs
        unassigned_jobs = []
        for job in jobs:
            if job.id not in assigned_job_ids:
                unassigned_jobs.append({
                    'id': job.id,
                    'location_name': job.location.name,
                    'action': job.action.value,
                    'priority': job.priority,
                    'reason': f'Unassigned (priority {job.priority})'
                })
                
        return {
            'routes': routes,
            'unassigned_jobs': unassigned_jobs,
            'total_cost': float(result.cost()),
            'objective_breakdown': {
                'total_cost': float(result.cost()),
                'num_routes': len(routes),
                'num_assigned': len(assigned_job_ids),
                'num_unassigned': len(unassigned_jobs)
            }
        }
        
    def _create_empty_result(self, trucks: List[Truck], jobs: List[Job], computation_time: float) -> Dict[str, Any]:
        """Create empty result when solver fails."""
        return {
            'routes': [],
            'unassigned_jobs': [
                {
                    'id': job.id,
                    'location_name': job.location.name,
                    'action': job.action.value,
                    'priority': job.priority,
                    'reason': 'Solver failed'
                }
                for job in jobs
            ],
            'total_cost': 0.0,
            'computation_time_seconds': computation_time,
            'solver_used': 'pyvrp',
            'objective_breakdown': {
                'total_cost': 0.0,
                'num_routes': 0,
                'num_assigned': 0,
                'num_unassigned': len(jobs)
            }
        }
            
    def _calculate_job_weight(self, job: Job) -> float:
        """Calculate total weight for a job based on items."""
        total_weight = 0.0
        
        for job_item in job.job_items:
            item_weight = job_item.item.weight_lb_per_unit * job_item.qty
            total_weight += item_weight
            
        return max(1.0, total_weight)  # Minimum 1 lb
        
    def _get_job_time_window(self, job: Job) -> Tuple[int, int]:
        """Get time window for job in seconds from midnight."""
        # Default: 8 AM to 6 PM
        default_start = 8 * 3600  # 8 AM in seconds
        default_end = 18 * 3600   # 6 PM in seconds
        
        if job.earliest:
            start_dt = job.earliest
            start_seconds = start_dt.hour * 3600 + start_dt.minute * 60 + start_dt.second
        else:
            start_seconds = default_start
            
        if job.latest:
            end_dt = job.latest
            end_seconds = end_dt.hour * 3600 + end_dt.minute * 60 + end_dt.second
        else:
            end_seconds = default_end
            
        # Apply truck restrictions for large truck jobs
        if hasattr(job, 'requires_large_truck') and job.requires_large_truck:
            # Check if location is in restricted area (Santa Monica)
            restricted_start = self._check_restricted_area_time_window(job)
            if restricted_start:
                start_seconds = max(start_seconds, restricted_start)
                
        return start_seconds, end_seconds
        
    def _check_restricted_area_time_window(self, job: Job) -> Optional[int]:
        """Check if job location has truck restrictions."""
        try:
            # Load city rules
            city_rules_path = self.config.city_rules_file if hasattr(self.config, 'city_rules_file') else 'config/city_rules.yaml'
            
            # Santa Monica restriction: large trucks cannot enter before 8 AM
            # This is a simplified check - in production, would use geofencing
            if 'santa monica' in job.location.name.lower():
                return 8 * 3600  # 8 AM in seconds
                
            return None
            
        except Exception as e:
            logger.warning(f"Could not check truck restrictions for job {job.id}: {e}")
            return None
            
    def _parse_pyvrp_solution(
        self,
        result: Any,
        trucks: List[Truck],
        jobs: List[Job],
        locations: List[Location],
        distance_matrix: np.ndarray,
        service_times: List[float]
    ) -> Dict[str, Any]:
        """Parse PyVRP solution into standard format."""
        routes = []
        assigned_job_ids = set()
        
        # Parse routes
        for route_idx, route in enumerate(result.routes()):
            if route_idx >= len(trucks):
                break  # More routes than trucks
                
            truck = trucks[route_idx]
            route_jobs = []
            
            # Calculate route timing
            current_time = 8 * 60  # Start at 8 AM (minutes)
            total_drive_time = 0.0
            total_service_time = 0.0
            
            for i, customer_idx in enumerate(route):
                if customer_idx == 0:  # Skip depot
                    continue
                    
                job_idx = customer_idx - 1  # Adjust for depot offset
                if job_idx >= len(jobs):
                    continue
                    
                job = jobs[job_idx]
                assigned_job_ids.add(job.id)
                
                # Calculate travel time from previous location
                prev_location_idx = route[i-1] if i > 0 else 0  # Previous location or depot
                travel_time = distance_matrix[prev_location_idx][customer_idx]
                
                current_time += travel_time
                total_drive_time += travel_time
                
                # Service time
                service_time = service_times[customer_idx]
                total_service_time += service_time
                
                # Create route stop
                route_stop = {
                    'job_id': job.id,
                    'location_name': job.location.name,
                    'action': job.action.value,
                    'priority': job.priority,
                    'estimated_arrival': current_time,
                    'service_minutes': service_time,
                    'drive_minutes_from_previous': travel_time
                }
                
                route_jobs.append(route_stop)
                current_time += service_time
                
            if route_jobs:  # Only add routes with jobs
                total_minutes = total_drive_time + total_service_time
                overtime_minutes = max(0, total_minutes - 480)  # 8 hours = 480 minutes
                
                route_data = {
                    'truck_name': truck.name,
                    'truck_id': truck.id,
                    'ordered_stops': route_jobs,
                    'drive_minutes': total_drive_time,
                    'service_minutes': total_service_time,
                    'total_minutes': total_minutes,
                    'overtime_minutes': overtime_minutes
                }
                routes.append(route_data)
                
        # Find unassigned jobs
        unassigned_jobs = []
        for job in jobs:
            if job.id not in assigned_job_ids:
                unassigned_jobs.append({
                    'id': job.id,
                    'location_name': job.location.name,
                    'action': job.action.value,
                    'priority': job.priority,
                    'reason': f'Unassigned (priority {job.priority})'
                })
                
        return {
            'routes': routes,
            'unassigned_jobs': unassigned_jobs,
            'total_cost': float(result.cost()),
            'objective_breakdown': {
                'total_cost': float(result.cost()),
                'num_routes': len(routes),
                'num_assigned': len(assigned_job_ids),
                'num_unassigned': len(unassigned_jobs)
            }
        }
        
    def _create_empty_result(self, trucks: List[Truck], jobs: List[Job], computation_time: float) -> Dict[str, Any]:
        """Create empty result when solver fails."""
        return {
            'routes': [],
            'unassigned_jobs': [
                {
                    'id': job.id,
                    'location_name': job.location.name,
                    'action': job.action.value,
                    'priority': job.priority,
                    'reason': 'Solver failed'
                }
                for job in jobs
            ],
            'total_cost': 0.0,
            'computation_time_seconds': computation_time,
            'solver_used': 'pyvrp',
            'objective_breakdown': {
                'total_cost': 0.0,
                'num_routes': 0,
                'num_assigned': 0,
                'num_unassigned': len(jobs)
            }
        }
