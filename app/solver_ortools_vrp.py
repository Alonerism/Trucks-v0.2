"""
OR-Tools Vehicle Routing Problem (VRP) solver for truck optimization.
Implements capacitated VRP with time windows, priorities, and constraints.
Scales to 50-100+ jobs without Google API limits.
"""

import logging
from datetime import datetime, timedelta
from typing import List, Dict, Tuple, Optional, Any
from dataclasses import dataclass

try:
    from ortools.constraint_solver import routing_enums_pb2
    from ortools.constraint_solver import pywrapcp
    ORTOOLS_AVAILABLE = True
except ImportError:
    ORTOOLS_AVAILABLE = False
    logger = logging.getLogger(__name__)
    logger.warning("OR-Tools not available. Install with: poetry install --extras ortools")

from .models import Truck, Job, JobItem, Location
from .distance import RouteMatrix, Coordinates
from .distance_offline import OfflineDistanceCalculator
from .constraints import ConstraintValidator, LoadInfo, ConstraintViolation
from .schemas import AppConfig
# Define lightweight data classes locally to avoid circular imports with greedy solver
@dataclass
class JobAssignment:
    job: Job
    job_items: List[JobItem]
    truck: Truck
    stop_order: int
    estimated_arrival: datetime
    estimated_departure: datetime
    drive_minutes_from_previous: float
    service_minutes: float
    location_index: int
    wait_minutes: float = 0.0
    slack_minutes: float = 0.0
    leg_distance_meters: float = 0.0


@dataclass
class TruckRoute:
    truck: Truck
    assignments: List[JobAssignment]
    total_drive_minutes: float
    total_service_minutes: float
    total_weight_lb: float
    overtime_minutes: float

    @property
    def total_time_minutes(self) -> float:
        return self.total_drive_minutes + self.total_service_minutes

    def calculate_cost(self, config: AppConfig) -> float:
        # Mirror greedy cost function to keep parity
        if hasattr(config.solver, "weights"):
            drive_cost = self.total_drive_minutes * config.solver.weights.drive_minutes
            service_cost = self.total_service_minutes * config.solver.weights.service_minutes
            overtime_cost = self.overtime_minutes * config.solver.weights.overtime_minutes
            max_route_cost = self.total_time_minutes * config.solver.weights.max_route_minutes

            # Accumulate priority position cost
            raw_priority = 0.0
            for i, assignment in enumerate(self.assignments):
                position_penalty = i + 1
                if hasattr(config.solver, 'priority') and hasattr(config.solver.priority, 'urgency_weights'):
                    urgency_weights = config.solver.priority.urgency_weights
                    if assignment.job.priority == 0:
                        priority_weight = urgency_weights.critical
                    elif assignment.job.priority == 1:
                        priority_weight = urgency_weights.high
                    elif assignment.job.priority == 2:
                        priority_weight = urgency_weights.medium
                    else:
                        priority_weight = urgency_weights.low
                else:
                    if assignment.job.priority == 0:
                        priority_weight = 10.0
                    else:
                        priority_weight = 4.0 - assignment.job.priority
                job_priority_cost = position_penalty * priority_weight
                raw_priority += job_priority_cost
                print(f"Priority Debug: Job {assignment.job.id} P{assignment.job.priority} at position {i+1}, weight={priority_weight}, cost={job_priority_cost}")

            if hasattr(config.solver, 'priority') and hasattr(config.solver.priority, 'performance_trade_off'):
                raw_priority *= config.solver.priority.performance_trade_off

            # If balance slider is set, use normalized combination like greedy
            s = getattr(config.solver, 'balance_slider', None)
            if isinstance(s, (int, float)):
                travel_raw = drive_cost + service_cost
                travel_norm = travel_raw / (1.0 + travel_raw)
                priority_soft = raw_priority * config.solver.weights.priority_soft_cost
                priority_norm = priority_soft / (1.0 + priority_soft)
                f = 10 ** (1 - s)
                g = 10 ** (s - 1)
                total_cost = overtime_cost + max_route_cost + (f * travel_norm) + (g * priority_norm)
                print(f"OR-Tools Route Cost (norm): f={f:.4f}, g={g:.4f}, travel_norm={travel_norm:.4f}, priority_norm={priority_norm:.4f}, overtime={overtime_cost:.2f}, max_route={max_route_cost:.2f}, total={total_cost:.2f}")
                return total_cost
            else:
                priority_cost = raw_priority * config.solver.weights.priority_soft_cost
                total_cost = drive_cost + service_cost + overtime_cost + max_route_cost + priority_cost
                print(f"OR-Tools Route Cost: drive={drive_cost:.2f}, service={service_cost:.2f}, overtime={overtime_cost:.2f}, priority={priority_cost:.2f}, total={total_cost:.2f}")
                return total_cost
        else:
            efficiency_cost = (self.total_drive_minutes + self.total_service_minutes) * config.solver.efficiency_weight
            overtime_cost = self.overtime_minutes * config.solver.overtime_penalty_per_minute
            priority_cost = 0.0
            for i, assignment in enumerate(self.assignments):
                position_penalty = i + 1
                if hasattr(config.solver, 'priority') and hasattr(config.solver.priority, 'urgency_weights'):
                    urgency_weights = config.solver.priority.urgency_weights
                    if assignment.job.priority == 0:
                        priority_weight = urgency_weights.critical
                    elif assignment.job.priority == 1:
                        priority_weight = urgency_weights.high
                    elif assignment.job.priority == 2:
                        priority_weight = urgency_weights.medium
                    else:
                        priority_weight = urgency_weights.low
                else:
                    if assignment.job.priority == 0:
                        priority_weight = 10.0
                    else:
                        priority_weight = 4.0 - assignment.job.priority
                priority_cost += position_penalty * priority_weight
            if hasattr(config.solver, 'priority') and hasattr(config.solver.priority, 'performance_trade_off'):
                priority_cost *= config.solver.priority.performance_trade_off
            priority_cost *= config.solver.priority_weight
            return efficiency_cost + overtime_cost + priority_cost


@dataclass
class Solution:
    routes: List[TruckRoute]
    unassigned_jobs: List[Job]
    total_cost: float
    feasible: bool
    computation_time_seconds: float


logger = logging.getLogger(__name__)


@dataclass
class VRPData:
    """VRP problem data structure for OR-Tools."""
    time_matrix: List[List[int]]      # Travel times in minutes (integer)
    distance_matrix: List[List[int]]  # Distances in meters (integer)
    demands: List[int]                # Weight demands per location
    vehicle_capacities: List[int]     # Capacity per vehicle
    num_vehicles: int                 # Number of vehicles
    depot: int                        # Depot index (always 0)
    service_times: List[int]          # Service time per location
    time_windows: List[Tuple[int, int]]  # (earliest, latest) time per location
    priorities: List[int]             # Priority per location (1=high, 3=low)
    large_item_flags: List[bool]      # Whether location requires large truck
    pickup_delivery_pairs: List[Tuple[int, int]]  # (pickup_idx, delivery_idx) pairs


class ORToolsVRPSolver:
    """
    OR-Tools based VRP solver that handles complex constraints and scales well.
    Removes dependency on Google Distance Matrix API by using offline calculations.
    """
    
    def __init__(self, config: AppConfig):
        """Initialize OR-Tools VRP solver."""
        if not ORTOOLS_AVAILABLE:
            raise ImportError("OR-Tools is required but not installed. Run: poetry install --extras ortools")
        
        self.config = config
        self.validator = ConstraintValidator(config)
        self.offline_calculator = OfflineDistanceCalculator(config)
        
        # OR-Tools solver parameters
        self.solver_params = {
            "first_solution_strategy": routing_enums_pb2.FirstSolutionStrategy.AUTOMATIC,
            "local_search_metaheuristic": routing_enums_pb2.LocalSearchMetaheuristic.GUIDED_LOCAL_SEARCH,
            "time_limit_seconds": getattr(config.solver, "ortools_time_limit_seconds", 30),
            "solution_limit": getattr(config.solver, "ortools_solution_limit", 100),
            "log_search": getattr(config.solver, "ortools_log_search", False),
        }
        
        logger.info("OR-Tools VRP solver initialized")
    
    def solve(
        self,
        trucks: List[Truck],
        jobs: List[Job],
        job_items_map: Dict[int, List[JobItem]],
        locations: List[Location],
        depot_coords: Coordinates,
        workday_start: datetime,
        trace: bool = False
    ) -> Solution:
        """
        Solve VRP using OR-Tools.
        
        Args:
            trucks: Available trucks
            jobs: Jobs to assign
            job_items_map: Mapping from job_id to list of JobItems
            locations: All locations (depot should be first)
            depot_coords: Depot coordinates
            workday_start: Start time of workday
            trace: Whether to record decision trace data
            
        Returns:
            Complete solution
        """
        start_time = datetime.now()
        
        logger.info(f"Starting OR-Tools VRP solver with {len(trucks)} trucks, {len(jobs)} jobs")
        
        if not jobs:
            # No jobs to optimize
            empty_routes = [TruckRoute(
                truck=truck,
                assignments=[],
                total_drive_minutes=0.0,
                total_service_minutes=0.0,
                total_weight_lb=0.0,
                overtime_minutes=0.0
            ) for truck in trucks]
            
            return Solution(
                routes=empty_routes,
                unassigned_jobs=[],
                total_cost=0.0,
                feasible=True,
                computation_time_seconds=0.0
            )
        
        try:
            # Prepare VRP data
            vrp_data = self._prepare_vrp_data(
                trucks, jobs, job_items_map, locations, depot_coords, workday_start
            )
            
            # Create OR-Tools routing model
            manager, routing = self._create_routing_model(vrp_data)
            
            # Add constraints
            self._add_constraints(manager, routing, vrp_data, trucks, jobs)
            
            # Solve the problem
            search_parameters = pywrapcp.DefaultRoutingSearchParameters()
            search_parameters.first_solution_strategy = self.solver_params["first_solution_strategy"]
            search_parameters.local_search_metaheuristic = self.solver_params["local_search_metaheuristic"]
            search_parameters.time_limit.seconds = self.solver_params["time_limit_seconds"]
            search_parameters.solution_limit = self.solver_params["solution_limit"]
            search_parameters.log_search = self.solver_params["log_search"]
            
            logger.info(f"Running OR-Tools solver with {self.solver_params['time_limit_seconds']}s time limit")
            assignment = routing.SolveWithParameters(search_parameters)
            
            if assignment:
                # Convert solution
                solution = self._convert_solution(
                    manager, routing, assignment, vrp_data, trucks, jobs, 
                    job_items_map, locations, workday_start
                )
                
                computation_time = (datetime.now() - start_time).total_seconds()
                solution.computation_time_seconds = computation_time
                
                logger.info(f"OR-Tools solver completed in {computation_time:.2f}s: "
                           f"{len(jobs) - len(solution.unassigned_jobs)}/{len(jobs)} jobs assigned")
                
                return solution
            else:
                logger.warning("OR-Tools could not find a solution")
                
                # Return empty solution with all jobs unassigned
                empty_routes = [TruckRoute(
                    truck=truck,
                    assignments=[],
                    total_drive_minutes=0.0,
                    total_service_minutes=0.0,
                    total_weight_lb=0.0,
                    overtime_minutes=0.0
                ) for truck in trucks]
                
                computation_time = (datetime.now() - start_time).total_seconds()
                
                return Solution(
                    routes=empty_routes,
                    unassigned_jobs=jobs,
                    total_cost=float('inf'),
                    feasible=False,
                    computation_time_seconds=computation_time
                )
                
        except Exception as e:
            logger.error(f"OR-Tools solver failed: {e}")
            
            # Fall back to empty solution
            empty_routes = [TruckRoute(
                truck=truck,
                assignments=[],
                total_drive_minutes=0.0,
                total_service_minutes=0.0,
                total_weight_lb=0.0,
                overtime_minutes=0.0
            ) for truck in trucks]
            
            computation_time = (datetime.now() - start_time).total_seconds()
            
            return Solution(
                routes=empty_routes,
                unassigned_jobs=jobs,
                total_cost=float('inf'),
                feasible=False,
                computation_time_seconds=computation_time
            )
    
    def _prepare_vrp_data(
        self,
        trucks: List[Truck],
        jobs: List[Job],
        job_items_map: Dict[int, List[JobItem]],
        locations: List[Location],
        depot_coords: Coordinates,
        workday_start: datetime
    ) -> VRPData:
        """Prepare data structures for OR-Tools VRP solver."""
        # Create location coordinates list (depot first)
        location_coords = [depot_coords]
        for job in jobs:
            if job.location.lat and job.location.lon:
                location_coords.append(Coordinates(lat=job.location.lat, lon=job.location.lon))
            else:
                logger.warning(f"Missing coordinates for job {job.id}, using depot coordinates")
                location_coords.append(depot_coords)
        
        # Calculate offline distance matrix
        distance_matrix_float = self.offline_calculator.compute_travel_matrix(
            location_coords, departure_time=workday_start
        )
        
        # Convert to integer matrices (OR-Tools requirement)
        time_matrix = [[int(duration) for duration in row] for row in distance_matrix_float.durations_minutes]
        distance_matrix = [[int(distance) for distance in row] for row in distance_matrix_float.distances_meters]
        
        # Prepare demands (weight requirements)
        demands = [0]  # Depot has no demand
        for job in jobs:
            job_items = job_items_map[job.id]
            total_weight = sum(item.item.weight_lb_per_unit * item.qty for item in job_items)
            demands.append(int(total_weight))
        
        # Prepare vehicle capacities
        vehicle_capacities = [int(truck.max_weight_lb) for truck in trucks]
        
        # Prepare service times
        service_times = [0]  # Depot has no service time
        for job in jobs:
            job_items = job_items_map[job.id]
            service_time = self.validator.calculate_service_time(job_items)
            service_times.append(int(service_time))
        
        # Prepare time windows (convert to minutes from workday start)
        time_windows = [(0, 24 * 60)]  # Depot is always available
        workday_end_minutes = 10 * 60  # 10 hours workday
        
        for job in jobs:
            if job.earliest and job.latest:
                earliest_delta = job.earliest - workday_start
                latest_delta = job.latest - workday_start
                earliest_minutes = int(earliest_delta.total_seconds() / 60)
                latest_minutes = int(latest_delta.total_seconds() / 60)
            else:
                # No specific time window - use workday bounds
                earliest_minutes = 0
                latest_minutes = workday_end_minutes
            
            time_windows.append((earliest_minutes, latest_minutes))
        
        # Prepare priorities
        priorities = [1]  # Depot has neutral priority
        for job in jobs:
            priorities.append(job.priority)
        
        # Prepare large item flags
        large_item_flags = [False]  # Depot
        for job in jobs:
            job_items = job_items_map[job.id]
            requires_large = any(item.item.requires_large_truck for item in job_items)
            large_item_flags.append(requires_large)
        
        # Identify pickup-delivery pairs (for future enhancement)
        pickup_delivery_pairs = []
        # TODO: Implement pickup-delivery pairing logic
        
        return VRPData(
            time_matrix=time_matrix,
            distance_matrix=distance_matrix,
            demands=demands,
            vehicle_capacities=vehicle_capacities,
            num_vehicles=len(trucks),
            depot=0,
            service_times=service_times,
            time_windows=time_windows,
            priorities=priorities,
            large_item_flags=large_item_flags,
            pickup_delivery_pairs=pickup_delivery_pairs
        )
    
    def _create_routing_model(self, vrp_data: VRPData) -> Tuple[Any, Any]:
        """Create OR-Tools routing model."""
        # Create routing index manager
        manager = pywrapcp.RoutingIndexManager(
            len(vrp_data.time_matrix),     # Number of locations
            vrp_data.num_vehicles,         # Number of vehicles
            vrp_data.depot                 # Depot index
        )
        
        # Create routing model
        routing = pywrapcp.RoutingModel(manager)
        
        return manager, routing
    
    def _add_constraints(
        self,
        manager: Any,
        routing: Any,
        vrp_data: VRPData,
        trucks: List[Truck],
        jobs: List[Job]
    ) -> None:
        """Add constraints to the routing model."""
        
        # 1. Travel time constraint
        def time_callback(from_index, to_index):
            from_node = manager.IndexToNode(from_index)
            to_node = manager.IndexToNode(to_index)
            return vrp_data.time_matrix[from_node][to_node]
        
        time_callback_index = routing.RegisterTransitCallback(time_callback)
        routing.SetArcCostEvaluatorOfAllVehicles(time_callback_index)
        
        # 2. Capacity constraint
        def demand_callback(from_index):
            from_node = manager.IndexToNode(from_index)
            return vrp_data.demands[from_node]
        
        demand_callback_index = routing.RegisterUnaryTransitCallback(demand_callback)
        routing.AddDimensionWithVehicleCapacity(
            demand_callback_index,
            0,  # Null capacity slack
            vrp_data.vehicle_capacities,  # Vehicle capacities
            True,  # Start cumul to zero
            'Capacity'
        )
        
        # 3. Time windows constraint
        routing.AddDimension(
            time_callback_index,
            30,  # Allow 30 minutes slack
            24 * 60,  # Maximum route duration (24 hours in minutes)
            False,  # Don't force start cumul to zero
            'Time'
        )
        time_dimension = routing.GetDimensionOrDie('Time')
        
        # Add time window constraints for each location
        for location_idx, time_window in enumerate(vrp_data.time_windows):
            if location_idx == vrp_data.depot:
                continue
            index = manager.NodeToIndex(location_idx)
            time_dimension.CumulVar(index).SetRange(time_window[0], time_window[1])
        
        # Set depot time window
        depot_idx = manager.NodeToIndex(vrp_data.depot)
        time_dimension.CumulVar(depot_idx).SetRange(0, 24 * 60)
        
        # 4. Large truck constraint (vehicles that can handle large items)
        for job_idx, requires_large in enumerate(vrp_data.large_item_flags):
            if requires_large and job_idx > 0:  # Skip depot
                node_index = manager.NodeToIndex(job_idx)
                # Only allow large-capable trucks for this job
                allowed_vehicles = [i for i, truck in enumerate(trucks) if truck.large_capable]
                if allowed_vehicles:
                    routing.VehicleVar(node_index).SetValues(allowed_vehicles)
        
        # 5. Priority-based penalties (higher priority = lower penalty)
        for job_idx, priority in enumerate(vrp_data.priorities):
            if job_idx > 0:  # Skip depot
                node_index = manager.NodeToIndex(job_idx)
                # Lower priority (higher number) gets higher penalty
                penalty = priority * 1000  # Scale penalty
                routing.AddDisjunction([node_index], penalty)
        
        # 6. Service time constraint
        for job_idx, service_time in enumerate(vrp_data.service_times):
            if job_idx > 0:  # Skip depot
                index = manager.NodeToIndex(job_idx)
                time_dimension.SetCumulVarSoftUpperBound(
                    index,
                    vrp_data.time_windows[job_idx][1] + service_time,
                    1000  # Penalty for violating soft bound
                )
    
    def _convert_solution(
        self,
        manager: Any,
        routing: Any,
        assignment: Any,
        vrp_data: VRPData,
        trucks: List[Truck],
        jobs: List[Job],
        job_items_map: Dict[int, List[JobItem]],
        locations: List[Location],
        workday_start: datetime
    ) -> Solution:
        """Convert OR-Tools solution to our solution format."""
        routes = []
        unassigned_jobs = []
        assigned_job_ids = set()
        
        # Extract routes for each vehicle
        for vehicle_id in range(vrp_data.num_vehicles):
            truck = trucks[vehicle_id]
            assignments = []
            
            # Follow the route for this vehicle
            index = routing.Start(vehicle_id)
            route_distance = 0
            route_time = 0
            route_weight = 0
            
            stop_order = 0
            current_time = workday_start
            prev_node = vrp_data.depot
            
            while not routing.IsEnd(index):
                node_index = manager.IndexToNode(index)
                
                if node_index != vrp_data.depot:  # Skip depot
                    # Find corresponding job
                    job = jobs[node_index - 1]  # Adjust for depot offset
                    job_items = job_items_map[job.id]
                    
                    # Calculate service time
                    service_minutes = vrp_data.service_times[node_index]
                    
                    # Calculate travel time and distance from previous location
                    travel_time = vrp_data.time_matrix[prev_node][node_index]
                    leg_distance_meters = float(vrp_data.distance_matrix[prev_node][node_index])
                    current_time += timedelta(minutes=travel_time)
                    
                    arrival_time = current_time
                    # Wait if early against job/location windows
                    wait_minutes = 0.0
                    service_start = arrival_time
                    job_earliest = job.earliest
                    loc_ws = job.location.window_start
                    if job_earliest and service_start < job_earliest:
                        wait_minutes = max(wait_minutes, (job_earliest - service_start).total_seconds() / 60)
                        service_start = job_earliest
                    if loc_ws and service_start.time() < loc_ws:
                        service_start = service_start.replace(hour=loc_ws.hour, minute=loc_ws.minute, second=0)
                        wait_minutes = max(wait_minutes, (service_start - arrival_time).total_seconds() / 60)
                    
                    # Slack relative to latest
                    slack_minutes = 0.0
                    if job.latest:
                        slack_minutes = ((job.latest - service_start).total_seconds() / 60) - service_minutes
                        slack_minutes = float(slack_minutes)
                    
                    departure_time = service_start + timedelta(minutes=service_minutes)
                    
                    # Create assignment
                    assignment_obj = JobAssignment(
                        job=job,
                        job_items=job_items,
                        truck=truck,
                        stop_order=stop_order,
                        estimated_arrival=arrival_time,
                        estimated_departure=departure_time,
                        drive_minutes_from_previous=float(travel_time),
                        service_minutes=float(service_minutes),
                        location_index=node_index,
                        wait_minutes=max(0.0, float(wait_minutes)),
                        slack_minutes=float(slack_minutes),
                        leg_distance_meters=leg_distance_meters
                    )
                    
                    assignments.append(assignment_obj)
                    assigned_job_ids.add(job.id)
                    
                    # Update totals
                    route_weight += vrp_data.demands[node_index]
                    route_time += travel_time
                    current_time = departure_time
                    stop_order += 1
                
                # Move to next location
                prev_node = node_index
                index = assignment.Value(routing.NextVar(index))
            
            # Calculate overtime
            workday_end = workday_start.replace(
                hour=int(self.config.depot.workday_window.end[:2]),
                minute=int(self.config.depot.workday_window.end[3:]),
                second=0
            )
            
            overtime_minutes = 0.0
            if assignments and assignments[-1].estimated_departure > workday_end:
                overtime_delta = assignments[-1].estimated_departure - workday_end
                overtime_minutes = overtime_delta.total_seconds() / 60
            
            # Create route
            truck_route = TruckRoute(
                truck=truck,
                assignments=assignments,
                total_drive_minutes=float(route_time),
                total_service_minutes=sum(a.service_minutes for a in assignments),
                total_weight_lb=float(route_weight),
                overtime_minutes=overtime_minutes
            )
            
            routes.append(truck_route)
        
        # Find unassigned jobs
        for job in jobs:
            if job.id not in assigned_job_ids:
                unassigned_jobs.append(job)
        
        # Calculate total cost
        total_cost = sum(route.calculate_cost(self.config) for route in routes if route.assignments)
        
        return Solution(
            routes=routes,
            unassigned_jobs=unassigned_jobs,
            total_cost=total_cost,
            feasible=len(unassigned_jobs) == 0,
            computation_time_seconds=0.0  # Will be set by caller
        )
