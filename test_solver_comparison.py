#!/usr/bin/env python3
"""
Comprehensive Solver Comparison Test
====================================

This standalone test compares OR-Tools VRP solver vs Greedy solver performance
on various datasets to validate the new offline optimization system.

Run with: python test_solver_comparison.py
"""

import sys
import os
import time
import json
from datetime import datetime, date, timedelta
from typing import List, Dict, Any, Tuple, Optional
from dataclasses import dataclass, asdict
import random
import math

# Add the app directory to Python path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '.'))

from app.models import Truck, Job, Location, JobItem, Item, ActionType
from app.distance import Coordinates
from app.solver_greedy import GreedySolver
from app.solver_ortools_vrp import ORToolsVRPSolver
from app.distance_offline import OfflineDistanceCalculator
from app.schemas import AppConfig
import yaml


@dataclass
class SolverResult:
    """Results from a solver run."""
    solver_name: str
    computation_time_seconds: float
    total_cost: float
    total_drive_minutes: float
    total_service_minutes: float
    overtime_minutes: float
    num_routes: int
    num_assigned_jobs: int
    num_unassigned_jobs: int
    routes_summary: List[Dict[str, Any]]
    success: bool
    error_message: str = ""


@dataclass
class TestCase:
    """A test case with specific parameters."""
    name: str
    num_jobs: int
    area_radius_miles: float
    job_complexity: str  # "simple", "mixed", "complex"
    priority_distribution: str  # "none", "mixed", "high_priority"
    
    # Additional fields populated during test generation
    trucks: List[Truck] = None
    jobs: List[Job] = None
    job_items_map: Dict[int, List[JobItem]] = None
    locations: List[Location] = None
    distance_matrix: Any = None  # RouteMatrix
    depot_coords: Coordinates = None
    

class SolverComparisonTest:
    """Main test class for comparing solvers."""
    
    def __init__(self):
        """Initialize the test environment."""
        # Load configuration
        with open("config/params.yaml", 'r') as f:
            config_data = yaml.safe_load(f)
        self.config = AppConfig.model_validate(config_data)
        
        self.offline_calculator = OfflineDistanceCalculator(self.config)
        
        # Fixed depot coordinates (Los Angeles)
        self.depot_coords = Coordinates(lat=34.0522, lon=-118.2437)
        
        # Create test fleet
        self.trucks = self._create_test_fleet()
        self.items = self._create_test_items()
        
    def _create_test_fleet(self) -> List[Truck]:
        """Create a test fleet of trucks."""
        return [
            Truck(
                id=1,
                name="Big Truck",
                max_weight_lb=12000,
                bed_len_ft=14,
                bed_width_ft=8,
                height_limit_ft=9,
                large_capable=True
            ),
            Truck(
                id=2,
                name="Small Truck A",
                max_weight_lb=3500,
                bed_len_ft=8,
                bed_width_ft=5.5,
                height_limit_ft=8,
                large_capable=False
            ),
            Truck(
                id=3,
                name="Small Truck B",
                max_weight_lb=3500,
                bed_len_ft=8,
                bed_width_ft=5.5,
                height_limit_ft=8,
                large_capable=False
            )
        ]
    
    def _create_test_items(self) -> List[Item]:
        """Create test items catalog."""
        return [
            Item(id=1, name="big drill", category="machine", weight_lb_per_unit=1800, 
                 dims_lwh_ft=[6, 3, 4], requires_large_truck=True),
            Item(id=2, name="small pump", category="equipment", weight_lb_per_unit=180,
                 dims_lwh_ft=[2.5, 2, 2], requires_large_truck=False),
            Item(id=3, name="rebar", category="material", weight_lb_per_unit=20,
                 requires_large_truck=False),
            Item(id=4, name="sand bag", category="material", weight_lb_per_unit=50,
                 requires_large_truck=False),
            Item(id=5, name="diesel", category="fuel", weight_lb_per_unit=7,
                 requires_large_truck=False),
        ]
    
    def _generate_random_location(self, radius_miles: float) -> Coordinates:
        """Generate a random location within radius of depot."""
        # Convert miles to approximate degrees (1 degree ≈ 69 miles)
        radius_deg = radius_miles / 69.0
        
        # Generate random point in circle
        angle = random.uniform(0, 2 * math.pi)
        r = random.uniform(0, radius_deg) * math.sqrt(random.random())
        
        lat = self.depot_coords.lat + r * math.cos(angle)
        lon = self.depot_coords.lon + r * math.sin(angle)
        
        return Coordinates(lat=lat, lon=lon)
    
    def _create_job_items(self, complexity: str) -> List[JobItem]:
        """Create job items based on complexity."""
        if complexity == "simple":
            # Single item jobs
            item = random.choice(self.items)
            quantity = random.randint(1, 3)
            return [JobItem(id=1, job_id=1, item_id=item.id, quantity=quantity, item=item)]
        
        elif complexity == "mixed":
            # 1-3 different items
            num_items = random.randint(1, 3)
            job_items = []
            selected_items = random.sample(self.items, num_items)
            
            for i, item in enumerate(selected_items):
                quantity = random.randint(1, 4)
                job_items.append(JobItem(
                    id=i+1, job_id=1, item_id=item.id, 
                    quantity=quantity, item=item
                ))
            return job_items
        
        else:  # complex
            # 3-5 different items with higher quantities
            num_items = random.randint(3, min(5, len(self.items)))
            job_items = []
            selected_items = random.sample(self.items, num_items)
            
            for i, item in enumerate(selected_items):
                quantity = random.randint(2, 8)
                job_items.append(JobItem(
                    id=i+1, job_id=1, item_id=item.id,
                    quantity=quantity, item=item
                ))
            return job_items
    
    def _get_priority(self, distribution: str) -> int:
        """Get job priority based on distribution (returns integer)."""
        if distribution == "none":
            return 1  # Normal priority
        elif distribution == "mixed":
            return random.choice([1, 1, 2, 3])  # Normal, Normal, High, Urgent
        else:  # high_priority
            return random.choice([2, 3, 3, 2])  # High, Urgent, Urgent, High
    
    def _generate_test_jobs(self, test_case: TestCase) -> Tuple[List[Job], List[Location]]:
        """Generate test jobs and locations for a test case."""
        jobs = []
        locations = []
        
        for i in range(test_case.num_jobs):
            # Create location
            coords = self._generate_random_location(test_case.area_radius_miles)
            location = Location(
                id=i+1,
                name=f"TestSite_{i+1}",
                address=f"Test Address {i+1}, Los Angeles, CA",
                lat=coords.lat,
                lon=coords.lon
            )
            locations.append(location)
            
            # Create job items
            job_items = self._create_job_items(test_case.job_complexity)
            
            # Update job_id for all items
            for item in job_items:
                item.job_id = i+1
            
            # Create job
            priority = self._get_priority(test_case.priority_distribution)
            job = Job(
                id=i+1,
                location_id=location.id,
                location=location,
                date=date.today().strftime("%Y-%m-%d"),
                action=ActionType.PICKUP,  # Default to pickup for test
                priority=priority,
                job_items=job_items
            )
            jobs.append(job)
        
        return jobs, locations
    
    def _run_greedy_solver(self, test_case: TestCase) -> Optional[SolverResult]:
        """Run greedy solver on test case."""
        try:
            solver = GreedySolver(self.config)
            
            start_time = time.time()
            solution = solver.solve(
                trucks=test_case.trucks,
                jobs=test_case.jobs,
                job_items_map=test_case.job_items_map,
                locations=test_case.locations,
                distance_matrix=test_case.distance_matrix,
                depot_coords=test_case.depot_coords,
                workday_start=datetime.now()
            )
            
            computation_time = time.time() - start_time
            
            # Calculate metrics
            total_drive_minutes = sum(route.total_drive_minutes for route in solution.routes)
            total_service_minutes = sum(route.total_service_minutes for route in solution.routes)
            overtime_minutes = sum(route.overtime_minutes for route in solution.routes)
            
            routes_summary = []
            for route in solution.routes:
                routes_summary.append({
                    "truck": route.truck.name,
                    "jobs": len(route.assignments),
                    "drive_minutes": route.total_drive_minutes,
                    "service_minutes": route.total_service_minutes,
                    "total_minutes": route.total_drive_minutes + route.total_service_minutes,
                    "overtime_minutes": route.overtime_minutes
                })
            
            return SolverResult(
                solver_name="Greedy (Offline)",
                computation_time_seconds=computation_time,
                total_cost=solution.total_cost,
                total_drive_minutes=total_drive_minutes,
                total_service_minutes=total_service_minutes,
                overtime_minutes=overtime_minutes,
                num_routes=len(solution.routes),
                num_assigned_jobs=len([job for route in solution.routes for job in route.jobs]),
                num_unassigned_jobs=len(solution.unassigned_jobs),
                routes_summary=routes_summary,
                success=True
            )
            
        except Exception as e:
            return SolverResult(
                solver_name="Greedy (Offline)",
                computation_time_seconds=0,
                total_cost=float('inf'),
                total_drive_minutes=0,
                total_service_minutes=0,
                overtime_minutes=0,
                num_routes=0,
                num_assigned_jobs=0,
                num_unassigned_jobs=len(test_case.jobs),
                routes_summary=[],
                success=False,
                error_message=str(e)
            )
    
    def _run_ortools_solver(self, test_case: TestCase) -> Optional[SolverResult]:
        """Run OR-Tools VRP solver on test case."""
        try:
            solver = ORToolsVRPSolver(self.config)
            
            start_time = time.time()
            solution = solver.solve(
                trucks=test_case.trucks,
                jobs=test_case.jobs,
                job_items_map=test_case.job_items_map,
                locations=test_case.locations,
                depot_coords=test_case.depot_coords,
                workday_start=datetime.now()
            )
            
            computation_time = time.time() - start_time
            
            # Calculate metrics
            total_drive_minutes = sum(route.total_drive_minutes for route in solution.routes)
            total_service_minutes = sum(route.total_service_minutes for route in solution.routes)
            overtime_minutes = sum(route.overtime_minutes for route in solution.routes)
            
            routes_summary = []
            for route in solution.routes:
                routes_summary.append({
                    "truck": route.truck.name,
                    "jobs": len(route.assignments),
                    "drive_minutes": route.total_drive_minutes,
                    "service_minutes": route.total_service_minutes,
                    "total_minutes": route.total_drive_minutes + route.total_service_minutes,
                    "overtime_minutes": route.overtime_minutes
                })
            
            return SolverResult(
                solver_name="OR-Tools VRP",
                computation_time_seconds=computation_time,
                total_cost=solution.total_cost,
                total_drive_minutes=total_drive_minutes,
                total_service_minutes=total_service_minutes,
                overtime_minutes=overtime_minutes,
                num_routes=len(solution.routes),
                num_assigned_jobs=sum(len(route.assignments) for route in solution.routes),
                num_unassigned_jobs=len(solution.unassigned_jobs),
                routes_summary=routes_summary,
                success=True,
                error_message=None
            )
            
        except Exception as e:
            print(f"OR-Tools solver failed: {e}")
            return SolverResult(
                solver_name="OR-Tools VRP",
                computation_time_seconds=0,
                total_cost=float('inf'),
                total_drive_minutes=0,
                total_service_minutes=0,
                overtime_minutes=0,
                num_routes=0,
                num_assigned_jobs=0,
                num_unassigned_jobs=len(test_case.jobs),
                routes_summary=[],
                success=False,
                error_message=str(e)
            )
        """Run the OR-Tools VRP solver and return results."""
        try:
            start_time = time.time()
            
            # Create solver
            solver = ORToolsVRPSolver(self.config)
            
            # Build job items mapping
            job_items_map = {job.id: job.job_items for job in jobs}
            
            # Solve
            solution = solver.solve(
                trucks=self.trucks,
                jobs=jobs,
                job_items_map=job_items_map,
                locations=locations,
                depot_coords=self.depot_coords,
                workday_start=datetime.now(),
                trace=False
            )
            
            computation_time = time.time() - start_time
            
            # Calculate metrics
            total_drive_minutes = sum(route.drive_minutes for route in solution.routes)
            total_service_minutes = sum(route.service_minutes for route in solution.routes)
            overtime_minutes = sum(route.overtime_minutes for route in solution.routes)
            
            routes_summary = []
            for route in solution.routes:
                routes_summary.append({
                    "truck": route.truck.name,
                    "jobs": len(route.jobs),
                    "drive_minutes": route.drive_minutes,
                    "service_minutes": route.service_minutes,
                    "total_minutes": route.total_minutes,
                    "overtime_minutes": route.overtime_minutes
                })
            
            return SolverResult(
                solver_name="OR-Tools VRP",
                computation_time_seconds=computation_time,
                total_cost=solution.total_cost,
                total_drive_minutes=total_drive_minutes,
                total_service_minutes=total_service_minutes,
                overtime_minutes=overtime_minutes,
                num_routes=len(solution.routes),
                num_assigned_jobs=len([job for route in solution.routes for job in route.jobs]),
                num_unassigned_jobs=len(solution.unassigned_jobs),
                routes_summary=routes_summary,
                success=True
            )
            
        except Exception as e:
            import traceback
            print(f"OR-Tools solver failed: {str(e)}")
            traceback.print_exc()
            return SolverResult(
                solver_name="OR-Tools VRP",
                computation_time_seconds=0,
                total_cost=float('inf'),
                total_drive_minutes=0,
                total_service_minutes=0,
                overtime_minutes=0,
                num_routes=0,
                num_assigned_jobs=0,
                num_unassigned_jobs=len(test_case.jobs),
                routes_summary=[],
                success=False,
                error_message=str(e)
            )
    
    def run_test_case(self, test_case: TestCase) -> Dict[str, Any]:
        """Run a single test case comparing both solvers."""
        print(f"\n🧪 Running Test Case: {test_case.name}")
        print(f"   Jobs: {test_case.num_jobs}, Area: {test_case.area_radius_miles} miles")
        print(f"   Complexity: {test_case.job_complexity}, Priority: {test_case.priority_distribution}")
        
        # Generate test data and populate the TestCase
        jobs, locations = self._generate_test_jobs(test_case)
        
        # Create job items map
        job_items_map = {}
        for job in jobs:
            job_items_map[job.id] = getattr(job, 'job_items', [])
        
        # Calculate distance matrix
        coords = [self.depot_coords] + [Coordinates(lat=loc.lat, lon=loc.lon) for loc in locations]
        distance_matrix = self.offline_calculator.compute_travel_matrix(coords)
        
        # Populate the test case with generated data
        test_case.trucks = self.trucks
        test_case.jobs = jobs
        test_case.job_items_map = job_items_map
        test_case.locations = [Location(id=0, name="Depot", address="Depot", lat=self.depot_coords.lat, lon=self.depot_coords.lon)] + locations
        test_case.distance_matrix = distance_matrix
        test_case.depot_coords = self.depot_coords
        
        # Run both solvers
        print("   🔄 Running Greedy solver...")
        greedy_result = self._run_greedy_solver(test_case)
        
        print("   🔄 Running OR-Tools solver...")
        ortools_result = self._run_ortools_solver(test_case)
        
        # Compare results
        comparison = self._compare_results(greedy_result, ortools_result)
        
        return {
            "test_case": asdict(test_case),
            "greedy_result": asdict(greedy_result),
            "ortools_result": asdict(ortools_result),
            "comparison": comparison
        }
    
    def _compare_results(self, greedy: SolverResult, ortools: SolverResult) -> Dict[str, Any]:
        """Compare two solver results."""
        if not greedy.success or not ortools.success:
            return {
                "status": "error",
                "greedy_success": greedy.success,
                "ortools_success": ortools.success,
                "greedy_error": greedy.error_message,
                "ortools_error": ortools.error_message
            }
        
        # Calculate improvements
        cost_improvement = ((greedy.total_cost - ortools.total_cost) / greedy.total_cost * 100) if greedy.total_cost > 0 else 0
        time_ratio = ortools.computation_time_seconds / greedy.computation_time_seconds if greedy.computation_time_seconds > 0 else float('inf')
        
        # Determine winner
        winner = "OR-Tools" if ortools.total_cost < greedy.total_cost else "Greedy"
        if abs(ortools.total_cost - greedy.total_cost) < 0.01:
            winner = "Tie"
        
        return {
            "status": "success",
            "winner": winner,
            "cost_improvement_percent": cost_improvement,
            "computation_time_ratio": time_ratio,
            "metrics": {
                "greedy": {
                    "cost": greedy.total_cost,
                    "routes": greedy.num_routes,
                    "assigned_jobs": greedy.num_assigned_jobs,
                    "time_seconds": greedy.computation_time_seconds
                },
                "ortools": {
                    "cost": ortools.total_cost,
                    "routes": ortools.num_routes,
                    "assigned_jobs": ortools.num_assigned_jobs,
                    "time_seconds": ortools.computation_time_seconds
                }
            }
        }
    
    def run_comprehensive_test(self) -> Dict[str, Any]:
        """Run comprehensive test suite."""
        print("🚀 Starting Comprehensive Solver Comparison Test")
        print("=" * 60)
        
        # Define test cases
        test_cases = [
            TestCase("Small Simple", 5, 10, "simple", "none"),
            TestCase("Small Mixed", 5, 10, "mixed", "mixed"),
            TestCase("Medium Simple", 15, 15, "simple", "none"),
            TestCase("Medium Complex", 15, 15, "complex", "mixed"),
            TestCase("Large Simple", 30, 20, "simple", "mixed"),
            TestCase("Large Complex", 30, 20, "complex", "high_priority"),
            TestCase("Extra Large", 50, 25, "mixed", "mixed"),
            TestCase("Stress Test", 75, 30, "complex", "high_priority"),
        ]
        
        results = []
        summary = {
            "total_tests": len(test_cases),
            "greedy_wins": 0,
            "ortools_wins": 0,
            "ties": 0,
            "errors": 0,
            "average_cost_improvement": 0,
            "average_time_ratio": 0
        }
        
        # Run all test cases
        for test_case in test_cases:
            try:
                result = self.run_test_case(test_case)
                results.append(result)
                
                # Update summary
                comparison = result["comparison"]
                if comparison["status"] == "error":
                    summary["errors"] += 1
                else:
                    if comparison["winner"] == "Greedy":
                        summary["greedy_wins"] += 1
                    elif comparison["winner"] == "OR-Tools":
                        summary["ortools_wins"] += 1
                    else:
                        summary["ties"] += 1
                    
                    summary["average_cost_improvement"] += comparison["cost_improvement_percent"]
                    summary["average_time_ratio"] += comparison["computation_time_ratio"]
                
                # Print result
                if comparison["status"] == "success":
                    print(f"   ✅ Winner: {comparison['winner']} "
                          f"(Cost improvement: {comparison['cost_improvement_percent']:.1f}%, "
                          f"Time ratio: {comparison['computation_time_ratio']:.2f}x)")
                else:
                    print(f"   ❌ Error in test case")
                    
            except Exception as e:
                print(f"   💥 Failed: {str(e)}")
                summary["errors"] += 1
        
        # Calculate averages
        successful_tests = summary["total_tests"] - summary["errors"]
        if successful_tests > 0:
            summary["average_cost_improvement"] /= successful_tests
            summary["average_time_ratio"] /= successful_tests
        
        return {
            "timestamp": datetime.now().isoformat(),
            "summary": summary,
            "detailed_results": results
        }
    
    def print_summary(self, results: Dict[str, Any]):
        """Print a nice summary of the test results."""
        print("\n" + "=" * 60)
        print("📊 SOLVER COMPARISON SUMMARY")
        print("=" * 60)
        
        summary = results["summary"]
        
        print(f"Total Tests Run: {summary['total_tests']}")
        print(f"Errors: {summary['errors']}")
        print(f"Successful Tests: {summary['total_tests'] - summary['errors']}")
        print()
        print("🏆 WINNERS:")
        print(f"   Greedy Solver: {summary['greedy_wins']} wins")
        print(f"   OR-Tools VRP: {summary['ortools_wins']} wins")
        print(f"   Ties: {summary['ties']}")
        print()
        print("📈 AVERAGE PERFORMANCE:")
        print(f"   Cost Improvement (OR-Tools vs Greedy): {summary['average_cost_improvement']:.1f}%")
        print(f"   Computation Time Ratio (OR-Tools/Greedy): {summary['average_time_ratio']:.2f}x")
        print()
        
        # Recommendations
        if summary['ortools_wins'] > summary['greedy_wins']:
            print("🎯 RECOMMENDATION: OR-Tools VRP solver provides better optimization quality")
        elif summary['greedy_wins'] > summary['ortools_wins']:
            print("🎯 RECOMMENDATION: Greedy solver provides competitive results with faster computation")
        else:
            print("🎯 RECOMMENDATION: Both solvers perform similarly - choose based on specific needs")
        
        if summary['average_time_ratio'] > 3:
            print("⚠️  WARNING: OR-Tools significantly slower - consider time limits for large datasets")
        
        print("\n" + "=" * 60)


def main():
    """Main test function."""
    # Set random seed for reproducible results
    random.seed(42)
    
    # Run the test
    tester = SolverComparisonTest()
    results = tester.run_comprehensive_test()
    
    # Print summary
    tester.print_summary(results)
    
    # Save detailed results
    output_file = f"solver_comparison_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(output_file, 'w') as f:
        json.dump(results, f, indent=2, default=str)
    
    print(f"\n💾 Detailed results saved to: {output_file}")


if __name__ == "__main__":
    import sys
    import os
    
    # Use the virtual environment Python path
    venv_python = "/Users/alonflorentin/Downloads/FreeLance/Truck-Optimize/.venv/bin/python"
    
    # If not running with venv python, restart with it
    if sys.executable != venv_python:
        print(f"Restarting with virtual environment Python: {venv_python}")
        os.execv(venv_python, [venv_python] + sys.argv)
    
    main()
