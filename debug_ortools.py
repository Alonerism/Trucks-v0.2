#!/usr/bin/env python3
"""Quick test to debug the OR-Tools solver issue."""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '.'))

from app.models import Truck, Job, Location, JobItem, Item, ActionType
from app.distance import Coordinates
from app.solver_ortools_vrp import ORToolsVRPSolver
from app.schemas import AppConfig
import yaml
from datetime import datetime, date

# Load configuration
with open("config/params.yaml", 'r') as f:
    config_data = yaml.safe_load(f)
config = AppConfig.model_validate(config_data)

# Create test data
depot_coords = Coordinates(lat=34.0522, lon=-118.2437)

# Create a truck
truck = Truck(
    id=1,
    name="Test Truck",
    max_weight_lb=3500,
    bed_len_ft=8,
    bed_width_ft=5.5,
    height_limit_ft=8,
    large_capable=False
)

# Create an item
item = Item(id=1, name="test item", category="material", weight_lb_per_unit=50, requires_large_truck=False)

# Create a location
location = Location(
    id=1,
    name="Test Location",
    address="Test Address",
    lat=34.0622,
    lon=-118.2537
)

# Create job item
job_item = JobItem(
    id=1,
    job_id=1,
    item_id=1,
    quantity=2,
    item=item
)

# Create a job
job = Job(
    id=1,
    location_id=1,
    location=location,
    date="2025-09-04",
    action=ActionType.PICKUP,
    priority=1,
    job_items=[job_item]
)

# Test OR-Tools solver
print("Testing OR-Tools solver with minimal data...")
try:
    solver = ORToolsVRPSolver(config)
    
    job_items_map = {1: [job_item]}
    
    solution = solver.solve(
        trucks=[truck],
        jobs=[job],
        job_items_map=job_items_map,
        locations=[location],
        depot_coords=depot_coords,
        workday_start=datetime.now(),
        trace=True
    )
    
    print("✅ OR-Tools solver succeeded!")
    print(f"Total cost: {solution.total_cost}")
    print(f"Number of routes: {len(solution.routes)}")
    
except Exception as e:
    import traceback
    print(f"❌ OR-Tools solver failed: {str(e)}")
    traceback.print_exc()
