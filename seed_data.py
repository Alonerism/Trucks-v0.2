#!/usr/bin/env python3
"""
Seed script for truck optimizer test data.
Creates scenarios that trigger overtime/defer popup and Santa Monica restrictions.
"""

import sys
import asyncio
from datetime import datetime, date, timedelta
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from app.service import TruckOptimizerService
from app.models import Truck, Location, Item, Job, JobItem, ActionType, ItemCategory
from app.repo import DatabaseRepository


async def main():
    """Create seed data for testing."""
    print("Creating seed data for Truck Optimizer...")
    
    # Initialize service
    service = TruckOptimizerService()
    repo = service.repo
    
    # Clear existing data
    print("Clearing existing data...")
    with repo.get_session() as session:
        # Delete in reverse dependency order using SQLAlchemy text
        from sqlalchemy import text
        session.exec(text("DELETE FROM jobitem"))
        session.exec(text("DELETE FROM job"))
        session.exec(text("DELETE FROM routestop"))
        session.exec(text("DELETE FROM routeassignment"))
        session.exec(text("DELETE FROM unassignedjob"))
        session.exec(text("DELETE FROM item"))
        session.exec(text("DELETE FROM location"))
        session.exec(text("DELETE FROM truck"))
        session.commit()
    
    # Create trucks with large_truck designation
    print("Creating trucks...")
    trucks_data = [
        {
            "name": "Small Truck A",
            "max_weight_lb": 3500,
            "bed_len_ft": 8,
            "bed_width_ft": 5.5,
            "height_limit_ft": 8,
            "large_capable": False,
            "large_truck": False
        },
        {
            "name": "Small Truck B", 
            "max_weight_lb": 3500,
            "bed_len_ft": 8,
            "bed_width_ft": 5.5,
            "height_limit_ft": 8,
            "large_capable": False,
            "large_truck": False
        },
        {
            "name": "Large Truck",
            "max_weight_lb": 12000,
            "bed_len_ft": 14,
            "bed_width_ft": 8,
            "height_limit_ft": 9,
            "large_capable": True,
            "large_truck": True  # Subject to Santa Monica restrictions
        }
    ]
    
    with repo.get_session() as session:
        for truck_data in trucks_data:
            truck = Truck(**truck_data)
            session.add(truck)
        session.commit()
    
    print(f"Created {len(trucks_data)} trucks")
    
    # Create items with various weights and large-truck requirements
    print("Creating items...")
    items_data = [
        # Heavy items requiring large truck
        {"name": "Concrete Mixer", "category": ItemCategory.MACHINE, "weight_lb_per_unit": 2500, "requires_large_truck": True},
        {"name": "Steel Beam", "category": ItemCategory.MATERIAL, "weight_lb_per_unit": 800, "requires_large_truck": True},
        {"name": "Industrial Generator", "category": ItemCategory.EQUIPMENT, "weight_lb_per_unit": 1200, "requires_large_truck": True},
        
        # Medium items
        {"name": "Small Pump", "category": ItemCategory.EQUIPMENT, "weight_lb_per_unit": 180, "requires_large_truck": False},
        {"name": "Rebar Bundle", "category": ItemCategory.MATERIAL, "weight_lb_per_unit": 400, "requires_large_truck": False},
        {"name": "Tools Kit", "category": ItemCategory.EQUIPMENT, "weight_lb_per_unit": 50, "requires_large_truck": False},
        
        # Light items
        {"name": "Diesel Fuel", "category": ItemCategory.FUEL, "weight_lb_per_unit": 7, "requires_large_truck": False},
        {"name": "Sand Bag", "category": ItemCategory.MATERIAL, "weight_lb_per_unit": 50, "requires_large_truck": False},
        {"name": "Small Parts", "category": ItemCategory.MATERIAL, "weight_lb_per_unit": 25, "requires_large_truck": False},
    ]
    
    with repo.get_session() as session:
        for item_data in items_data:
            item = Item(**item_data)
            session.add(item)
        session.commit()
    
    print(f"Created {len(items_data)} items")
    
    # Create locations including Santa Monica area
    print("Creating locations...")
    locations_data = [
        # Santa Monica locations (will trigger large truck restrictions)
        {"name": "Santa Monica Pier Construction", "address": "Santa Monica Pier, CA", "lat": 34.0089, "lon": -118.4973},
        {"name": "Santa Monica Airport", "address": "Santa Monica Airport, CA", "lat": 34.0158, "lon": -118.4513},
        {"name": "Third Street Promenade", "address": "Third Street Promenade, Santa Monica, CA", "lat": 34.0154, "lon": -118.4965},
        
        # Other LA area locations
        {"name": "Downtown LA Tower", "address": "Downtown Los Angeles, CA", "lat": 34.0522, "lon": -118.2437},
        {"name": "Beverly Hills Site", "address": "Beverly Hills, CA", "lat": 34.0736, "lon": -118.4004},
        {"name": "Hollywood Studio", "address": "Hollywood, CA", "lat": 34.0928, "lon": -118.3287},
        {"name": "Pasadena Factory", "address": "Pasadena, CA", "lat": 34.1478, "lon": -118.1445},
        {"name": "Long Beach Port", "address": "Long Beach, CA", "lat": 33.7701, "lon": -118.1937},
        {"name": "Burbank Warehouse", "address": "Burbank, CA", "lat": 34.1808, "lon": -118.3090},
        {"name": "Glendale Distribution", "address": "Glendale, CA", "lat": 34.1425, "lon": -118.2551},
        
        # Additional high-service-time locations to create overtime scenarios
        {"name": "Complex Assembly Site A", "address": "West Los Angeles, CA", "lat": 34.0522, "lon": -118.4435, "service_minutes_default": 45},
        {"name": "Complex Assembly Site B", "address": "Mid-City LA, CA", "lat": 34.0522, "lon": -118.3000, "service_minutes_default": 45},
        {"name": "Complex Assembly Site C", "address": "East LA, CA", "lat": 34.0522, "lon": -118.1800, "service_minutes_default": 45},
    ]
    
    with repo.get_session() as session:
        for loc_data in locations_data:
            location = Location(**loc_data)
            session.add(location)
        session.commit()
    
    print(f"Created {len(locations_data)} locations")
    
    # Create jobs for today that will trigger overtime/defer scenarios
    today = date.today().isoformat()
    print(f"Creating jobs for {today}...")
    
    # Get created items and locations
    created_items = repo.get_items()
    locations = repo.get_locations()
    
    # Create job scenarios
    jobs_data = []
    
    # High-priority jobs (should be protected from deferral)
    priority_0_jobs = [
        {"location": "Downtown LA Tower", "action": ActionType.PICKUP, "priority": 0, "items": [("Concrete Mixer", 1)]},
        {"location": "Beverly Hills Site", "action": ActionType.DROP, "priority": 0, "items": [("Steel Beam", 2)]},
        {"location": "Hollywood Studio", "action": ActionType.PICKUP, "priority": 0, "items": [("Industrial Generator", 1)]},
    ]
    
    # Santa Monica jobs (will trigger large truck restrictions)
    santa_monica_jobs = [
        {"location": "Santa Monica Pier Construction", "action": ActionType.DROP, "priority": 1, "items": [("Concrete Mixer", 1), ("Steel Beam", 1)]},
        {"location": "Santa Monica Airport", "action": ActionType.PICKUP, "priority": 2, "items": [("Industrial Generator", 1)]},
        {"location": "Third Street Promenade", "action": ActionType.DROP, "priority": 1, "items": [("Steel Beam", 2)]},
    ]
    
    # Complex assembly jobs (high service time to trigger overtime)
    complex_jobs = [
        {"location": "Complex Assembly Site A", "action": ActionType.DROP, "priority": 2, "items": [("Small Pump", 3), ("Tools Kit", 5)]},
        {"location": "Complex Assembly Site B", "action": ActionType.PICKUP, "priority": 2, "items": [("Rebar Bundle", 4), ("Tools Kit", 3)]},
        {"location": "Complex Assembly Site C", "action": ActionType.DROP, "priority": 3, "items": [("Small Pump", 2), ("Sand Bag", 10)]},
    ]
    
    # Regular jobs to fill up the schedule
    regular_jobs = []
    location_names = [loc.name for loc in locations if "Complex" not in loc.name and "Santa Monica" not in loc.name]
    
    for i in range(30):  # Create 30 regular jobs
        import random
        location_name = random.choice(location_names)
        action = random.choice([ActionType.PICKUP, ActionType.DROP])
        priority = random.choice([1, 2, 2, 3])  # Weighted toward lower priority
        
        # Mix of items
        if priority == 1:
            items = [("Small Pump", random.randint(1, 2)), ("Tools Kit", random.randint(1, 3))]
        elif priority == 2:
            items = [("Rebar Bundle", random.randint(1, 2)), ("Sand Bag", random.randint(2, 5))]
        else:
            items = [("Small Parts", random.randint(1, 4)), ("Diesel Fuel", random.randint(5, 20))]
        
        regular_jobs.append({
            "location": location_name,
            "action": action,
            "priority": priority,
            "items": items
        })
    
    all_jobs = priority_0_jobs + santa_monica_jobs + complex_jobs + regular_jobs
    
    # Create jobs in database
    item_lookup = {item.name: item for item in created_items}
    location_lookup = {loc.name: loc for loc in locations}
    
    with repo.get_session() as session:
        for job_data in all_jobs:
            # Create job
            job = Job(
                location_id=location_lookup[job_data["location"]].id,
                action=job_data["action"],
                priority=job_data["priority"],
                date=today,
                notes=f"Seed data - Priority {job_data['priority']} {job_data['action'].value}"
            )
            session.add(job)
            session.flush()  # Get job ID
            
            # Add job items
            for item_name, qty in job_data["items"]:
                job_item = JobItem(
                    job_id=job.id,
                    item_id=item_lookup[item_name].id,
                    qty=qty
                )
                session.add(job_item)
        
        session.commit()
    
    print(f"Created {len(all_jobs)} jobs ({len(priority_0_jobs)} critical, {len(santa_monica_jobs)} Santa Monica, {len(complex_jobs)} complex, {len(regular_jobs)} regular)")
    
    print("Seed data creation complete!")
    print("\nScenarios created:")
    print(f"- {len(priority_0_jobs)} critical priority jobs (should be protected)")
    print(f"- {len(santa_monica_jobs)} Santa Monica jobs (large truck restrictions)")
    print(f"- {len(complex_jobs)} complex jobs with high service times")
    print(f"- {len(regular_jobs)} regular jobs to fill capacity")
    print(f"- Total: {len(all_jobs)} jobs for optimization")
    print("\nExpected behavior:")
    print("- Should require overtime for full schedule")
    print("- Santa Monica jobs should be restricted before 08:00 for large truck")
    print("- Should trigger overtime/defer popup if exceeds base workday + 60min")


if __name__ == "__main__":
    asyncio.run(main())
