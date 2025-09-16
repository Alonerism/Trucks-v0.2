#!/usr/bin/env python3
"""
Test script to debug priority handling in the optimization.
Creates jobs with different priorities at the same location to test ordering.
"""

import asyncio
import sys
from datetime import datetime, date
sys.path.append('/Users/alonflorentin/Downloads/FreeLance/Truck-Optimize')

from app.service import TruckOptimizerService
from app.repo import DatabaseRepository
from app.models import Job, Location, JobItem
from app.schemas import OptimizeRequest
from sqlmodel import create_engine, Session

async def test_priority_ordering():
    """Test priority ordering by creating P1 and P3 jobs at the same location."""
    
    # Setup service (which initializes its own repo)
    service = TruckOptimizerService()
    repo = service.repo
    engine = repo.engine
    
    today = date.today().isoformat()
    
    print(f"Testing priority ordering for date: {today}")
    
    # Clear existing jobs for today
    with Session(engine) as session:
        existing_jobs = session.query(Job).filter(Job.date == today).all()
        # Also delete job items to avoid foreign key issues
        for job in existing_jobs:
            job_items = session.query(JobItem).filter(JobItem.job_id == job.id).all()
            for item in job_items:
                session.delete(item)
            session.delete(job)
        session.commit()
        print(f"Cleared {len(existing_jobs)} existing jobs for today")
    
    # Get or create test location
    location = repo.get_location_by_name("Construction Site Alpha")
    if not location:
        print("Creating test location...")
        location = Location(
            name="Construction Site Alpha",
            address="456 Alpha St, Los Angeles, CA",
            lat=34.0522,
            lon=-118.2437
        )
        with Session(engine) as session:
            session.add(location)
            session.commit()
            session.refresh(location)
    
    print(f"Using location: {location.name} (ID: {location.id})")
    
    # Create test jobs with different priorities at the same location
    test_jobs = [
        {
            "priority": 3,
            "notes": "P3 Low Priority Job - should be scheduled AFTER P1",
            "action": "pickup"
        },
        {
            "priority": 1, 
            "notes": "P1 High Priority Job - should be scheduled BEFORE P3",
            "action": "pickup"
        },
        {
            "priority": 3,
            "notes": "P3 Another Low Priority Job",
            "action": "drop"
        },
        {
            "priority": 1,
            "notes": "P1 Another High Priority Job", 
            "action": "drop"
        }
    ]
    
    created_jobs = []
    with Session(engine) as session:
        for job_data in test_jobs:
            # Create job
            job = Job(
                location_id=location.id,
                action=job_data["action"],
                priority=job_data["priority"],
                date=today,
                notes=job_data["notes"]
            )
            session.add(job)
            session.commit()
            session.refresh(job)
            
            # Create job item
            job_item = JobItem(
                job_id=job.id,
                item_name="rebar",
                qty=5
            )
            session.add(job_item)
            session.commit()
            
            created_jobs.append(job)
            print(f"Created Job {job.id}: P{job.priority} - {job.notes}")
    
    print(f"\nCreated {len(created_jobs)} test jobs")
    
    # Run optimization
    print("\n" + "="*60)
    print("RUNNING OPTIMIZATION")
    print("="*60)
    
    request = OptimizeRequest(
        date=today,
        single_truck=False,
        seed=42
    )
    
    try:
        result = await service.optimize_routes(request)
        
        print(f"\nOptimization completed!")
        print(f"Total routes: {len(result.routes)}")
        print(f"Unassigned jobs: {len(result.unassigned_jobs)}")
        print(f"Solver used: {result.solver_used}")
        
        if result.unassigned_jobs:
            print("\nUnassigned jobs:")
            for job in result.unassigned_jobs:
                print(f"  Job {job.id}: P{job.priority} - {job.notes}")
        
    except Exception as e:
        print(f"Error during optimization: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_priority_ordering())
