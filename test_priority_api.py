#!/usr/bin/env python3
"""
Test script to debug priority handling in the optimization.
Creates jobs with different priorities at the same location to test ordering.
"""

import asyncio
import sys
import requests
import json
from datetime import date

def create_test_job(priority: int, action: str, notes: str):
    """Create a test job via API."""
    today = date.today().isoformat()
    
    job_data = {
        "location_name": "Construction Site Alpha",
        "action": action,
        "priority": priority,
        "date": today,
        "notes": notes,
        "items": "rebar:5"
    }
    
    response = requests.post("http://localhost:8000/jobs/quick_add", 
                           headers={"Content-Type": "application/json"},
                           json=job_data)
    
    if response.status_code == 200:
        result = response.json()
        print(f"Job creation result: {result}")
        # The quick_add endpoint returns stats, not the job itself
        print(f"Created P{priority} job: {notes}")
        return {"priority": priority, "notes": notes}
    else:
        print(f"Failed to create job: {response.status_code} - {response.text}")
        return None

def test_optimization():
    """Test optimization with mixed priority jobs."""
    today = date.today().isoformat()
    
    print(f"Testing priority ordering for date: {today}")
    print("Creating test jobs...")
    
    # Create mixed priority jobs at same location
    jobs = []
    test_cases = [
        (3, "pickup", "P3 Low Priority Job - should be scheduled AFTER P1"),
        (1, "pickup", "P1 High Priority Job - should be scheduled BEFORE P3"),
        (3, "drop", "P3 Another Low Priority Job"),
        (1, "drop", "P1 Another High Priority Job")
    ]
    
    for priority, action, notes in test_cases:
        job = create_test_job(priority, action, notes)
        if job:
            jobs.append(job)
    
    print(f"\nCreated {len(jobs)} test jobs")
    
    # Run optimization
    print("\n" + "="*60)
    print("RUNNING OPTIMIZATION")
    print("="*60)
    
    optimize_data = {
        "date": today,
        "single_truck": False,
        "seed": 42
    }
    
    response = requests.post("http://localhost:8000/optimize",
                           headers={"Content-Type": "application/json"},
                           json=optimize_data)
    
    if response.status_code == 200:
        result = response.json()
        print(f"\nOptimization completed!")
        print(f"Total routes: {len(result['routes'])}")
        print(f"Unassigned jobs: {len(result.get('unassigned_jobs', []))}")
        print(f"Solver used: {result.get('solver_used', 'unknown')}")
        
        if result.get('unassigned_jobs'):
            print("\nUnassigned jobs:")
            for job in result['unassigned_jobs']:
                print(f"  Job {job['id']}: P{job['priority']} - {job['notes']}")
                
    else:
        print(f"Optimization failed: {response.status_code} - {response.text}")

def check_server():
    """Check if server is running."""
    try:
        response = requests.get("http://localhost:8000/health")
        if response.status_code == 200:
            print("Server is running")
            return True
    except requests.exceptions.ConnectionError:
        print("Server is not running. Please start it with: poetry run uvicorn app.api:app --host 0.0.0.0 --port 8000")
        return False

if __name__ == "__main__":
    if check_server():
        test_optimization()
