"""
Test soft delete functionality for jobs.
"""

import pytest
from datetime import datetime
from fastapi.testclient import TestClient
from app.api import create_app
from app.service import TruckOptimizerService


@pytest.fixture
def client():
    """Create test client."""
    app = create_app()
    return TestClient(app)


@pytest.fixture  
def service():
    """Create service instance."""
    return TruckOptimizerService()


def test_job_soft_delete_persistence(client: TestClient, service: TruckOptimizerService):
    """Test that soft-deleted jobs persist and don't reappear."""
    # Create a test job
    job_data = {
        "location": "Test Location",
        "action": "pickup",
        "items": "test_item:1",
        "priority": 1,
        "notes": "Test job for deletion"
    }
    
    # Add job
    response = client.post("/jobs/quick-add", json=job_data)
    assert response.status_code == 200
    job_id = response.json()["job_id"]
    
    # Verify job exists
    jobs_response = client.get("/jobs")
    assert response.status_code == 200
    jobs = jobs_response.json()
    job_ids = [job["id"] for job in jobs]
    assert job_id in job_ids
    
    # Soft delete the job
    delete_response = client.delete(f"/jobs/{job_id}")
    assert delete_response.status_code == 200
    
    # Verify job is gone from regular queries
    jobs_after_delete = client.get("/jobs")
    assert jobs_after_delete.status_code == 200
    remaining_jobs = jobs_after_delete.json()
    remaining_job_ids = [job["id"] for job in remaining_jobs]
    assert job_id not in remaining_job_ids
    
    # Verify job is still in database but marked deleted
    with service.repo.get_session() as session:
        from app.models import Job
        job = session.get(Job, job_id)
        assert job is not None
        assert job.is_deleted == True
        assert job.deleted_at is not None
    
    # Verify job doesn't appear in optimization
    today = datetime.now().date().isoformat()
    jobs_for_optimization = service.repo.get_jobs_by_date(today)
    opt_job_ids = [job.id for job in jobs_for_optimization]
    assert job_id not in opt_job_ids


def test_job_soft_delete_idempotent(client: TestClient):
    """Test that deleting a job multiple times is idempotent."""
    # Create a test job
    job_data = {
        "location": "Test Location",
        "action": "pickup", 
        "items": "test_item:1",
        "priority": 1
    }
    
    response = client.post("/jobs/quick-add", json=job_data)
    assert response.status_code == 200
    job_id = response.json()["job_id"]
    
    # Delete once
    delete_response1 = client.delete(f"/jobs/{job_id}")
    assert delete_response1.status_code == 200
    
    # Delete again - should still return success
    delete_response2 = client.delete(f"/jobs/{job_id}")
    assert delete_response2.status_code == 200


def test_job_soft_delete_nonexistent(client: TestClient):
    """Test deleting a non-existent job."""
    response = client.delete("/jobs/99999")
    assert response.status_code == 200  # Idempotent, no error
    assert "not found" in response.json()["message"].lower()


def test_job_defer_to_next_day(client: TestClient, service: TruckOptimizerService):
    """Test deferring a job to the next day."""
    # Create a test job for today
    today = datetime.now().date().isoformat()
    
    job_data = {
        "location": "Test Location",
        "action": "pickup",
        "items": "test_item:1", 
        "priority": 2
    }
    
    response = client.post("/jobs/quick-add", json=job_data)
    assert response.status_code == 200
    job_id = response.json()["job_id"]
    
    # Defer the job with priority update
    defer_data = {"new_priority": 1}
    defer_response = client.post(f"/jobs/{job_id}/defer", json=defer_data)
    assert defer_response.status_code == 200
    
    # Verify job is no longer scheduled for today
    today_jobs = service.repo.get_jobs_by_date(today)
    today_job_ids = [job.id for job in today_jobs]
    assert job_id not in today_job_ids
    
    # Verify job is scheduled for tomorrow with updated priority
    from datetime import date, timedelta
    tomorrow = (date.today() + timedelta(days=1)).isoformat()
    tomorrow_jobs = service.repo.get_jobs_by_date(tomorrow)
    tomorrow_job_ids = [job.id for job in tomorrow_jobs]
    assert job_id in tomorrow_job_ids
    
    # Check priority was updated
    deferred_job = next(job for job in tomorrow_jobs if job.id == job_id)
    assert deferred_job.priority == 1


def test_bulk_defer_jobs(client: TestClient, service: TruckOptimizerService):
    """Test bulk deferring multiple jobs."""
    # Create multiple test jobs
    job_ids = []
    for i in range(3):
        job_data = {
            "location": f"Test Location {i}",
            "action": "pickup",
            "items": "test_item:1",
            "priority": 3
        }
        response = client.post("/jobs/quick-add", json=job_data)
        assert response.status_code == 200
        job_ids.append(response.json()["job_id"])
    
    # Bulk defer with different priority updates
    defer_request = {
        "jobs": [
            {"job_id": job_ids[0], "new_priority": 1},
            {"job_id": job_ids[1], "new_priority": 2},
            {"job_id": job_ids[2]}  # No priority update
        ]
    }
    
    bulk_response = client.post("/jobs/defer", json=defer_request)
    assert bulk_response.status_code == 200
    assert bulk_response.json()["deferred_count"] == 3
    
    # Verify all jobs were deferred
    today = datetime.now().date().isoformat()
    today_jobs = service.repo.get_jobs_by_date(today)
    today_job_ids = [job.id for job in today_jobs]
    
    for job_id in job_ids:
        assert job_id not in today_job_ids


def test_get_jobs_excludes_deleted_by_default(client: TestClient, service: TruckOptimizerService):
    """Test that get_jobs excludes soft-deleted jobs by default."""
    # Create and delete a job
    job_data = {
        "location": "Test Location",
        "action": "pickup",
        "items": "test_item:1",
        "priority": 1
    }
    
    response = client.post("/jobs/quick-add", json=job_data)
    job_id = response.json()["job_id"]
    
    # Delete the job
    client.delete(f"/jobs/{job_id}")
    
    # Regular get_jobs should exclude it
    regular_jobs = service.repo.get_jobs()
    regular_job_ids = [job.id for job in regular_jobs]
    assert job_id not in regular_job_ids
    
    # get_jobs with include_deleted=True should include it
    all_jobs = service.repo.get_jobs(include_deleted=True)
    all_job_ids = [job.id for job in all_jobs]
    assert job_id in all_job_ids
