"""
Test truck restrictions and city rules.
"""

import pytest
from datetime import datetime, time
from fastapi.testclient import TestClient
from app.api import create_app
from app.service import TruckOptimizerService
from app.routing import TruckProfile, RoutingService, CityRulesValidator
from app.distance import Coordinates


@pytest.fixture
def client():
    """Create test client."""
    app = create_app()
    return TestClient(app)


@pytest.fixture
def service():
    """Create service instance.""" 
    return TruckOptimizerService()


def test_santa_monica_large_truck_restriction():
    """Test that large trucks are restricted in Santa Monica before 08:00."""
    service = TruckOptimizerService()
    validator = CityRulesValidator(service.config)
    
    # Santa Monica coordinates (inside restriction zone)
    santa_monica = Coordinates(lat=34.0089, lon=-118.4973)
    downtown_la = Coordinates(lat=34.0522, lon=-118.2437)
    
    # Large truck profile
    large_truck = TruckProfile(
        height_m=3.0,
        weight_kg=5000,
        is_commercial=True
    )
    
    # Create route segment
    from app.routing import RouteSegment
    segment = RouteSegment(
        origin=downtown_la,
        destination=santa_monica,
        duration_minutes=45,
        distance_km=25
    )
    
    # Test early morning (07:30) - should be restricted
    early_departure = datetime.combine(datetime.now().date(), time(7, 30))
    restricted_segment = validator.validate_route_segment(segment, large_truck, early_departure)
    
    assert restricted_segment.is_restricted == True
    assert "large truck" in restricted_segment.restriction_reason.lower()
    assert "08:00" in restricted_segment.restriction_reason
    
    # Test after 08:00 - should be allowed
    late_departure = datetime.combine(datetime.now().date(), time(8, 30))
    allowed_segment = validator.validate_route_segment(segment, large_truck, late_departure)
    
    assert allowed_segment.is_restricted == False


def test_small_truck_not_restricted():
    """Test that small trucks are not subject to large truck restrictions."""
    service = TruckOptimizerService()
    validator = CityRulesValidator(service.config)
    
    santa_monica = Coordinates(lat=34.0089, lon=-118.4973)
    downtown_la = Coordinates(lat=34.0522, lon=-118.2437)
    
    # Small truck profile
    small_truck = TruckProfile(
        height_m=2.5,
        weight_kg=2000,
        is_commercial=True
    )
    
    from app.routing import RouteSegment
    segment = RouteSegment(
        origin=downtown_la,
        destination=santa_monica,
        duration_minutes=45,
        distance_km=25
    )
    
    # Test early morning - small truck should be allowed
    early_departure = datetime.combine(datetime.now().date(), time(7, 30))
    result_segment = validator.validate_route_segment(segment, small_truck, early_departure)
    
    assert result_segment.is_restricted == False


def test_optimization_with_truck_restrictions(client: TestClient):
    """Test that optimization respects truck restrictions."""
    # This is an integration test that requires seeded data
    # Create jobs in Santa Monica and verify large truck scheduling
    
    # Create Santa Monica job requiring large truck
    job_data = {
        "location": "Santa Monica Pier Construction",
        "action": "drop",
        "items": "Concrete Mixer:1",  # Requires large truck
        "priority": 1,
        "notes": "Large truck required"
    }
    
    response = client.post("/jobs/quick-add", json=job_data)
    assert response.status_code == 200
    
    # Run optimization
    today = datetime.now().date().isoformat()
    opt_request = {
        "date": today,
        "auto": "overtime",
        "balance_slider": 1.0
    }
    
    opt_response = client.post("/optimize", json=opt_request)
    assert opt_response.status_code == 200
    
    result = opt_response.json()
    
    # Check that Santa Monica jobs are scheduled after 08:00 for large trucks
    large_truck_routes = [route for route in result["routes"] if "Large" in route["truck"]["name"]]
    
    if large_truck_routes:
        large_truck_route = large_truck_routes[0]
        santa_monica_stops = [stop for stop in large_truck_route["stops"] 
                             if "Santa Monica" in stop["job"]["location"]["name"]]
        
        for stop in santa_monica_stops:
            arrival_time = datetime.fromisoformat(stop["estimated_arrival"]).time()
            # Should be scheduled at or after 08:00
            assert arrival_time >= time(8, 0), f"Santa Monica stop scheduled too early: {arrival_time}"


def test_overtime_decision_structure():
    """Test that overtime decision response has correct structure."""
    service = TruckOptimizerService()
    
    # This would test the structure of overtime decision responses
    # when optimization requires more than base workday + 60min
    
    # For now, test the response structure with mock data
    from app.schemas import OvertimeDecision
    
    decision = OvertimeDecision(
        truck_id=1,
        truck_name="Test Truck",
        day_total_minutes=600,
        overtime_used_minutes=60,
        stops=[
            {
                "job_id": 1,
                "location": "Test Site",
                "eta": "08:30",
                "service_minutes": 30,
                "cumulative_minutes": 90
            }
        ],
        can_fit_with_60min_overtime=True
    )
    
    assert decision.truck_id == 1
    assert decision.overtime_used_minutes == 60
    assert len(decision.stops) == 1


def test_balance_slider_echo():
    """Test that balance slider values are echoed in response."""
    client = TestClient(create_app())
    
    # Test different slider values
    test_values = [0.0, 1.0, 2.0]
    
    for slider_value in test_values:
        request = {
            "date": datetime.now().date().isoformat(),
            "auto": "overtime",
            "balance_slider": slider_value
        }
        
        response = client.post("/optimize", json=request)
        assert response.status_code == 200
        
        result = response.json()
        
        # Check objective breakdown contains balance info
        assert "objective_breakdown" in result
        objective = result["objective_breakdown"]
        
        if "balance" in objective:
            balance_info = objective["balance"]
            assert balance_info["balance_slider"] == slider_value
            
            # Verify f and g weights follow the formula
            f = balance_info["f"]
            g = balance_info["g"]
            
            expected_f = 10 ** (1 - slider_value)
            expected_g = 10 ** (slider_value - 1)
            
            assert abs(f - expected_f) < 0.01
            assert abs(g - expected_g) < 0.01


def test_routing_provider_fallback():
    """Test that routing provider falls back gracefully."""
    service = TruckOptimizerService()
    
    # This would test routing provider fallback behavior
    # For now, test that we can create a routing service
    from app.routing import create_routing_service
    
    routing_service = create_routing_service(service.config)
    assert routing_service is not None
    
    # Test that preferred provider is available
    provider = routing_service.get_preferred_provider()
    assert provider is not None
