"""
Offline distance and travel time calculations for route optimization.
Uses Haversine distance calculations with LA traffic patterns instead of Google APIs.
This removes API limits and provides predictable cost structure.
"""

import logging
import math
from datetime import datetime, time
from typing import List, Dict, Tuple, Optional
from dataclasses import dataclass

from .distance import Coordinates, RouteMatrix
from .schemas import AppConfig


logger = logging.getLogger(__name__)


@dataclass
class TrafficPattern:
    """Traffic pattern for different times of day in LA."""
    peak_morning_start: time
    peak_morning_end: time
    peak_evening_start: time
    peak_evening_end: time
    base_speed_mph: float
    peak_speed_mph: float
    weekend_speed_mph: float


class OfflineDistanceCalculator:
    """
    Offline distance and travel time calculator that doesn't rely on external APIs.
    Uses Haversine distance with LA-specific traffic patterns and road factors.
    """
    
    def __init__(self, config: AppConfig):
        """Initialize with configuration."""
        self.config = config
        
        # LA traffic patterns (approximate)
        self.traffic_pattern = TrafficPattern(
            peak_morning_start=time(7, 0),   # 7:00 AM
            peak_morning_end=time(9, 30),    # 9:30 AM
            peak_evening_start=time(16, 30), # 4:30 PM
            peak_evening_end=time(19, 0),    # 7:00 PM
            base_speed_mph=35.0,             # Normal traffic speed
            peak_speed_mph=20.0,             # Rush hour speed
            weekend_speed_mph=40.0           # Weekend speed
        )
        
        # LA road network factors
        self.la_factors = {
            "highway_ratio": 0.3,      # 30% highway driving
            "surface_ratio": 0.7,      # 70% surface streets
            "detour_factor": 1.4,      # Roads aren't straight lines
            "stop_penalty_minutes": 2.0, # Time for stops/lights per mile
        }
        
        logger.info("Initialized offline distance calculator with LA traffic patterns")
    
    def compute_travel_matrix(
        self,
        locations: List[Coordinates],
        departure_time: Optional[datetime] = None
    ) -> RouteMatrix:
        """
        Compute travel time matrix between all location pairs.
        Uses offline calculations instead of Google APIs.
        
        Args:
            locations: List of coordinates (depot should be first)
            departure_time: When travel begins (for traffic estimation)
            
        Returns:
            RouteMatrix with durations and distances
        """
        if not locations:
            raise ValueError("No locations provided")
        
        n = len(locations)
        logger.info(f"Computing offline travel matrix for {n} locations")
        
        # Use current time if no departure time specified
        if departure_time is None:
            departure_time = datetime.now()
        
        # Calculate traffic speed multiplier
        speed_multiplier = self._get_speed_multiplier(departure_time)
        
        durations_minutes = []
        distances_meters = []
        
        for i in range(n):
            duration_row = []
            distance_row = []
            for j in range(n):
                if i == j:
                    duration_row.append(0.0)
                    distance_row.append(0.0)
                else:
                    distance_km, duration_min = self._calculate_travel_time(
                        locations[i], locations[j], speed_multiplier
                    )
                    duration_row.append(duration_min)
                    distance_row.append(distance_km * 1000)
                    # Fine‑grained debug (only when matrix small to avoid log spam)
                    if n <= 30:
                        logger.debug(
                            f"offline-matrix leg i={i} j={j} dist_km={distance_km:.2f} min={duration_min:.2f} speed_mult={speed_multiplier:.2f}"
                        )
            durations_minutes.append(duration_row)
            distances_meters.append(distance_row)
        
        logger.info(f"Offline travel matrix computed successfully ({n}x{n} elements)")
        
        return RouteMatrix(
            origins=locations,
            destinations=locations,
            durations_minutes=durations_minutes,
            distances_meters=distances_meters
        )
    
    def _get_speed_multiplier(self, departure_time: datetime) -> float:
        """Get speed multiplier based on time of day and traffic patterns."""
        current_time = departure_time.time()
        is_weekend = departure_time.weekday() >= 5  # Saturday = 5, Sunday = 6
        
        if is_weekend:
            # Weekend traffic
            base_speed = self.traffic_pattern.weekend_speed_mph
        elif (self.traffic_pattern.peak_morning_start <= current_time <= self.traffic_pattern.peak_morning_end or
              self.traffic_pattern.peak_evening_start <= current_time <= self.traffic_pattern.peak_evening_end):
            # Rush hour traffic
            base_speed = self.traffic_pattern.peak_speed_mph
        else:
            # Normal traffic
            base_speed = self.traffic_pattern.base_speed_mph
        
        # Return multiplier relative to base speed
        return base_speed / self.traffic_pattern.base_speed_mph
    
    def _calculate_travel_time(
        self,
        origin: Coordinates,
        destination: Coordinates,
        speed_multiplier: float
    ) -> Tuple[float, float]:
        """
        Calculate travel distance and time between two points.
        
        Args:
            origin: Starting coordinates
            destination: Ending coordinates
            speed_multiplier: Traffic adjustment factor
            
        Returns:
            Tuple of (distance_km, duration_minutes)
        """
        # Calculate Haversine distance
        straight_distance_km = self._haversine_distance(origin, destination)

        # Detect coincident / near‑identical coordinates and short‑circuit
        if straight_distance_km < 0.01:  # <10m roughly
            logger.debug(f"offline-leg clamp coincident dist_km={straight_distance_km:.4f} -> 0.0m 2min floor")
            return 0.0, 2.0  # 2 minute nominal maneuver time
        
        # Apply road network detour factor (softened for micro hops)
        detour_factor = self.la_factors["detour_factor"]
        if straight_distance_km < 0.3:
            # Reduce excessive inflation for very short legs
            detour_factor = 1.05 + (straight_distance_km / 0.3) * 0.25  # 1.05..1.30
        road_distance_km = straight_distance_km * detour_factor
        
        # Calculate base travel time
        base_speed_kmh = self.traffic_pattern.base_speed_mph * 1.60934  # Convert to km/h
        adjusted_speed_kmh = base_speed_kmh * speed_multiplier
        
        # Calculate highway vs surface street travel
        highway_distance = road_distance_km * self.la_factors["highway_ratio"]
        surface_distance = road_distance_km * self.la_factors["surface_ratio"]
        
        # Highway is typically faster (less affected by traffic)
        highway_speed = adjusted_speed_kmh * 1.5
        surface_speed = adjusted_speed_kmh
        
        highway_time_hours = highway_distance / highway_speed
        surface_time_hours = surface_distance / surface_speed
        
        total_time_hours = highway_time_hours + surface_time_hours
        total_time_minutes = total_time_hours * 60
        
        # Add stop penalty for surface streets (reduced for micro legs)
        stop_penalty_minutes = self.la_factors["stop_penalty_minutes"]
        if straight_distance_km < 0.3:
            stop_penalty_minutes *= (straight_distance_km / 0.3)  # scale down toward 0
        stop_penalty = surface_distance * stop_penalty_minutes
        total_time_minutes += stop_penalty
        
        # Minimum travel time (clamp logic for micro legs)
        if straight_distance_km < 0.3:
            # Clamp to 2–4 minutes depending on distance (avoid multi‑hour inflation)
            min_time_minutes = max(2.0, straight_distance_km * 6.0)
        else:
            min_time_minutes = max(5.0, road_distance_km * 2)
        clamped_time = max(total_time_minutes, min_time_minutes)
        if clamped_time != total_time_minutes:
            logger.debug(
                f"offline-leg clamp dist_km={straight_distance_km:.3f} raw_min={total_time_minutes:.2f} -> {clamped_time:.2f}"
            )
        total_time_minutes = clamped_time
        return road_distance_km, total_time_minutes
    
    def _haversine_distance(self, coord1: Coordinates, coord2: Coordinates) -> float:
        """
        Calculate the great circle distance between two points on Earth.
        
        Args:
            coord1: First coordinate
            coord2: Second coordinate
            
        Returns:
            Distance in kilometers
        """
        # Convert decimal degrees to radians
        lat1, lon1, lat2, lon2 = map(
            math.radians, [coord1.lat, coord1.lon, coord2.lat, coord2.lon]
        )
        
        # Haversine formula
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        a = (math.sin(dlat / 2) ** 2 + 
             math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2)
        c = 2 * math.asin(math.sqrt(a))
        
        # Radius of earth in kilometers
        r = 6371
        
        return c * r
    
    def estimate_route_duration(
        self,
        route_coords: List[Coordinates],
        departure_time: datetime
    ) -> float:
        """
        Estimate total duration for a route visiting multiple coordinates.
        
        Args:
            route_coords: List of coordinates in visiting order
            departure_time: When the route starts
            
        Returns:
            Total duration in minutes
        """
        if len(route_coords) < 2:
            return 0.0
        
        total_duration = 0.0
        current_time = departure_time
        
        for i in range(len(route_coords) - 1):
            # Get speed multiplier for current time
            speed_multiplier = self._get_speed_multiplier(current_time)
            
            # Calculate segment duration
            _, segment_duration = self._calculate_travel_time(
                route_coords[i], route_coords[i + 1], speed_multiplier
            )
            
            total_duration += segment_duration
            
            # Update current time for next segment (assuming some service time)
            current_time = current_time.replace(
                minute=current_time.minute + int(segment_duration) + 15  # 15 min service time
            )
        
        return total_duration
    
    def get_distance_km(self, origin: Coordinates, destination: Coordinates) -> float:
        """Get direct distance between two points in kilometers."""
        return self._haversine_distance(origin, destination)


# Utility functions for backward compatibility
def create_offline_distance_matrix(
    locations: List[Coordinates],
    config: AppConfig,
    departure_time: Optional[datetime] = None
) -> RouteMatrix:
    """
    Create distance matrix using offline calculations.
    Convenience function for easy integration.
    """
    calculator = OfflineDistanceCalculator(config)
    return calculator.compute_travel_matrix(locations, departure_time)
