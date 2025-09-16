#!/usr/bin/env python3
"""
Fix missing coordinates for locations in the database.
Assigns default LA coordinates to locations without coordinates.
"""

import yaml
from app.repo import DatabaseRepository
from app.schemas import AppConfig

# Load config
with open('config/params.yaml', 'r') as f:
    config_data = yaml.safe_load(f)
config = AppConfig.model_validate(config_data)

# Default coordinates for LA area (spread around different neighborhoods)
LA_COORDS = [
    (34.0522, -118.2437),  # Downtown LA
    (34.0736, -118.4004),  # Santa Monica
    (34.1478, -118.1445),  # Pasadena
    (34.0928, -118.3287),  # West Hollywood
    (34.0207, -118.3826),  # Culver City
    (34.1016, -118.4068),  # Beverly Hills
    (34.0245, -118.2967),  # USC area
    (34.0689, -118.4452),  # Venice
    (34.1184, -118.3004),  # Hollywood
    (34.1365, -118.3616),  # Universal City
]

def fix_missing_coordinates():
    repo = DatabaseRepository(config)
    
    # Get all locations
    locations = repo.get_locations()
    missing_coords = []
    
    for loc in locations:
        if not loc.lat or not loc.lon:
            missing_coords.append(loc)
    
    print(f"Found {len(missing_coords)} locations without coordinates:")
    for loc in missing_coords:
        print(f"  - {loc.name}")
    
    if not missing_coords:
        print("No missing coordinates found!")
        return
    
    # Assign coordinates
    coord_index = 0
    for loc in missing_coords:
        lat, lon = LA_COORDS[coord_index % len(LA_COORDS)]
        
        print(f"Assigning {lat}, {lon} to {loc.name}")
        
        # Update coordinates
        repo.update_location_coordinates(
            location_id=loc.id,
            lat=lat,
            lon=lon
        )
        
        coord_index += 1
    
    print(f"Updated {len(missing_coords)} locations with coordinates.")

if __name__ == "__main__":
    fix_missing_coordinates()
