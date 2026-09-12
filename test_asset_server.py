import json
from mcp_servers.asset_server.server import get_regional_assets, calculate_transit_window

print("Running Standalone Tool Testing Logic for Asset MCP Server v2...\n")

# Test Case 1: Proximity lookup using the seed data off the coast of LA
# Mock Target Crash Coordinate: 33.9000 Lat, -118.4000 Lon
mock_lat = 33.9000
mock_lon = -118.4000

print(f"Executing [get_regional_assets] centered at: [{mock_lat}, {mock_lon}]...")
spatial_search_result = get_regional_assets(latitude=mock_lat, longitude=mock_lon, radius_km=150.0)
print(spatial_search_result)
print("-" * 60)

# Test Case 2: Travel window estimation logic checks
# Distance: 45.5 KM away, deploying a vehicle pacing at 120 Knots (e.g. HH-65 Dolphin helicopter)
mock_speed = 120.0
mock_distance = 45.5

print(f"Executing [calculate_transit_window] at {mock_speed} Knots over {mock_distance} KM...")
transit_result = calculate_transit_window(speed_knots=mock_speed, distance_km=mock_distance)
print(transit_result)