import json
from mcp_servers.incident_server.server import update_mission_status, broadcast_incident_alerts

print("Running Standalone Tool Testing Logic for Incident Coordination MCP Server v2...\n")

# Test Case 1: Upsert an active incident tracking record into the local PostGIS engine
mock_hex = "ADCD4023B340001"
mock_vessel = "SANS_SOUCI_HD"

print(f"Executing [update_mission_status] for Hex ID {mock_hex}...")
db_result = update_mission_status(beacon_hex_id=mock_hex, status="ACTIVE", vessel_name=mock_vessel)
print(db_result)
print("-" * 60)

# Test Case 2: Build out and verify the NAVTEX maritime broadcast matrix
mock_rcc = "RCC_ALAMEDA"
mock_coords = json.dumps({"latitude": 33.9122, "longitude": -118.4231})

print(f"Executing [broadcast_incident_alerts] to dispatch target: {mock_rcc}...")
broadcast_result = broadcast_incident_alerts(
    rcc_identifier=mock_rcc, 
    beacon_hex_id=mock_hex, 
    rescue_coordinates_json=mock_coords
)
print(broadcast_result)