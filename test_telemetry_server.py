import json
from mcp_servers.telemetry_server.server import decode_beacon_hex, estimate_doppler_position

print("Running Standalone Tool Testing Logic for Telemetry MCP Server...\n")

# Test Case 1: Standard 15-Hex Distress ID Payload Decoding
mock_hex = "ADCD4023B340001"
print(f"Executing [decode_beacon_hex] with ID: {mock_hex}")
decoding_result = decode_beacon_hex(mock_hex)
print(decoding_result)
print("-" * 50)

# Test Case 2: Doppler Geospatial Frequency Coordinate Processing
mock_bursts = [
    {"timestamp": 1718012300, "frequency_hz": 406025100, "lat": 34.0500, "lon": -118.2500},
    {"timestamp": 1718012600, "frequency_hz": 406025350, "lat": 34.0540, "lon": -118.2380}
]
mock_json_input = json.dumps(mock_bursts)

print("Executing [estimate_doppler_position] with satellite frequency data points...")
position_result = estimate_doppler_position(mock_json_input)
print(position_result)