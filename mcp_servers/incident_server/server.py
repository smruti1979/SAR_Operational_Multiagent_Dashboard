import os
import sys
import json
import psycopg2
from mcp.server.fastmcp import FastMCP

# Initialize MCPServer explicitly following v2 SDK standards
mcp = FastMCP("Incident-Coordination-Server")

# Database parameters targeting your local running PostGIS container
DB_PARAMS = {
    "host": "sar-postgis-db",
    "user": "sar_admin",
    "password": "SecretRescuePassword123!",
    "database": "sar_mission_control",
    "port": 5432
}

@mcp.tool()
def update_mission_status(beacon_hex_id: str, status: str, vessel_name: str = "UNKNOWN") -> str:
    """
    Creates or updates the macro state timeline for an active emergency search incident 
    within the PostgreSQL state repository. Status values: ACTIVE, RESOLVED, FALSE_ALARM.
    """
    cleaned_hex = beacon_hex_id.strip().upper()
    target_status = status.strip().upper()
    
    if target_status not in ["ACTIVE", "RESOLVED", "FALSE_ALARM"]:
        return json.dumps({"error": f"Invalid mission status: {status}. Must be ACTIVE, RESOLVED, or FALSE_ALARM."})

    try:
        conn = psycopg2.connect(**DB_PARAMS)
        cursor = conn.cursor()
        
        # Upsert logic to handle fresh alerts and repeat distress satellite tracks cleanly
        query = """
            INSERT INTO incident_missions (beacon_hex_id, vessel_name, status, updated_at)
            VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT (beacon_hex_id) 
            DO UPDATE SET 
                status = EXCLUDED.status,
                vessel_name = COALESCE(NULLIF(EXCLUDED.vessel_name, 'UNKNOWN'), incident_missions.vessel_name),
                updated_at = CURRENT_TIMESTAMP
            RETURNING mission_id, status;
        """
        
        cursor.execute(query, (cleaned_hex, vessel_name, target_status))
        row = cursor.fetchone()
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return json.dumps({
            "beacon_hex_id": cleaned_hex,
            "database_mission_id": row[0],
            "current_sync_status": row[1],
            "transaction_log": "STATE_SYNCHRONIZATION_SUCCESSFUL"
        }, indent=2)
        
    except Exception as e:
        return json.dumps({"error": f"Mission persistent database update failure: {str(e)}"})


@mcp.tool()
def broadcast_incident_alerts(rcc_identifier: str, beacon_hex_id: str, rescue_coordinates_json: str) -> str:
    """
    Simulates broadcasting high-priority emergency handoff dispatches to the nearest 
    Regional Rescue Coordination Center (RCC) and builds out structural NAVTEX marine advisory sheets.
    """
    try:
        coordinates = json.loads(rescue_coordinates_json)
        lat = coordinates.get("latitude")
        lon = coordinates.get("longitude")
        
        if lat is None or lon is None:
            return json.dumps({"error": "Invalid rescue coordinates provided. Latitude and Longitude fields are required."})
    
        # Formulate a structured NAVTEX safety broadcast bulletin string
        navtex_bulletin = (
            f"ZCZC BROADCAST NO: SAR-{beacon_hex_id[:6]}\n"
            f"SECURITE / MARITIME DISTRESS ALERTS ENGINE CALLSIGN: INGRESS_NODE\n"
            f"DISTRESS SIGNAL DETECTED FROM BEACON HEX ID: {beacon_hex_id}\n"
            f"PROXIMITY LOCATION AREA TARGET POSITION: POSITION {abs(lat)} {'N' if lat >= 0 else 'S'} / "
            f"{abs(lon)} {'E' if lon >= 0 else 'W'}\n"
            f"ALL VESSELS AREA JURISDICTION {rcc_identifier.upper()} KEEP SHARP LOOKOUT AND REPORT ON SCENE\n"
            f"NNNN"
        )
        
        dispatch_receipt = {
            "target_agency_endpoint": rcc_identifier.upper(),
            "alert_status_dispatched": True,
            "navtex_broadcast_payload": navtex_bulletin,
            "comms_channel_verified": "SAT-DIGITAL-DSC"
        }
        
        return json.dumps(dispatch_receipt, indent=2)
        
    except Exception as e:
        return json.dumps({"error": f"Agency dispatch execution path failure: {str(e)}"})


if __name__ == "__main__":
    # Standard I/O pathway execution config for client pipeline handshakes
    mcp.run(transport="stdio")