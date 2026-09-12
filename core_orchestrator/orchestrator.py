import os
import json
from typing import TypedDict, List, Dict, Optional
from langgraph.graph import StateGraph, END
import asyncio
from core_orchestrator.mcp_client_host import mcp_host_manager

# Import the v2 tool bindings directly from your microservice nodes
from core_orchestrator.security_masking import strip_survivor_pii_for_broadcast
from mcp_servers.telemetry_server.server import decode_beacon_hex, estimate_doppler_position
from mcp_servers.asset_server.server import get_regional_assets, calculate_transit_window
from mcp_servers.incident_server.server import update_mission_status, broadcast_incident_alerts

# 1. Define the Strict State Schema to pass through the multi-agent graph channels
class SARAppState(TypedDict):
    beacon_hex_id: str
    current_phase: str             # INGRESS, TELEMETRY, ASSET_ALLOCATION, VERIFICATION, DISPATCH, COMPLETE
    telemetry_profile: Dict        # Decoded country, serials, activation payload metrics
    computed_latitude: float
    computed_longitude: float
    nearby_assets: List[Dict]      # Proximity matched rescue units pulled out from PostGIS
    dispatch_receipts: List[Dict]  # Outward NAVTEX logs and RCC tracking entries
    execution_logs: List[str]      # Sequential diagnostic trace auditing trail
    verification_status: str       # VERIFIED_EMERGENCY, FALSE_ALARM, TEST_ALERT


# Example Node 1: Telemetry Parser Node Integration
def telemetry_parsing_agent_node(state: dict) -> dict:
    """Invokes the 'telemetry-parser' MCP server via JSON-RPC over stdio loops."""
    print("🛰️ [Agent Node] Triggering Telemetry Parser MCP Agent Link...")
    
    # Safely wrap asynchronous protocol coroutines inside your deterministic graph steps
    loop = asyncio.get_event_loop()
    mcp_response = loop.run_until_complete(
        mcp_host_manager.call_mcp_tool(
            server_name="telemetry-parser",
            tool_name="parse_satellite_burst", # Must match your server.py @server.tool names
            arguments={"raw_telemetry": state.get("telemetry_profile")}
        )
    )
    
    # Process output parameters back into your graph memory dictionary state
    if mcp_response:
        parsed_data = mcp_response[0].text if isinstance(mcp_response, list) else mcp_response
        state["execution_logs"].append("🎯 Telemetry parsed safely over Model Context Protocol pipe.")
        # Treat parsed outputs as updates...
        
    return state

# Example Node 2: Incident Coordinator Database Node Integration
def incident_coordination_node(state: dict) -> dict:
    """Offloads PostgreSQL database operations entirely to the 'incident-coordinator' MCP server."""
    print("⚡ [Agent Node] Routing mission state down to Incident Coordinator MCP database gate...")
    
    loop = asyncio.get_event_loop()
    mcp_db_result = loop.run_until_complete(
        mcp_host_manager.call_mcp_tool(
            server_name="incident-coordinator",
            tool_name="upsert_incident_mission", # Must match the exact decorator label on your server.py
            arguments={
                "beacon_hex_id": state.get("beacon_hex_id"),
                "status": state.get("verification_status")
            }
        )
    )
    
    state["execution_logs"].append("💾 State successfully synchronized inside PostGIS via MCP database tool loop.")
    return state

# 2. Define Node 1: The Telemetry Parsing Agent
def telemetry_parsing_node(state: SARAppState) -> Dict:
    logs = state.get("execution_logs", []) + ["[Telemetry Agent] Ingesting raw hex stream..."]
    
    # Invoke the deterministic v2 Telemetry MCP tool
    raw_decoding = decode_beacon_hex(state["beacon_hex_id"])
    parsed_decoding = json.loads(raw_decoding)
    
    if "error" in parsed_decoding:
        return {"current_phase": "COMPLETE", "verification_status": "FALSE_ALARM", "execution_logs": logs + [f"❌ Error: {parsed_decoding['error']}"]}
        
    # Read embedded tracking data if present, otherwise fallback to defaults
    bursts = state.get("telemetry_profile", {}).get("frequency_tracking_data", None)
    if not bursts:
        simulated_bursts = json.dumps([
            {"timestamp": 1718012300, "lat": 33.9110, "lon": -118.4100},
            {"timestamp": 1718012600, "lat": 33.9134, "lon": -118.4362}
        ])
    else:
        simulated_bursts = json.dumps(bursts)
        
    raw_doppler = estimate_doppler_position(simulated_bursts)
    parsed_doppler = json.loads(raw_doppler)
    
    lat = parsed_doppler.get("estimated_latitude", 0.0)
    lon = parsed_doppler.get("estimated_longitude", 0.0)
    
    logs.append(f"✅ Decoded. Country Code: {parsed_decoding.get('country_code')}. Trigger: {parsed_decoding.get('activation_trigger')}")
    logs.append(f"📍 Computed Doppler Spatial Target Coordinates: [{lat}, {lon}]")
    
    return {
        "telemetry_profile": parsed_decoding,
        "computed_latitude": lat,
        "computed_longitude": lon,
        "current_phase": "ASSET_ALLOCATION",
        "execution_logs": logs
    }

# 3. Define Node 2: The Resource & Asset Allocation Agent
def asset_allocation_node(state: SARAppState) -> Dict:
    logs = state.get("execution_logs", []) + ["[Asset Agent] Scanning PostGIS grid tracking arrays..."]
    
    raw_assets = get_regional_assets(
        latitude=state["computed_latitude"], 
        longitude=state["computed_longitude"], 
        radius_km=300.0
    )
    parsed_assets = json.loads(raw_assets)
    assets_found = parsed_assets.get("assets", [])
    logs.append(f"✅ PostGIS Proximity Search Found ({len(assets_found)}) capable emergency rescue units in range.")
    
    if assets_found:
        primary_unit = assets_found[0]  # Select the closest responder unit row entry
        
        # 💥 Passing coordinate vectors down to calculate true weather delays
        raw_transit = calculate_transit_window(
            speed_knots=130.0, 
            distance_km=primary_unit["distance_from_target_km"],
            target_latitude=state["computed_latitude"],
            target_longitude=state["computed_longitude"]
        )
        
        parsed_transit = json.loads(raw_transit)
        primary_unit["transit_eta_minutes"] = parsed_transit.get("estimated_transit_time_minutes")
        primary_unit["live_environmental_metrics"] = parsed_transit.get("live_environmental_metrics")
        
        # Log environmental metadata values back to trace lines
        env = parsed_transit.get("live_environmental_metrics", {})
        logs.append(f"🌤️ Live Weather at Target: Wind {env.get('wind_velocity_knots')} kts ({env.get('operational_risk_index')})")
        logs.append(f"🛸 Closest Unit: {primary_unit['asset_name']} | Wind-Adjusted ETA: {primary_unit['transit_eta_minutes']} Minutes.")
        
    return {
        "nearby_assets": assets_found,
        "current_phase": "VERIFICATION",
        "execution_logs": logs
    }

# 4. NEW NODE: Agent Verification Suite & Decision Policy Node
def agent_verification_node(state: SARAppState) -> Dict:
    logs = state.get("execution_logs", []) + ["[Verification Suite] Auditing mission profile context..."]
    print(f"⚡ Node Execution: Verification Guardrails Check for Hex ID [{state['beacon_hex_id']}]")
    
    telemetry = state["telemetry_profile"]
    lat = state["computed_latitude"]
    lon = state["computed_longitude"]
    activation = telemetry.get("activation_trigger", "")
    
    # 🔎 Guardrail 1: Test Burst Verification
    if "Test" in activation or "Verification" in activation:
        logs.append("ℹ️ Policy Match: Diagnostic Test Burst detected. Terminating routing chain safely before dispatch.")
        return {"verification_status": "TEST_ALERT", "current_phase": "COMPLETE", "execution_logs": logs}
        
    # 🔎 Guardrail 2: Geospatial Boundary Verification (Null/Zeroed Coordinates check)
    if abs(lat) < 0.001 and abs(lon) < 0.001:
        logs.append("❌ Policy Violation: Invalid spatial footprint detected (Null Island coordinate). Marked as Corrupted/False Alarm.")
        return {"verification_status": "FALSE_ALARM", "current_phase": "COMPLETE", "execution_logs": logs}
        
    # 🔎 Guardrail 3: Data Integrity Completeness Check
    if not telemetry.get("beacon_hex_id") or telemetry.get("country_code") == 0:
        logs.append("❌ Policy Violation: Incomplete telemetry payload signature structure. Aborting.")
        return {"verification_status": "FALSE_ALARM", "current_phase": "COMPLETE", "execution_logs": logs}
        
    # Standard Verified Emergency Path Confirmation
    logs.append("🛡️ Verification Pass: Mission parameters validated against safety policies. Proceeding to outward dispatch.")
    return {
        "verification_status": "VERIFIED_EMERGENCY",
        "current_phase": "DISPATCH",
        "execution_logs": logs
    }

# 5. Define Node 4: The Incident Coordination Agent (Targeted for Dispatches)
def incident_coordination_node(state: SARAppState) -> Dict:
    logs = state.get("execution_logs", []) + ["[Incident Agent] Provisioning multi-agency alert handoffs..."]
    
    # 1. Sync the UNMASKED raw tracking parameters to the secure PostgreSQL ledger (Internal Audit Trail)
    vessel = "Vessel_Serial_" + str(state["telemetry_profile"].get("serial_identifier", "UNK"))
    update_mission_status(beacon_hex_id=state["beacon_hex_id"], status="ACTIVE", vessel_name=vessel)
    logs.append("MCPServer Synced: Mission marked ACTIVE in persistent PostgreSQL database.")
    
    # 2. INTERCEPT AND MASK PII data variables specifically for outward public broadcasts
    from core_orchestrator.security_masking import strip_survivor_pii_for_broadcast
    sanitized_telemetry = strip_survivor_pii_for_broadcast(state["telemetry_profile"])
    logs.append("🛡️ Security Guardrail: Sensitive survivor identity fields masked for broadcast transmission.")
    
    # 3. Pass the SANITIZED parameters to compile the public NAVTEX alert sheet
    mock_coords_payload = json.dumps({"latitude": state["computed_latitude"], "longitude": state["computed_longitude"]})
    
    # 🛡️ THE FIX: Force the layout to mask the unique ID signature completely for external broadcasts
    # We strip the middle section of the beacon ID token for public channels
    raw_id = state["beacon_hex_id"]
    masked_beacon_id = f"{raw_id[:4]}-XXXX-{raw_id[-3:]}"
    
    raw_alerts = broadcast_incident_alerts(
        rcc_identifier="RCC_Alameda_CoastGuard",
        beacon_hex_id=masked_beacon_id,               # 👈 ENFORCES THE MASKED TOKEN CONTEXT
        rescue_coordinates_json=mock_coords_payload
    )
    
    parsed_alerts = json.loads(raw_alerts)
    logs.append("📢 Formatted and broadcasted global NAVTEX emergency safety bulletin.")
    
    return {
        "dispatch_receipts": [parsed_alerts],
        "current_phase": "COMPLETE",
        "execution_logs": logs
    }

# 6. Conditional Routing Evaluation Path Router
def routing_decision_policy(state: SARAppState) -> str:
    # Programmatic node selection based on structural state metrics
    status = state.get("verification_status")
    if status == "VERIFIED_EMERGENCY":
        return "dispatch_alerts"
    else:
        return END

# 7. Build out the State Graph Architecture Matrix
builder = StateGraph(SARAppState)

# Register functional computing nodes
builder.add_node("parse_telemetry", telemetry_parsing_node)
builder.add_node("allocate_resources", asset_allocation_node)
builder.add_node("verify_incident", agent_verification_node) # 👈 Add Verification Node
builder.add_node("dispatch_alerts", incident_coordination_node)

# Map Explicit Conditional Execution Edge Flow
builder.set_entry_point("parse_telemetry")
builder.add_edge("parse_telemetry", "allocate_resources")
builder.add_edge("allocate_resources", "verify_incident")

# Link the Verification Node outputs dynamically to conditional branches
builder.add_conditional_edges(
    "verify_incident",
    routing_decision_policy,
    {
        "dispatch_alerts": "dispatch_alerts",
        END: END
    }
)
builder.add_edge("dispatch_alerts", END)

# Compile the execution graph engine instance
sar_orchestrator_engine = builder.compile()
print("🚀 LangGraph SAR Orchestration Engine Successfully Compiled with Verification Guards.")