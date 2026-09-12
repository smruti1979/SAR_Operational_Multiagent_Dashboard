import os
import json
from datetime import datetime

# Define the target local path for the compliant logging database
AUDIT_LOG_DIR = "core_audit_logs"

def commit_mission_audit_trace(final_state: dict):
    """
    Accepts the concluding state output dictionary from the LangGraph SAR engine,
    formats a structured compliance report, and dumps it to a local JSON audit file.
    """
    beacon_id = final_state.get("beacon_hex_id", "UNKNOWN_BEACON")
    status = final_state.get("verification_status", "UNVERIFIED")
    
    # Generate timestamp markers to differentiate distinct tracking events across the same ID
    timestamp_str = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    log_filename = f"incident_{beacon_id}_{timestamp_str}.json"
    log_filepath = os.path.join(AUDIT_LOG_DIR, log_filename)
    
    # Structure the formalized regulatory compliance output payload
    audit_payload = {
        "audit_version": "2.0.0",
        "log_generated_at_utc": datetime.utcnow().isoformat() + "Z",
        "incident_identification": {
            "beacon_hex_id": beacon_id,
            "verification_conclusion": status
        },
        "spatial_metrics": {
            "computed_latitude": final_state.get("computed_latitude", 0.0),
            "computed_longitude": final_state.get("computed_longitude", 0.0),
            "spatial_reference_system": "EPSG:4326 (WGS 84)"
        },
        "telemetry_profile": final_state.get("telemetry_profile", {}),
        "allocated_resources": final_state.get("nearby_assets", []),
        "dispatch_records": final_state.get("dispatch_receipts", []),
        "sequential_execution_traces": final_state.get("execution_logs", []),
        "llm_briefing_text": final_state.get("llm_briefing_text")
    }
    
    try:
        # Ensure the output directory structure is fully active
        os.makedirs(AUDIT_LOG_DIR, exist_ok=True)
        
        with open(log_filepath, "w", encoding="utf-8") as file_stream:
            json.dump(audit_payload, file_stream, indent=2)
            
        print(f"💾 [Audit Compliance] Secure trace file successfully written to disk: {log_filepath}")
        return log_filepath
    except Exception as e:
        print(f"❌ [Audit Error] Failed to generate compliance log file: {str(e)}")
        return None