import json
from core_orchestrator.orchestrator import sar_orchestrator_engine

def run_simulation():
    print("======================================================================")
    print("🚨 COSPAS-SARSAT SEARCH & RESCUE MISSION EMULATION SYSTEM STATUS: RUNNING")
    print("======================================================================\n")
    
    # 1. Initialize the root state layout payload with a raw hex distress token
    # This simulates a real satellite burst arriving via the system gateway proxy
    initial_state = {
        "beacon_hex_id": "ADCD4023B340001",
        "current_phase": "INGRESS",
        "telemetry_profile": {},
        "computed_latitude": 0.0,
        "computed_longitude": 0.0,
        "nearby_assets": [],
        "dispatch_receipts": [],
        "execution_logs": ["🚀 Initialization: Raw 406 MHz Ingress payload registered at gateway."]
    }
    
    # 2. Stream the incident payload completely through the graph logic thread
    final_output_state = sar_orchestrator_engine.invoke(initial_state)
    
    print("\n======================================================================")
    print("🏁 MISSION PIPELINE COMPUTATION SUMMARY LOG:")
    print("======================================================================")
    
    print("\n📋 Execution Diagnostic Traces:")
    for trace_entry in final_output_state["execution_logs"]:
        print(f"  {trace_entry}")
        
    print("\n📦 Structured Summary Output Context Bundle:")
    summary_bundle = {
        "target_id": final_output_state["beacon_hex_id"],
        "crash_grid": [final_output_state["computed_latitude"], final_output_state["computed_longitude"]],
        "allocated_assets_count": len(final_output_state["nearby_assets"]),
        "navtex_bulletin_preview": final_output_state["dispatch_receipts"][0]["navtex_broadcast_payload"].split("\n")[2]
    }
    print(json.dumps(summary_bundle, indent=2))
    print("\n======================================================================")

if __name__ == "__main__":
    run_simulation()