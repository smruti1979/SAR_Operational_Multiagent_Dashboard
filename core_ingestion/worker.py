import sys
import os
import json
import glob
from kafka import KafkaConsumer
import requests
import threading
import time

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from core_orchestrator.orchestrator import sar_orchestrator_engine
from core_orchestrator.audit_logger import commit_mission_audit_trace

# IMPORT THE COMPLIANT SECURITY GATEWAY INTERCEPTORS
from core_ingestion.security import verify_packet_authentication, sanitize_and_validate_payload

import asyncio
from core_orchestrator.mcp_client_host import mcp_host_manager

KAFKA_BOOTSTRAP_SERVERS = ["sar-kafka-broker:29092"] # ROUTES VIA INTERNAL CONTAINER PORT BRIDGE
TARGET_INGRESS_TOPIC = "satellite-ingress-raw"
AUDIT_LOG_DIR = "core_audit_logs"
OLLAMA_ENDPOINT = "http://sar-ollama-engine:11434/api/generate"


def _async_ollama_worker(beacon_id, status, target_file_path):
    """
    Independent background worker thread that executes the LLM prompt and
    directly updates the specific timestamped file created by the main thread.
    """
    import uuid
    trace_id = str(uuid.uuid4())[:8]
    prompt = f"""
    [trace:{trace_id}]
    Write a short 3-sentence maritime SAR report for Beacon: {beacon_id}, Status: {status}.
    Provide exactly these 3 lines:
    1. OPERATOR SUMMARY: Describe beacon readiness by repeating the exact Beacon ID and Status given above.
    2. NAVTEX BROADCAST: Confirm message processing.
    3. RCC HANDOFF: State transfer of operations to Coast Guard.
    Keep it extremely brief and under 40 words total. Do not cut off mid-sentence.
    """
    
    try:
        # Give the main thread a safe 1-second window to completely finish its disk writes [1]
        time.sleep(1.0)
        
        print(f"🤖 [LLM Asynchronous Pass] Querying Ollama for Hex ID [{beacon_id}]...")
        response = requests.post(OLLAMA_ENDPOINT, json={
            "model": "qwen2.5:1.5b", 
            "prompt": prompt,
            "stream": False,
            "keep_alive": "30m",
            "options": {
                "temperature": 0.1,
                "num_predict": 100
            }
        }, timeout=600) 
        
        if response.status_code == 200:
            ai_text = response.json().get("response", "Brief unavailable.")
            
            # Verify the exact target file still exists before writing [1]
            if os.path.exists(target_file_path):
                with open(target_file_path, "r", encoding="utf-8") as f:
                    current_payload = json.load(f)
                
                # Update only the briefing text key parameter
                current_payload["llm_briefing_text"] = ai_text
                
                # Overwrite the exact file cleanly without changing its timestamp suffix name [1]
                with open(target_file_path, "w", encoding="utf-8") as f:
                    json.dump(current_payload, f, indent=2)
                print(f"💾 [Audit Storage Sync] Successfully injected LLM text into: {os.path.basename(target_file_path)}")
        else: 
            print(f"⚠️ Ollama returned {response.status_code}: {response.text[:300]}") 
                  
    except Exception as async_err:
        print(f"⚠️ [Background LLM Fault] Thread computation dropped: {async_err}")

def spawn_llm_briefing_thread(beacon_id, status, target_file_path):
    if not target_file_path:
        print(f"⚠️ [LLM Dispatch] No audit file path available for beacon {beacon_id}, skipping briefing.")
        return
    llm_thread = threading.Thread(
        target=_async_ollama_worker,
        args=(beacon_id, status, target_file_path),
        daemon=True
    )
    llm_thread.start()
    print(f"🚀 [Async Core Onboarded] Decoupled thread spawned for target: {os.path.basename(target_file_path)}")

def run_llm_operator_briefing(final_state):
    """
    Generates the strict placeholder keyword to force the UI spinner state, 
    and handles file path target handoffs to the async thread safely.
    """
    beacon_id = final_state.get("beacon_hex_id")
    status = final_state.get("verification_status")
    
    # 🚀 Optimization: Force a strict processing keyword string!
    # This prevents app.py from displaying early, keeping the spinner active.
    final_state["llm_briefing_text"] = "Processing final report formatting metrics..."
    
    # Pre-calculate the exact filename that commit_mission_audit_trace will use [1]
    # (Matches the standard datetime format inside your audit_logger.py script)
    # from datetime import datetime
    # AUDIT_LOG_DIR = "core_audit_logs"
    
    # # Locate the newest timestamp marker pattern string [1]
    # timestamp_str = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    # log_filename = f"incident_{beacon_id}_{timestamp_str}.json"
    # target_file_path = os.path.join(AUDIT_LOG_DIR, log_filename)

    # # Spin up the background model runner thread, passing the precise future file target path [1]
    # llm_thread = threading.Thread(
    #     target=_async_ollama_worker, 
    #     args=(beacon_id, status, target_file_path),
    #     daemon=True
    # )
    # llm_thread.start()
    # print(f"🚀 [Async Core Onboarded] Decoupled thread spawned for target: {log_filename}")
        
    return final_state

def cleanup_old_audit_logs(max_kept_files: int = 10):
    """
    Scans the audit log directory, sorts trace files by modification time,
    and purges older files to enforce a strict rolling storage limit.
    """
    try:
        # Locate all tracking logs inside the directory
        log_files = glob.glob(os.path.join(AUDIT_LOG_DIR, "incident_*.json"))
        
        # If we have more logs than the maximum allowed limit, purge the oldest ones
        if len(log_files) > max_kept_files:
            # Sort files by modification time (oldest first)
            log_files.sort(key=os.path.getmtime)
            
            # Isolate the files that exceed our retention budget threshold
            files_to_delete = log_files[:-max_kept_files]
            
            for old_file in files_to_delete:
                os.remove(old_file)
                print(f"♻️ [Rolling Cleanup] Purged historical trace file: {os.path.basename(old_file)}")
                
    except Exception as cleanup_err:
        print(f"⚠️ [Cleanup Warning] Failed running rolling log purge loop: {cleanup_err}")

def warm_ollama_model():
    try:
        print("🔥 [Warmup] Pre-loading Ollama model into memory...")
        requests.post(OLLAMA_ENDPOINT, json={
            "model": "qwen2.5:1.5b",
            "prompt": "Hello",
            "stream": False,
            "keep_alive": "30m",
            "options": {"num_predict": 1}
        }, timeout=600)
        print("✅ [Warmup] Model loaded and resident.")
    except Exception as e:
        print(f"⚠️ [Warmup] Failed to pre-load model: {e}")

def start_ingress_worker():
    print("======================================================================")
    print("SAR INGESTION WORKER STATUS: RUNNING (EDGE SECURITY ENFORCED)")
    print("======================================================================")

    # THE BOOTSTRAP HOOK: Spawn subprocess paths for your 3 MCP servers inside active RAM blocks
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
    # Bootstrap your subprocess pipes safely
    loop.run_until_complete(mcp_host_manager.initialize_servers())
    warm_ollama_model()

    consumer = KafkaConsumer(
        TARGET_INGRESS_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="sar-ingestion-worker-group-v2",
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        session_timeout_ms=30000,
        max_poll_interval_ms=600000
    )
    
    print("Awaiting secure distress tracking events...\n")
    
    for message in consumer:
        raw_bytes = message.value
        
        # 1. AUTHENTICATION TIER CHECK: Extract signature from Kafka message headers
        packet_signature = ""
        if message.headers:
            for header_key, header_val in message.headers:
                if header_key == "mcc-signature":
                    packet_signature = header_val.decode('utf-8')
                    
        if not verify_packet_authentication(raw_bytes, packet_signature):
            print(f"❌ [SECURITY ALERT] Dropping packet from partition [{message.partition}]!")
            print(f"   -> Received Signature Header: '{packet_signature}'")
            print(f"   -> Payload Context Peek: {raw_bytes[:100]}...")
            continue
            
        # Parse payload safely now that origin validity is verified
        try:
            event_payload = json.loads(raw_bytes.decode('utf-8'))
        except Exception:
            print("❌ [Gateway Error] Failed parsing authenticated bytes to JSON structure.")
            continue
            
        # 2. EDGE SECURITY SANITIZATION TIER CHECK
        is_safe, security_code = sanitize_and_validate_payload(event_payload)
        if not is_safe:
            print(f"🛑 [EDGE REFUSAL] Packet blocked by gateway firewall filter. Reason: {security_code}")
            continue
            
        beacon_id = event_payload.get("beacon_hex_id")
        print(f"\n🔒 [Security Pass] Packet verified and sanitized for Hex ID [{beacon_id}]")
        
        initial_graph_state = {
            "beacon_hex_id": beacon_id,
            "current_phase": "INGRESS",
            "telemetry_profile": {"frequency_tracking_data": event_payload.get("frequency_tracking_data")},
            "computed_latitude": 0.0,
            "computed_longitude": 0.0,
            "nearby_assets": [],
            "dispatch_receipts": [],
            "verification_status": "UNVERIFIED",
            "execution_logs": [f"Ingress Bus: Authenticated & sanitized payload accepted at edge gateway."]
        }
        
        # Run safely through the multi-agent decision steps
        # Catch downstream orchestration engine database write exceptions explicitly!
        try:
            print(f"Invoking multi-agent decision tree engine for Hex ID [{beacon_id}]...")
            final_state = sar_orchestrator_engine.invoke(initial_graph_state)

            # Pass output through the probabilistic Ollama container node text tier safely
            final_state = run_llm_operator_briefing(final_state)

            log_filepath = commit_mission_audit_trace(final_state)
            spawn_llm_briefing_thread(beacon_id, final_state.get("verification_status"), log_filepath)

            # Trigger the rolling retention window filter instantly!
            # Keeps only the 10 newest json files on disk automatically.
            cleanup_old_audit_logs(max_kept_files=10)

            print("----------------------------------------------------------------------")
            print(f"🏁 PIPELINE CONCLUSION STATUS: {final_state.get('verification_status')}")
            print("----------------------------------------------------------------------\n")
            
        except Exception as orchestrator_error:
            import traceback
            print("\n🚨 [ORCHESTRATOR ENGINE SYSTEM CRASH DETECTED] 🚨")
            print(f"Exception Message: {orchestrator_error}")
            print("Detailed Execution Traceback Stack:")
            traceback.print_exc()
            print("----------------------------------------------------------------------\n")
            continue # Ensure the worker loops safely onto the next event stream instead of killing the container

if __name__ == "__main__":
    start_ingress_worker()