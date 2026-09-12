import streamlit as st
from streamlit_autorefresh import st_autorefresh
import psycopg2
import pandas as pd
import json
import os
import glob
from kafka.admin import KafkaAdminClient
from kafka import TopicPartition
from kafka.structs import OffsetAndMetadata
from kafka import KafkaConsumer


# 1. MUST BE THE ABSOLUTE FIRST STREAMLIT COMMAND
st.set_page_config(page_title="COSPAS-SARSAT Operational Dashboard", layout="wide")

# 2. KICK START THE LIVE BACKGROUND HEARTBEAT EVENT LOOP
# Sets a stable 5000ms (5 second) auto-refresh cycle to query PostGIS automatically.
# We assign a unique key parameter to prevent the widget from losing focus state rules.
refresh_count = st_autorefresh(interval=5000, limit=None, key="sar_dashboard_heartbeat")

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
CSS_PATH = os.path.join(CURRENT_DIR, "style.css")

# Read and inject the external CSS file safely after page config initialization
try:
    with open(CSS_PATH) as f:
        st.html(f"<style>{f.read()}</style>")
except Exception:
    pass

DB_PARAMS = {
    "host": "sar-postgis-db", 
    "user": "sar_admin", 
    "password": "SecretRescuePassword123!", 
    "database": "sar_mission_control", 
    "port": 5432
}
AUDIT_LOG_DIR = "core_audit_logs"

def get_db_connection():
    return psycopg2.connect(**DB_PARAMS)

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import UnknownTopicOrPartitionError
import time

def purge_kafka_topic():
    """
    Deletes and recreates the satellite-ingress-raw topic.
    This is the reliable way to fully clear a topic's data when
    the installed kafka-python version doesn't support delete_records.
    """
    admin_client = KafkaAdminClient(bootstrap_servers="sar-kafka-broker:29092")
    topic_name = "satellite-ingress-raw"
    try:
        # Capture current partition count so the recreated topic matches
        consumer = KafkaConsumer(bootstrap_servers="sar-kafka-broker:29092")
        partitions = consumer.partitions_for_topic(topic_name)
        num_partitions = len(partitions) if partitions else 3
        consumer.close()

        try:
            admin_client.delete_topics([topic_name])
            print(f"🗑️ [Purge] Deleted topic: {topic_name}")
        except UnknownTopicOrPartitionError:
            print(f"⚠️ [Purge] Topic {topic_name} did not exist, skipping delete.")

        # Deletion is async on the broker side — give it a moment before recreating
        time.sleep(2)

        admin_client.create_topics([
            NewTopic(name=topic_name, num_partitions=num_partitions, replication_factor=1)
        ])
        print(f"✅ [Purge] Recreated topic: {topic_name} with {num_partitions} partitions.")

    except Exception as e:
        import traceback
        print(f"❌ [Purge] Kafka topic recreate failed: {e}")
        traceback.print_exc()
        st.error(f"Kafka purge failed: {e}")

    finally:
        admin_client.close()

def purge_operational_database():
    """
    Cleans out tracking tables, resets autoincrementing indexes,
    purges local JSON trace files, and flushes Kafka topic queues.
    """
    try:
        # 1. Truncate relational tables inside PostGIS cleanly
        conn = get_db_connection()
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute("TRUNCATE TABLE beacon_telemetry_logs RESTART IDENTITY CASCADE;")
        cursor.execute("TRUNCATE TABLE incident_missions RESTART IDENTITY CASCADE;")
        cursor.close()
        conn.close()
        
        # 2. Flush out matching local JSON audit log dumps on disk
        purged_files_count = 0
        if os.path.exists(AUDIT_LOG_DIR):
            target_logs = glob.glob(os.path.join(AUDIT_LOG_DIR, "incident_*.json"))
            for log_file in target_logs:
                try:
                    os.remove(log_file)
                    purged_files_count += 1
                except Exception:
                    pass
        
        # THE CRITICAL INFRASTRUCTURE FIX:
        # Enforce that the empty directory is ALWAYS recreated instantly 
        # so the worker container never loses its target path destination!
        os.makedirs(AUDIT_LOG_DIR, exist_ok=True)

        # 3. Connect and wipe out the Kafka topic queue cleanly
        try:
            
            target_topic = "satellite-ingress-raw"
            target_group = "sar-ingestion-worker-group-v2"
            
            # Open a fast admin consumer session bound to the target worker group identifier
            consumer = KafkaConsumer(
                bootstrap_servers="sar-kafka-broker:29092",
                group_id=target_group,
                enable_auto_commit=False
            )
            
            # Isolate the exact partitions matching our ingress topic structure
            partitions = [TopicPartition(target_topic, p) for p in range(3)]  # Adjust the number of partitions as needed
            
            # Explicitly bind the consumer to those partitions
            consumer.assign(partitions)
            
            # 🎯 THE CRITICAL MOVEMENT: Force the consumer read pointers to the absolute END of the topic log!
            consumer.seek_to_end()
            
            # Commit the new advanced offset coordinates permanently inside the Kafka broker log metadata
            offsets_dict = {p: consumer.position(p) for p in partitions}
            for p, offset in offsets_dict.items():
                consumer.commit(offsets={p: int(offset)})
                
            print(f"♻️ Kafka Ingress offsets safely forwarded past historical queue log messages: {offsets_dict}")
            consumer.close()
            
        except Exception as kafka_err:
            st.warning(f"Database wiped, but Kafka broker offset forwarding sequence dropped: {kafka_err}")
                
        st.success(f"🗑️ System Clean Complete! Wiped tables, deleted {purged_files_count} audit files, and reset Kafka streams.")
        st.rerun()
        
    except Exception as purge_err:
        st.error(f"Failed to execute infrastructure purge sequence: {purge_err}")


def fetch_latest_weather_from_audit():
    """
    Parses the most recent local JSON audit log to extract 
    the live Open-Meteo weather parameters computed by the Asset Agent.
    """
    try:
        log_files = glob.glob(os.path.join(AUDIT_LOG_DIR, "incident_*.json"))
        if not log_files:
            return None
            
        latest_file = max(log_files, key=os.path.getctime)
        
        with open(latest_file, "r", encoding="utf-8") as file_stream:
            log_data = json.load(file_stream)
            
            # Step down into the nested metrics structure safely
            resources = log_data.get("allocated_resources", [])
            if resources and isinstance(resources, list) and len(resources) > 0:
                # Pull out item dictionary index 0 out from the array stack
                primary_asset = resources[0]
                return primary_asset.get("live_environmental_metrics", None)
    except Exception:
        pass
    return None

# ======================================================================
# SIDEBAR SYSTEM CONTROL MODULE
# ======================================================================
with st.sidebar:
    st.markdown("### Infrastructure Admin Panel")
    st.caption("COSPAS-SARSAT Node Management Engine")
    st.markdown("---")
    
    st.markdown("**Database Clean Operations**")
    st.caption("Removes all incident rows, resets database metrics, and flushes local JSON trace dumps.")
    
    # Render the clear button widget
    if st.button("Purge System Logs", type="primary", use_container_width=True):
        purge_operational_database()
        purge_kafka_topic()

st.html('<h1 class="centered-title">COSPAS-SARSAT SAR Operational Dashboard</h1>')
st.html('<h4 class="centered-subheader">Real-time Multi-Agent Incident Tracking Interface</h4>')
st.markdown("---")

# 2. Fetch data aggregates out from the operational database layers
try:
    conn = get_db_connection()
    df_missions = pd.read_sql_query(
        "SELECT mission_id, beacon_hex_id, vessel_name, status, updated_at FROM incident_missions ORDER BY updated_at DESC;", 
        conn
    )
    df_assets = pd.read_sql_query(
        "SELECT asset_id, asset_name, asset_type, current_latitude, current_longitude, availability_status FROM allocated_assets;", 
        conn
    )
    conn.close()
except Exception as e:
    st.error(f"Failed to bridge command console to PostGIS cluster metrics: {e}")
    st.stop()

# 3. Extract live weather vectors via the audit file tracker utility
live_weather = fetch_latest_weather_from_audit()

# 4. Main KPI Metrics Layout Group
col1, col2, col3, col4 = st.columns(4)
with col1:
    if not df_missions.empty:
        active_mask = df_missions['status'].str.upper().isin(['ACTIVE', 'VERIFIED_EMERGENCY'])
        active_count = len(df_missions[active_mask])
    else:
        active_count = 0
    st.metric(label="Active SAR Missions", value=active_count)

with col2:
    total_assets = len(df_assets) if not df_assets.empty else 0
    st.metric(label="Total Tracked Rescue Units", value=total_assets)

with col3:
    avail_assets = len(df_assets[df_assets['availability_status'].str.upper() == 'AVAILABLE']) if not df_assets.empty else 0
    st.metric(label="Available Responders", value=avail_assets)

with col4:
    if live_weather:
        wind_speed = live_weather.get("wind_velocity_knots", 0.0)
        risk_index = live_weather.get("operational_risk_index", "UNKNOWN").replace("_", " ")
        st.metric(label=f"Target Wind: {risk_index}", value=f"{wind_speed} KTS")
    else:
        st.metric(label="Target Wind Status", value="Awaiting Data")

st.markdown("### Active Distress Incidents Log")
if not df_missions.empty:
    st.dataframe(df_missions, use_container_width=True)
    
    total_records = len(df_missions)
    st.caption(f"Showing all {total_records} tracking beacon frequencies registered in PostGIS cluster layers.")
else:
    st.info("No active satellite distress signals detected on the network bus.")

# 5. Two-Column Details Group
col_left, col_right = st.columns(2)

with col_left:
    st.markdown("### Fleet Resource Allocation Status")
    if not df_assets.empty:
        st.dataframe(df_assets, use_container_width=True)
    else:
        st.info("No fleet resources currently indexed in PostGIS database.")

with col_right:
    st.markdown("### Generated NAVTEX Bulletin Preview")
    
    search_pattern = os.path.join(AUDIT_LOG_DIR, "incident_*.json")
    log_files = glob.glob(search_pattern)
    
    if log_files:
        latest_file = max(log_files, key=os.path.getmtime)
        
        # Initialize empty defaults to prevent rendering unbound variable bugs
        latest_hex = "UNKNOWN"
        llm_text = None
        
        # Wrap file reads to instantly skip if the file is locked by the worker!
        try:
            with open(latest_file, "r", encoding="utf-8") as f:
                log_data = json.load(f)
                latest_hex = log_data.get("incident_identification", {}).get("beacon_hex_id", "UNKNOWN_HEX")
                llm_text = log_data.get("llm_briefing_text")
        except (IOError, json.JSONDecodeError):
            # If the worker container is actively locking/writing the file, skip this frame safely!
            llm_text = "Processing"

        # Render spinner states vs completed AI text briefs dynamically
        if not llm_text or "Processing" in str(llm_text):
            with st.spinner("🛰️ Ingress Node Alert: Ollama Engine is actively compiling tactical summaries..."):
                st.text_area(
                    label="Ollama qwen2.5:1.5b Processing Output Stream:", 
                    value="Awaiting execution tokens from local CPU container tier...", 
                    height=350,
                    disabled=True
                )
        else:
            st.info(f"**Automated Emergency Handoff & NAVTEX Report (LLM Compute)**")
            st.text_area(label="Ollama qwen2.5:1.5b Processing Output Stream:", value=llm_text, height=350)
            
            st.download_button(
                label="Download Signed Radio Bulletin File",
                data=llm_text,
                file_name=f"NAVTEX_BULLETIN_{latest_hex[:6]}.txt",
                mime="text/plain",
                use_container_width=True
            )
    else:
        st.info("Awaiting verified signal telemetry payload to compile advisory transmission sheet.")