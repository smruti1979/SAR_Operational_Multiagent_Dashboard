import json
import time
import hmac
import hashlib
import random
from kafka import KafkaProducer

KAFKA_BOOTSTRAP_SERVERS = ["127.0.0.1:9092"]
TARGET_INGRESS_TOPIC = "satellite-ingress-raw"
SHARED_MCC_SECRET = b"CospasSarsatOperationalSecretKey2026!"

# A pool of realistic distress profiles to generate diverse variations
BEACON_PROFILES = [
    {"id": "ADCD4023B340001", "node": "MEOSAR_SARSAT_04", "base_lat": 31.5200, "base_lon": -119.8500, "type": "Emergency"},
    {"id": "AD3E4F67B12A89C", "node": "LEOSAR_SARSAT_11", "base_lat": 33.7455, "base_lon": -118.2612, "type": "Coastal Transit"},
    {"id": "FFFE2F4A8001234", "node": "GEOSAR_GOES_16", "base_lat": 32.7157, "base_lon": -117.1611, "type": "Aviation Transit"},
    {"id": "BCDE500123ABCDE", "node": "MEOSAR_SARSAT_09", "base_lat": 34.0522, "base_lon": -118.2437, "type": "Harbor Signature"}
]

def generate_random_alert():
    producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    
    # 1. Randomly pick an emergency asset configuration profile
    profile = random.choice(BEACON_PROFILES)
    beacon_id = profile["id"]
    satellite_node = profile["node"]
    
    # 2. Add random drift jitter to coordinates to simulate real boat/plane movement paths
    drift_lat_1 = profile["base_lat"] + random.uniform(-0.02, 0.02)
    drift_lon_1 = profile["base_lon"] + random.uniform(-0.02, 0.02)
    drift_lat_2 = drift_lat_1 + random.uniform(-0.005, 0.005)
    drift_lon_2 = drift_lon_1 + random.uniform(-0.005, 0.005)
    
    mock_satellite_burst = {
        "beacon_hex_id": beacon_id,
        "satellite_node": satellite_node,
        "received_timestamp": int(time.time()),
        "frequency_tracking_data": [
            {"timestamp": int(time.time()) - 300, "lat": round(drift_lat_1, 4), "lon": round(drift_lon_1, 4)},
            {"timestamp": int(time.time()),       "lat": round(drift_lat_2, 4), "lon": round(drift_lon_2, 4)}
        ]
    }
    
    # 3. Serialize and sign data payloads with structural HMAC codes safely
    payload_bytes = json.dumps(mock_satellite_burst).encode('utf-8')
    generated_hmac = hmac.new(SHARED_MCC_SECRET, payload_bytes, hashlib.sha256)
    signature_string = generated_hmac.hexdigest()
    
    message_headers = [("mcc-signature", signature_string.encode('utf-8'))]
    
    print(f"📤 [{profile['type']}] Sending signed burst for Hex ID [{beacon_id}] via {satellite_node}...")
    future = producer.send(TARGET_INGRESS_TOPIC, value=payload_bytes, headers=message_headers)
    metadata = future.get(timeout=10)
    
    print(f"   -> Part: [{metadata.partition}], Offset: [{metadata.offset}], Lat: {round(drift_lat_2,4)}, Lon: {round(drift_lon_2,4)}")
    producer.close()

def start_continuous_stream(interval_seconds: int = 8):
    print("======================================================================")
    print(f"🚀 CONTINUOUS SATELLITE BURST GENERATOR ONLINE (Interval: {interval_seconds}s)")
    print("   Press Ctrl + C to exit the data generator loop safely.")
    print("======================================================================\n")
    
    try:
        while True:
            generate_random_alert()
            print(f"⏳ Sleeping for {interval_seconds} seconds...\n")
            time.sleep(interval_seconds)
    except KeyboardInterrupt:
        print("\n🛑 Simulation stream stopped by administrative operator.")

if __name__ == "__main__":
    # Triggers a continuous random message flow loop every 8 seconds
    start_continuous_stream(interval_seconds=8)