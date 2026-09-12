import sys
import psycopg2
import redis
from kafka import KafkaProducer

print("Starting Complete SAR Core Environment Verification...")

# 1. PostGIS Database Checks
try:
    db_conn = psycopg2.connect(
        host="localhost", user="sar_admin", password="SecretRescuePassword123!", database="sar_mission_control"
    )
    cursor = db_conn.cursor()
    cursor.execute("SELECT PostGIS_Version();")
    print(f"✅ PostGIS DB Connection Operational: {cursor.fetchone()[0]}")
    cursor.close()
    db_conn.close()
except Exception as e:
    print(f"❌ PostGIS Database Check Failed: {e}")
    sys.exit(1)

# 2. Redis Cache Checks
try:
    cache_conn = redis.Redis(host='localhost', port=6379, decode_responses=True)
    cache_conn.set('mcp_infra_status', 'READY')
    if cache_conn.get('mcp_infra_status') == 'READY':
        print("✅ Redis Shared Mission State Cache Operational.")
    cache_conn.close()
except Exception as e:
    print(f"❌ Redis Cache Check Failed: {e}")
    sys.exit(1)

# 3. Apache Kafka Streaming Checks
try:
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'], request_timeout_ms=2000)
    if producer.bootstrap_connected():
        print("✅ Apache Kafka Infrastructure Connected & Accepting Streams.")
    producer.close()
except Exception as e:
    print(f"❌ Kafka Event Bus Check Failed: {e}")
    sys.exit(1)

print("\n🚀 Phase 1 complete! Environment successfully configured with Kafka.")