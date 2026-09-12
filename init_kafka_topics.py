from kafka.admin import KafkaAdminClient, NewTopic
import time

print("Initializing SAR Event Bus Streams...")

# Give Kafka a few seconds to warm up and bind ports if running fresh
time.sleep(3)

try:
    admin_client = KafkaAdminClient(
        bootstrap_servers="localhost:9092", 
        client_id='sar_infra_init'
    )

    # Core high-availability topic design for emergency workflows
    topic_list = [
        # Ingestion stream for multi-satellite feeds (Raw 406MHz Bursts)
        NewTopic(name="satellite-ingress-raw", num_partitions=3, replication_factor=1),
        # Telemetry parsed payloads outputted by our first MCP server
        NewTopic(name="telemetry-parsed", num_partitions=2, replication_factor=1),
        # Agency communication routing events and dispatch flags
        NewTopic(name="notification-dispatches", num_partitions=2, replication_factor=1)
    ]

    # Handle recreation cleanly if running scripts multiple times
    existing_topics = admin_client.list_topics()
    topics_to_create = [t for t in topic_list if t.name not in existing_topics]

    if topics_to_create:
        admin_client.create_topics(new_topics=topics_to_create, validate_only=False)
        print(f"✅ Successfully created SAR topics: {[t.name for t in topics_to_create]}")
    else:
        print("ℹ️ SAR topics already exist. Skipping creation.")
        
    admin_client.close()

except Exception as e:
    print(f"❌ Failed to verify/initialize Kafka Streams: {e}")