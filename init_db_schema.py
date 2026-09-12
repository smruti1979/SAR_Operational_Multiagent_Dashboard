import psycopg2

# Connection string pointing to your local ARM64 PostGIS container
DB_PARAMS = {
    "host": "localhost",
    "user": "sar_admin",
    "password": "SecretRescuePassword123!",
    "database": "sar_mission_control",
    "port": 5432
}

def initialize_database():
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        conn.autocommit = True
        cursor = conn.cursor()
        
        print("Initializing Spatial PostGIS Extensions...")
        # Enable PostGIS spatial functions inside this database instance
        cursor.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
        
        print("Creating SAR Incident Missions Table...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS incident_missions (
                mission_id SERIAL PRIMARY KEY,
                beacon_hex_id VARCHAR(15) NOT NULL UNIQUE,
                vessel_name VARCHAR(100),
                country_code INT,
                status VARCHAR(20) DEFAULT 'ACTIVE', -- ACTIVE, RESOLVED, FALSE_ALARM
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        print("Creating Beacon Telemetry Logs Table with Spatial Metrics...")
        # geom columns store actual GPS latitude/longitude as geometry points
        # 4326 denotes the standard WGS 84 spatial reference system used globally by GPS
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS beacon_telemetry_logs (
                log_id SERIAL PRIMARY KEY,
                beacon_hex_id VARCHAR(15) REFERENCES incident_missions(beacon_hex_id) ON DELETE CASCADE,
                computed_latitude NUMERIC(9,6) NOT NULL,
                computed_longitude NUMERIC(9,6) NOT NULL,
                beacon_location GEOMETRY(Point, 4326), 
                signal_strength_db NUMERIC(4,2),
                received_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
        print("Creating Allocated Assets Table...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS allocated_assets (
                asset_id VARCHAR(30) PRIMARY KEY,
                asset_name VARCHAR(100) NOT NULL,
                asset_type VARCHAR(20) NOT NULL, -- HELICOPTER, CUTTER, FIXED_WING
                current_latitude NUMERIC(9,6),
                current_longitude NUMERIC(9,6),
                current_location GEOMETRY(Point, 4326),
                availability_status VARCHAR(20) DEFAULT 'AVAILABLE', -- AVAILABLE, DEPLOYED, MAINTENANCE
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        print("Generating Spatial Spatial Performance Indexes...")
        # GiST (Generalized Search Tree) indexes drastically accelerate geospatial queries (e.g. distance checks)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_beacon_location ON beacon_telemetry_logs USING gist(beacon_location);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_asset_location ON allocated_assets USING gist(current_location);")

        print("✅ PostGIS Schema Initialization Complete!")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Failed to provision database schema: {e}")

if __name__ == "__main__":
    initialize_database()