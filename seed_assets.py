import psycopg2

DB_PARAMS = {
    "host": "localhost", "user": "sar_admin", "password": "SecretRescuePassword123!", "database": "sar_mission_control", "port": 5432
}

def seed_data():
    assets = [
        # (ID, Name, Type, Latitude, Longitude)
        ("CG-HELO-6501", "Rescue Dolphin 6501", "HELICOPTER", 34.0522, -118.2437), # Base location: Los Angeles
        ("CG-CUTTER-751", "USCGC Waesche", "CUTTER", 33.7455, -118.2612),       # Near LA Port
        ("CG-HELO-6502", "Rescue Dolphin 6502", "HELICOPTER", 32.7157, -117.1611)  # Further down in San Diego
    ]
    
    conn = psycopg2.connect(**DB_PARAMS)
    cursor = conn.cursor()
    
    for asset_id, name, atype, lat, lon in assets:
        # ST_SetSRID converts numbers into a true spatial point object geometry 
        cursor.execute("""
            INSERT INTO allocated_assets (asset_id, asset_name, asset_type, current_latitude, current_longitude, current_location)
            VALUES (%s, %s, %s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326))
            ON CONFLICT (asset_id) DO UPDATE SET 
                current_latitude = EXCLUDED.current_latitude,
                current_longitude = EXCLUDED.current_longitude,
                current_location = EXCLUDED.current_location;
        """, (asset_id, name, atype, lat, lon, lon, lat)) # PostGIS takes Longitude first in ST_MakePoint
        
    conn.commit()
    print("✅ Seeded default rescue assets into PostGIS repository.")
    cursor.close()
    conn.close()

if __name__ == "__main__":
    seed_data()