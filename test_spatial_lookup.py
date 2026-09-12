import psycopg2

DB_PARAMS = {
    "host": "localhost", "user": "sar_admin", "password": "SecretRescuePassword123!", "database": "sar_mission_control", "port": 5432
}

# Simulated Emergency Coordinates: Just off the coast of LA
EMERGENCY_LAT = 33.9000
EMERGENCY_LON = -118.4000

conn = psycopg2.connect(**DB_PARAMS)
cursor = conn.cursor()

# ST_DistanceSphere returns exact real-world distance in meters between two coordinates on the globe
cursor.execute("""
    SELECT asset_id, asset_name, 
           ST_DistanceSphere(current_location, ST_SetSRID(ST_MakePoint(%s, %s), 4326)) / 1000.0 as distance_km
    FROM allocated_assets
    ORDER BY distance_km ASC;
""", (EMERGENCY_LON, EMERGENCY_LAT))

print(f"Listing closest rescue units to Distress Coordinate [{EMERGENCY_LAT}, {EMERGENCY_LON}]:")
for row in cursor.fetchall():
    print(f"🛸 Asset: {row[1]} ({row[0]}) | Distance to Target: {row[2]:.2f} KM")

cursor.close()
conn.close()