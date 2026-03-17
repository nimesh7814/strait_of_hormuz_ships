import asyncio
import websockets
import json
import os
from datetime import datetime, timezone
from clickhouse_driver import Client
from dotenv import load_dotenv
from shapely.geometry import Point, Polygon, shape

# Load .env for local development outside Docker
load_dotenv()

# ── CONFIG ─────────────────────────────────────────────────────────────────────
AIS_API_KEY = os.environ["AISAPIKEY"]
CH_HOST     = os.environ.get("CLICKHOUSE_HOST",     "localhost")
CH_USER     = os.environ.get("CLICKHOUSE_USER",     "hormuz")
CH_PASS     = os.environ.get("CLICKHOUSE_PASSWORD", "hormuzFTW2024!")
CH_DB       = os.environ.get("CLICKHOUSE_DB",       "vessels_tracking")

# ── CLICKHOUSE SETUP ───────────────────────────────────────────────────────────
client = Client(host=CH_HOST, user=CH_USER, password=CH_PASS)

client.execute(f"CREATE DATABASE IF NOT EXISTS {CH_DB}")

client.execute(f"""
CREATE TABLE IF NOT EXISTS {CH_DB}.ais_data (
    ts          DateTime64(3, 'UTC'),
    ship_id     UInt32,
    ship_name   String,
    latitude    Float32,
    longitude   Float32,
    speed       Float32,
    heading     Float32,
    course      Float32,
    nav_status  UInt8,
    nav_label   String
) ENGINE = MergeTree()
ORDER BY ts
""")

print(f"ClickHouse ready — writing to {CH_DB}.ais_data")

# ── LOAD GEOJSON BOUNDARY ─────────────────────────────────────────────────────
def load_geojson_polygon(geojson_path):
    """Extract polygon coordinates from GeoJSON file."""
    try:
        with open(geojson_path, 'r') as f:
            geojson = json.load(f)
        
        # Get the first feature
        if geojson.get('type') != 'FeatureCollection' or not geojson.get('features'):
            print(f"Warning: Invalid GeoJSON structure in {geojson_path}")
            return None
        
        feature = geojson['features'][0]
        geometry = feature.get('geometry')
        
        if not geometry or geometry.get('type') != 'Polygon':
            print(f"Warning: Expected Polygon geometry in {geojson_path}")
            return None
        
        # Use shapely's shape function to convert GeoJSON geometry to Polygon
        polygon = shape(geometry)
        coords = list(polygon.exterior.coords)
        print(f"Loaded Hormuz boundary polygon from GeoJSON with {len(coords)} vertices")
        return polygon
    except Exception as e:
        print(f"Error loading GeoJSON: {e}")
        return None

HORMUZ_POLYGON = load_geojson_polygon("/data/strait_of_hormuz_apprx_boundary.geojson") or load_geojson_polygon("data/strait_of_hormuz_apprx_boundary.geojson")

def is_in_hormuz(lat, lon):
    """Check if a coordinate is within the Hormuz study area."""
    if HORMUZ_POLYGON is None:
        return True  # if no polygon, accept all
    return HORMUZ_POLYGON.contains(Point(lon, lat))

# ── NAV STATUS LABELS ──────────────────────────────────────────────────────────
NAV_STATUS = {
    0:  "Underway (engine)",
    1:  "At anchor",
    2:  "Not under command",
    3:  "Restricted manoeuvrability",
    4:  "Constrained by draught",
    5:  "Moored",
    6:  "Aground",
    7:  "Fishing",
    8:  "Underway (sailing)",
    15: "Unknown",
}

# ── AIS STREAM ─────────────────────────────────────────────────────────────────
async def connect_ais_stream():
    url = "wss://stream.aisstream.io/v0/stream"
    subscribe = {
        "APIKey":             AIS_API_KEY,
        "BoundingBoxes":      [[[22.0, 54.0], [28.0, 62.0]]],
        "FilterMessageTypes": ["PositionReport"],
    }

    while True:
        try:
            async with websockets.connect(url) as ws:
                await ws.send(json.dumps(subscribe))
                print("AIS stream connected — listening for Hormuz vessels...")

                async for raw in ws:
                    msg  = json.loads(raw)
                    pos  = msg.get("Message", {}).get("PositionReport", {})
                    meta = msg.get("MetaData", {})

                    mmsi      = meta.get("MMSI")
                    lat       = pos.get("Latitude")
                    lon       = pos.get("Longitude")
                    ship_name = (meta.get("ShipName") or "Unknown").strip() or "Unknown"
                    speed     = pos.get("Sog",   0)
                    heading   = pos.get("TrueHeading", pos.get("Cog", 0))
                    course    = pos.get("Cog",   0)
                    nav_code  = pos.get("NavigationalStatus", 15)
                    nav_label = NAV_STATUS.get(nav_code, "Unknown")

                    if not (mmsi and lat and lon):
                        continue
                    
                    # Filter to Hormuz study area only
                    if not is_in_hormuz(lat, lon):
                        continue

                    now = datetime.now(timezone.utc)

                    print(
                        f"[{now.strftime('%H:%M:%S')}] "
                        f"{ship_name:<25} MMSI:{mmsi} "
                        f"Lat:{lat:.4f} Lon:{lon:.4f} "
                        f"Speed:{speed} kts  Status:{nav_label}"
                    )

                    client.execute(
                        f"""
                        INSERT INTO {CH_DB}.ais_data
                        (ts, ship_id, ship_name, latitude, longitude,
                         speed, heading, course, nav_status, nav_label)
                        VALUES
                        """,
                        [(now, mmsi, ship_name, lat, lon,
                          speed, heading, course, nav_code, nav_label)]
                    )

        except Exception as e:
            print(f"Connection error: {e}. Reconnecting in 10s...")
            await asyncio.sleep(10)

if __name__ == "__main__":
    asyncio.run(connect_ais_stream())