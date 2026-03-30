import os
import json
import asyncio
import pandas as pd
from datetime import datetime, timezone
import websockets

MMSI = "311000789"
API_KEY = os.environ["AISSTREAM_API_KEY"]  
CSV_FILE = "data/african_puffin_ais.csv"

os.makedirs("data", exist_ok=True)

async def fetch_ais():
    url = "wss://stream.aisstream.io/v0/stream"

    subscribe_msg = {
        "APIKey": API_KEY,
        "BoundingBoxes": [[[-90, -180], [90, 180]]],  # monde entier
        "FilterMessageTypes": ["PositionReport"],
        "MMSI": [MMSI]
    }

    print(f"Connecting to AISStream for MMSI {MMSI}...")

    async with websockets.connect(url) as ws:
        await ws.send(json.dumps(subscribe_msg))

        
        try:
            raw = await asyncio.wait_for(ws.recv(), timeout=30)
            data = json.loads(raw)

            msg = data.get("Message", {}).get("PositionReport", {})
            meta = data.get("MetaData", {})

            row = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "mmsi": meta.get("MMSI", MMSI),
                "ship_name": meta.get("ShipName", "AFRICAN PUFFIN").strip(),
                "latitude": msg.get("Latitude"),
                "longitude": msg.get("Longitude"),
                "speed": msg.get("Sog"),         # Speed Over Ground
                "course": msg.get("Cog"),         # Course Over Ground
                "heading": msg.get("TrueHeading"),
                "nav_status": msg.get("NavigationalStatus"),
            }

            print(f"Position received: lat={row['latitude']}, lon={row['longitude']}, speed={row['speed']}")
            return row

        except asyncio.TimeoutError:
            print("No position data received within 30s — vessel may be out of coverage.")
            return None

row = asyncio.run(fetch_ais())

if row is None:
    exit(0)

new_df = pd.DataFrame([row])

if os.path.exists(CSV_FILE) and os.path.getsize(CSV_FILE) > 0:
    old_df = pd.read_csv(CSV_FILE)
    combined_df = pd.concat([old_df, new_df], ignore_index=True)
    combined_df = combined_df.drop_duplicates(subset=["timestamp"])
else:
    combined_df = new_df

combined_df.to_csv(CSV_FILE, index=False)
print(f"Dataset updated: {CSV_FILE} — Total rows: {len(combined_df)}")



