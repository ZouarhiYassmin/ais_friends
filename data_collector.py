import os
import json
import asyncio
import pandas as pd
from datetime import datetime, timezone
import websockets

API_KEY = os.environ["AISSTREAM_API_KEY"]


VESSELS = [
    {"mmsi": "311000789", "name": "african_puffin"}
]

os.makedirs("data", exist_ok=True)

async def fetch_vessel(mmsi: str, name: str):
    url = "wss://stream.aisstream.io/v0/stream"

    subscribe_msg = {
        "APIKey": API_KEY,
        "BoundingBoxes": [[[-90, -180], [90, 180]]],
        "FilterMessageTypes": ["PositionReport"],
        "MMSI": [mmsi]   
    }

    print(f"[{name}] Connecting for MMSI {mmsi}...")

    try:
        async with websockets.connect(url) as ws:
            await ws.send(json.dumps(subscribe_msg))

            
            raw = await asyncio.wait_for(ws.recv(), timeout=45)
            data = json.loads(raw)

            received_mmsi = str(data.get("MetaData", {}).get("MMSI", ""))
            if received_mmsi != mmsi:
                print(f"[{name}] Wrong MMSI received ({received_mmsi}), skipping.")
                return

            msg = data.get("Message", {}).get("PositionReport", {})
            meta = data.get("MetaData", {})

            row = {
                "timestamp":   datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                "mmsi":        mmsi,
                "ship_name":   meta.get("ShipName", name).strip(),
                "latitude":    msg.get("Latitude"),
                "longitude":   msg.get("Longitude"),
                "speed_knots": msg.get("Sog"),
                "course":      msg.get("Cog"),
                "heading":     msg.get("TrueHeading"),
                "nav_status":  msg.get("NavigationalStatus"),
            }

            print(f"[{name}] ✅ lat={row['latitude']}, lon={row['longitude']}, speed={row['speed_knots']}kn")

          
            csv_file = f"data/{name}_ais.csv"

            if os.path.exists(csv_file) and os.path.getsize(csv_file) > 0:
                old_df = pd.read_csv(csv_file)
                combined_df = pd.concat([old_df, pd.DataFrame([row])], ignore_index=True)
            else:
                combined_df = pd.DataFrame([row])


            combined_df = combined_df.drop_duplicates(subset=["timestamp"])
            combined_df.to_csv(csv_file, index=False)
            print(f"[{name}] Saved to {csv_file} — Total rows: {len(combined_df)}")

    except asyncio.TimeoutError:
        print(f"[{name}] ⚠️ No data received in 45s — vessel may be offline or out of coverage.")
    except Exception as e:
        print(f"[{name}] ❌ Error: {e}")

async def main():
  
    await asyncio.gather(*[fetch_vessel(v["mmsi"], v["name"]) for v in VESSELS])

asyncio.run(main())
