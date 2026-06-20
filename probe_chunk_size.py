"""
Probe script — finds the maximum candles the broker returns per chunk.
Tests offset values from 3600 up to 86400 and prints candle count per chunk.

Usage:
    python probe_chunk_size.py
"""
import os
import sys
import time
import asyncio
import json
import logging

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

sys.path.insert(0, ".")
from pyquotex.stable_api import Quotex

logging.basicConfig(level=logging.WARNING)

EMAIL    = os.getenv("QUOTEX_EMAIL", "")
PASSWORD = os.getenv("QUOTEX_PASSWORD", "")

ASSET  = "EURUSD_otc"
PERIOD = 60  # 1m candles

# Offsets to probe (seconds of history per chunk request)
OFFSETS = [3600, 7200, 14400, 28800, 43200, 86400]

TIMEOUT = 15


async def fetch_chunk(client, asset, period, fetch_time, offset):
    browser_index = int(time.time() * 100)
    client.api.candles.candles_data = None
    client.api.candle_v2_data[asset] = None
    client.api._temp_status = ""

    payload = {
        "asset": asset,
        "index": browser_index,
        "time": fetch_time,
        "offset": offset,
        "period": period,
    }
    ws_msg = f'42["history/load",{json.dumps(payload)}]'
    client.api.send_websocket_request(ws_msg)

    start = time.time()
    v2_data = None
    while time.time() - start < TIMEOUT:
        v2_data = client.api.candle_v2_data.get(asset)
        if v2_data and v2_data.get("index") == browser_index:
            break
        await asyncio.sleep(0.05)

    if not v2_data or v2_data.get("index") != browser_index:
        return None

    raw = v2_data.get("data", []) or v2_data.get("candles", [])
    return raw


async def main():
    client = Quotex(email=EMAIL, password=PASSWORD, lang="en")
    client.debug_ws_enable = False

    check, msg = await client.connect()
    if not check:
        print(f"Connection FAILED: {msg}")
        return

    client.start_candles_stream(ASSET, PERIOD)
    await asyncio.sleep(1)  # let stream stabilise

    fetch_time = int(time.time()) - 300  # anchor 5 min ago to avoid live edge noise

    print(f"\n{'Offset (s)':>12}  {'Offset (h)':>10}  {'Candles returned':>18}  {'Unique time span':>20}")
    print("-" * 70)

    for offset in OFFSETS:
        raw = await fetch_chunk(client, ASSET, PERIOD, fetch_time, offset)
        if raw is None:
            print(f"{offset:>12}  {offset/3600:>10.1f}h  {'TIMEOUT/NO DATA':>18}")
            continue

        count = len(raw)
        if count > 0:
            times = sorted(c["time"] for c in raw)
            span_s = times[-1] - times[0]
            span_h = span_s / 3600
            print(f"{offset:>12}  {offset/3600:>10.1f}h  {count:>18,}  {span_h:>18.2f}h  ({span_s}s)")
        else:
            print(f"{offset:>12}  {offset/3600:>10.1f}h  {'0 (empty)':>18}")

        await asyncio.sleep(0.3)

    print()
    await client.close()


if __name__ == "__main__":
    asyncio.run(main())
