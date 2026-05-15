#!/usr/bin/env python3
import os
import requests
import time

BASE_URL = "http://127.0.0.1:6070"
SERVER = f"{BASE_URL}/download_since"
LATEST_ID_URL = f"{BASE_URL}/latest_id"

# Save location
CSV_FILE = "imu_data.csv"
STATE_FILE = "imu_data.last_id"

# Poll interval
POLL_INTERVAL = 5

# Allow large CSV exports enough time to finish.
DOWNLOAD_TIMEOUT = 60

# Minimum bytes to consider valid
MIN_BYTES = 50


HEADER = "timestamp(YYYY-MM-DD HH:MM:SS:ms),ax,ay,az,gx,gy,gz\n"


def read_last_id():
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return int(f.read().strip() or "0")
    except (FileNotFoundError, ValueError):
        return None


def write_last_id(last_id):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        f.write(str(last_id))


def ensure_csv_header():
    if not os.path.exists(CSV_FILE) or os.path.getsize(CSV_FILE) == 0:
        with open(CSV_FILE, "w", encoding="utf-8", newline="") as f:
            f.write(HEADER)


def initialize_last_id():
    saved_id = read_last_id()
    if saved_id is not None:
        return saved_id

    try:
        latest = requests.get(LATEST_ID_URL, timeout=DOWNLOAD_TIMEOUT)
        latest.raise_for_status()
        last_id = int(latest.text.strip() or "0")
        write_last_id(last_id)
        ensure_csv_header()
        print(f"[VPS] Starting incremental sync after existing row id {last_id}")
        return last_id
    except Exception as e:
        print(f"[VPS] Could not initialize last id: {e}")
        return 0


print("[VPS] Waiting for laptop server...")
last_id = initialize_last_id()

while True:
    try:
        r = requests.get(
            SERVER,
            params={"after_id": last_id},
            timeout=DOWNLOAD_TIMEOUT
        )

        if r.status_code == 204:
            last_id = int(r.headers.get("X-Last-Id", last_id))
            write_last_id(last_id)
            print("[VPS] No new rows")
        elif r.status_code == 200 and r.content:
            ensure_csv_header()
            with open(CSV_FILE, "ab") as f:
                f.write(r.content)

            new_last_id = int(r.headers.get("X-Last-Id", last_id))
            added = new_last_id - last_id
            last_id = new_last_id
            write_last_id(last_id)

            size_kb = os.path.getsize(CSV_FILE) / 1024
            print(
                f"[VPS] CSV appended {added:,} rows "
                f"({size_kb:.1f} KB total)"
            )
        else:
            print(f"[VPS] No data yet (HTTP {r.status_code})")
    except Exception as e:
        print(f"[VPS] Server unreachable: {e}")
    time.sleep(POLL_INTERVAL)
