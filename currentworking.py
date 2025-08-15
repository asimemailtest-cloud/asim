import json
import csv
import requests
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from zoneinfo import ZoneInfo
from threading import Lock

# ======================= CONFIG ========================= #
API_KEY = "1178eddd-8d3f-43d2-9733-f971a3897563"
HEADERS = {
    "Authorization": API_KEY,
    "Content-Type": "application/json",
    "Accept": "application/json"
}
GEOJSON_FILE = "pol.geojson"

OUTPUT_DIR = Path("outputs");          OUTPUT_DIR.mkdir(exist_ok=True)
TXT_DIR    = Path("advertiser_ids");   TXT_DIR.mkdir(exist_ok=True)
ERROR_LOG  = Path("error_log.csv")

MAX_WORKERS          = 8
DEVICE_LIMIT         = 100_000
THRESHOLD_TO_SPLIT   = 90_000
CHUNK_DAYS           = 15

SUMMARY = []
log_lock = Lock()
# ======================================================== #

# ------------ utility helpers ---------------- #
def dt_utc(y, m, d):
    return datetime(y, m, d, tzinfo=ZoneInfo("UTC"))

def to_epoch_ms(dt: datetime):
    if dt.hour == 0 and dt.minute == 0 and dt.second == 0:
        dt = dt.replace(hour=23, minute=59, second=59)
    return int(dt.timestamp() * 1000)

def clean_name(name: str) -> str:
    return (
        name.replace(" ", "_")
            .replace("/", "_")
            .replace("-", "")
            .replace("\\", "_")
    )

def build_month_intervals():
    start = dt_utc(2025, 7, 1)
    end   = dt_utc(2025, 8, 9)
    intervals = []
    current = start
    while current <= end:
        nx_year  = current.year + (current.month // 12)
        nx_month = current.month % 12 + 1
        next_month_start = dt_utc(nx_year, nx_month, 1)
        interval_end = min(next_month_start - timedelta(days=1), end)
        intervals.append((current, interval_end))
        current = next_month_start
    return intervals
# -------------------------------------------------------- #

# -------- error logging (thread‑safe) ------------------- #
def log_error(poly_name, start_dt, end_dt, err_msg, status_code=""):
    with log_lock:
        first_write = not ERROR_LOG.exists()
        with open(ERROR_LOG, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            if first_write:
                writer.writerow(["Polygon", "Start", "End", "Status", "Error"])
            writer.writerow([
                poly_name,
                start_dt.strftime("%Y-%m-%d"),
                end_dt.strftime("%Y-%m-%d"),
                status_code,
                err_msg[:400]
            ])
# -------------------------------------------------------- #

# ----------- API call & processing ---------------------- #
def call_api(geometry, start_dt, end_dt):
    payload = {
        "type": "FeatureCollection",
        "features": [{
            "type": "Feature",
            "geometry": geometry,
            "properties": {
                "startDateTimeEpochMS": to_epoch_ms(start_dt),
                "endDateTimeEpochMS":   to_epoch_ms(end_dt)
            }
        }]
    }
    resp = requests.post(
        "https://api.gravyanalytics.com/v1.1/areas/devices",
        headers=HEADERS,
        json=payload,
        timeout=300
    )
    return resp.json(), resp.status_code


def process_interval(feature, start_dt, end_dt, polygon_name, index):
    geometry = feature["geometry"]
    name_clean = clean_name(polygon_name)

    try:
        data, status = call_api(geometry, start_dt, end_dt)
    except Exception as e:
        log_error(polygon_name, start_dt, end_dt, str(e))
        return set()

    if not (200 <= status < 300):
        log_error(polygon_name, start_dt, end_dt, json.dumps(data)[:400], status_code=status)
        return set()

    ids = {
        d.get("advertiserID")
        for feat in data.get("features", [])
        for d in feat.get("properties", {}).get("devices", [])
        if d.get("advertiserID")
    }

    interval_days = (end_dt - start_dt).days + 1
    if len(ids) > THRESHOLD_TO_SPLIT and interval_days > CHUNK_DAYS:
        mid_dt = start_dt + timedelta(days=interval_days // 2)
        print(f"⚠️  {polygon_name} {start_dt:%Y-%m-%d}–{end_dt:%Y-%m-%d} → {len(ids):,} devices — splitting into 15‑day chunks")
        left_ids  = process_interval(feature, start_dt, mid_dt, polygon_name, index)
        right_ids = process_interval(feature, mid_dt + timedelta(days=1), end_dt, polygon_name, index)
        return left_ids.union(right_ids)

    label = f"{name_clean}_{index:04}_{start_dt:%Y%m%d}_{end_dt:%Y%m%d}"
    json_path = OUTPUT_DIR / f"{label}.json"
    with open(json_path, "w") as f:
        json.dump(data, f, indent=2)

    txt_path = TXT_DIR / f"{label}.txt"
    with open(txt_path, "w") as f:
        for aid in sorted(ids):
            f.write(f"{aid}\n")

    print(f"✅ {polygon_name} {start_dt:%Y-%m-%d} → {end_dt:%Y-%m-%d}: {len(ids):,} devices")
    return ids
# -------------------------------------------------------- #

# --------------- THREAD WORKER -------------------------- #
def worker(index, feature, start_dt, end_dt):
    props = feature.get("properties", {})
    polygon_name = (
        props.get("Name of the business") or
        props.get("Name") or
        props.get("name") or
        f"polygon{index:02}"
    )
    devices = process_interval(feature, start_dt, end_dt, polygon_name, index)

    SUMMARY.append({
        "Polygon": polygon_name,
        "Start":   start_dt.strftime("%Y-%m-%d"),
        "End":     end_dt.strftime("%Y-%m-%d"),
        "Unique Devices": len(devices)
    })
# -------------------------------------------------------- #

# ================= MAIN EXECUTION ======================= #
if __name__ == "__main__":
    with open(GEOJSON_FILE, "r", encoding="utf-8") as f:
        features = json.load(f).get("features", [])

    month_intervals = build_month_intervals()

    tasks = [
        (i, feature, start_dt, end_dt)
        for i, feature in enumerate(features)
        for start_dt, end_dt in month_intervals
    ]

    print(f"🚀 Launching {len(tasks)} monthly requests with {MAX_WORKERS} threads…")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = [pool.submit(worker, i, feat, s, e) for i, feat, s, e in tasks]
        for _ in as_completed(futures):
            pass

    pd.DataFrame(SUMMARY).to_csv("device_counts_summary.csv", index=False)
    print("📊 device_counts_summary.csv saved")
    if ERROR_LOG.exists():
        print(f"⚠️  Errors were logged to {ERROR_LOG}")
    else:
        print("🎉 No API errors encountered")
