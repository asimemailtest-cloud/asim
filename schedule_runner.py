import json
import csv
import argparse
import sys
from dataclasses import dataclass
from pathlib import Path
from datetime import datetime, date, time, timedelta
from typing import List, Dict, Iterable, Optional, Tuple, Set
import unicodedata
from urllib import request as urlrequest
from urllib.error import URLError, HTTPError

from zoneinfo import ZoneInfo
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

# ======================= CONFIG ========================= #
API_KEY = "1178eddd-8d3f-43d2-9733-f971a3897563"
HEADERS = {
    "Authorization": API_KEY,
    "Content-Type": "application/json",
    "Accept": "application/json",
}

OUTPUT_DIR = Path("outputs");       OUTPUT_DIR.mkdir(exist_ok=True)
TXT_DIR    = Path("advertiser_ids");TXT_DIR.mkdir(exist_ok=True)
ERROR_LOG  = Path("error_log.csv")

MAX_WORKERS_DEFAULT = 8
THRESHOLD_TO_SPLIT  = 90_000
CHUNK_DAYS          = 15

SUMMARY: List[Dict[str, object]] = []
log_lock = Lock()
# ======================================================== #

# ----------------- Data models -------------------------- #
@dataclass
class EventRow:
    polygon_name: str
    start_date: date
    end_date: date
    repetition: str  # Once | Daily | Specific | Weekly
    specific_days: List[int]  # list of weekday numbers 0=Mon..6=Sun
    timezone: ZoneInfo
    start_time: Optional[time]  # None means all-day
    end_time: Optional[time]    # None means all-day
    overnight_flag: bool

@dataclass
class Interval:
    polygon_name: str
    polygon_index: int
    start_dt: datetime  # tz-aware
    end_dt: datetime    # tz-aware
    geometry: dict

# ----------------- Utilities ---------------------------- #

def normalize_name(value: str) -> str:
    if value is None:
        return ""
    value = value.strip()
    if value.startswith("*"):
        value = value[1:].strip()
    value = unicodedata.normalize("NFKC", value)
    value = " ".join(value.split())
    return value.casefold()


def to_epoch_ms_utc(dt_local: datetime) -> int:
    return int(dt_local.astimezone(ZoneInfo("UTC")).timestamp() * 1000)


def clean_label(name: str) -> str:
    return (
        name.replace(" ", "_")
            .replace("/", "_")
            .replace("-", "")
            .replace("\\", "_")
    )

# ----------------- Error logging ------------------------ #

def log_error(poly_name: str, start_dt: datetime, end_dt: datetime, err_msg: str, status_code: str | int = "") -> None:
    with log_lock:
        first_write = not ERROR_LOG.exists()
        with open(ERROR_LOG, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            if first_write:
                writer.writerow(["Polygon", "Start", "End", "Status", "Error"])
            writer.writerow([
                poly_name,
                start_dt.astimezone(ZoneInfo("UTC")).strftime("%Y-%m-%d %H:%M:%S %Z"),
                end_dt.astimezone(ZoneInfo("UTC")).strftime("%Y-%m-%d %H:%M:%S %Z"),
                status_code,
                err_msg[:400],
            ])

# ----------------- CSV parsing -------------------------- #

WEEKDAY_NAME_TO_NUM = {
    "monday": 0, "mondays": 0,
    "tuesday": 1, "tuesdays": 1,
    "wednesday": 2, "wednesdays": 2,
    "thursday": 3, "thursdays": 3,
    "friday": 4, "fridays": 4,
    "saturday": 5, "saturdays": 5,
    "sunday": 6, "sundays": 6,
}

TIME_FORMATS = ["%H:%M:%S", "%H:%M", "%I:%M %p"]
DATE_FORMATS = ["%m/%d/%Y", "%Y-%m-%d"]


def _empty_like(value: object) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        s = value.strip()
        return s == "" or s.casefold() in {"nan", "nat", "none"}
    return False


def parse_time(value: object) -> Optional[time]:
    if _empty_like(value):
        return None
    s = str(value).strip()
    for fmt in TIME_FORMATS:
        try:
            return datetime.strptime(s, fmt).time()
        except Exception:
            continue
    return None


def parse_date(value: object) -> Optional[date]:
    if _empty_like(value):
        return None
    s = str(value).strip()
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(s, fmt).date()
        except Exception:
            continue
    try:
        return datetime.fromisoformat(s).date()
    except Exception:
        return None


def parse_specific_days(value: object) -> List[int]:
    if _empty_like(value):
        return []
    raw = str(value)
    parts = [p.strip() for p in raw.replace("\n", ",").split(",") if p.strip()]
    result: List[int] = []
    for p in parts:
        key = p.casefold()
        if key in WEEKDAY_NAME_TO_NUM:
            result.append(WEEKDAY_NAME_TO_NUM[key])
    return sorted(set(result))


def parse_events(csv_path: Path) -> List[EventRow]:
    events: List[EventRow] = []
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            polygon_name = str(row.get("Polygon Exact Name", "")).strip()
            if not polygon_name:
                continue
            start_d = parse_date(row.get("Timeframe Start"))
            end_d   = parse_date(row.get("Timeframe End"))
            if not start_d:
                continue
            if not end_d:
                end_d = start_d

            repetition_raw = str(row.get("Time Repitition Frame", "")).strip() or "Once"
            repetition = repetition_raw.capitalize()
            if repetition not in {"Once", "Daily", "Specific", "Weekly"}:
                if repetition.casefold() in {"once"}: repetition = "Once"
                elif repetition.casefold() in {"daily"}: repetition = "Daily"
                elif repetition.casefold() in {"specific", "specific day", "specific days"}: repetition = "Specific"
                elif repetition.casefold() in {"weekly", "week"}: repetition = "Weekly"
                else:
                    repetition = "Once"

            specific_days = parse_specific_days(row.get("Specific Day"))
            tz_name = str(row.get("TimeZone", "UTC")).strip() or "UTC"
            try:
                tz = ZoneInfo(tz_name)
            except Exception:
                tz = ZoneInfo("UTC")

            st_time = parse_time(row.get("Start Time (empty= All Day)"))
            en_time = parse_time(row.get("End Time (empty= All Day)"))
            overnight_raw = str(row.get("OverNight Timelap", "")).strip().lower()
            overnight_flag = overnight_raw in {"yes", "true", "y", "1"}

            events.append(EventRow(
                polygon_name=polygon_name,
                start_date=start_d,
                end_date=end_d,
                repetition=repetition,
                specific_days=specific_days,
                timezone=tz,
                start_time=st_time,
                end_time=en_time,
                overnight_flag=overnight_flag,
            ))
    return events

# ----------------- GeoJSON loading ---------------------- #

def load_geojson(geojson_path: Path) -> Tuple[List[dict], Dict[str, Tuple[int, dict]]]:
    with open(geojson_path, "r", encoding="utf-8") as f:
        gj = json.load(f)
    features: List[dict] = gj.get("features", [])
    name_to_feature: Dict[str, Tuple[int, dict]] = {}
    for idx, feat in enumerate(features):
        props = feat.get("properties", {})
        name = props.get("Name of the business") or props.get("Name") or props.get("name") or f"polygon{idx:02}"
        name_to_feature[normalize_name(name)] = (idx, feat)
    return features, name_to_feature

# ----------------- Schedule expansion ------------------- #

def daterange(start: date, end: date) -> Iterable[date]:
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def expand_event_to_intervals(ev: EventRow, name_to_feature: Dict[str, Tuple[int, dict]]) -> List[Interval]:
    key = normalize_name(ev.polygon_name)
    idx_feat = name_to_feature.get(key)
    if idx_feat is None:
        key2 = normalize_name(ev.polygon_name.lstrip("* "))
        idx_feat = name_to_feature.get(key2)
    if idx_feat is None:
        return []

    poly_index, feature = idx_feat
    geometry = feature.get("geometry")

    def build_daily_interval(day: date) -> Tuple[datetime, datetime]:
        if ev.start_time is None or ev.end_time is None:
            start_dt = datetime.combine(day, time(0, 0, 0), tzinfo=ev.timezone)
            end_dt   = datetime.combine(day, time(23, 59, 59), tzinfo=ev.timezone)
            return start_dt, end_dt
        start_dt = datetime.combine(day, ev.start_time, tzinfo=ev.timezone)
        end_dt   = datetime.combine(day, ev.end_time, tzinfo=ev.timezone)
        if ev.overnight_flag or end_dt <= start_dt:
            end_dt = datetime.combine(day + timedelta(days=1), ev.end_time, tzinfo=ev.timezone)
        return start_dt, end_dt

    intervals: List[Interval] = []
    rep = ev.repetition

    if rep == "Once":
        for d in daterange(ev.start_date, ev.end_date):
            s, e = build_daily_interval(d)
            intervals.append(Interval(ev.polygon_name, poly_index, s, e, geometry))
        return intervals

    if rep == "Daily":
        for d in daterange(ev.start_date, ev.end_date):
            s, e = build_daily_interval(d)
            intervals.append(Interval(ev.polygon_name, poly_index, s, e, geometry))
        return intervals

    if rep == "Specific":
        days = set(ev.specific_days)
        if not days:
            days = {ev.start_date.weekday()}
        for d in daterange(ev.start_date, ev.end_date):
            if d.weekday() in days:
                s, e = build_daily_interval(d)
                intervals.append(Interval(ev.polygon_name, poly_index, s, e, geometry))
        return intervals

    if rep == "Weekly":
        anchor_days = ev.specific_days if ev.specific_days else [ev.start_date.weekday()]
        anchor_days = sorted(set(anchor_days))
        for d in daterange(ev.start_date, ev.end_date):
            if d.weekday() in anchor_days:
                s, e = build_daily_interval(d)
                intervals.append(Interval(ev.polygon_name, poly_index, s, e, geometry))
        return intervals

    s, e = build_daily_interval(ev.start_date)
    intervals.append(Interval(ev.polygon_name, poly_index, s, e, geometry))
    return intervals

# ----------------- API call & processing ---------------- #

def http_post_json(url: str, headers: Dict[str, str], payload: dict) -> Tuple[dict, int]:
    data = json.dumps(payload).encode("utf-8")
    req = urlrequest.Request(url, data=data, method="POST")
    for k, v in headers.items():
        req.add_header(k, v)
    try:
        with urlrequest.urlopen(req, timeout=300) as resp:
            status = resp.getcode()
            body = resp.read().decode("utf-8")
            try:
                return json.loads(body), status
            except Exception:
                return {"raw": body}, status
    except HTTPError as e:
        try:
            body = e.read().decode("utf-8")
            parsed = json.loads(body)
        except Exception:
            parsed = {"error": str(e)}
        return parsed, e.code
    except URLError as e:
        return {"error": str(e)}, 0


def call_api(geometry: dict, start_dt: datetime, end_dt: datetime) -> Tuple[dict, int]:
    payload = {
        "type": "FeatureCollection",
        "features": [{
            "type": "Feature",
            "geometry": geometry,
            "properties": {
                "startDateTimeEpochMS": to_epoch_ms_utc(start_dt),
                "endDateTimeEpochMS":   to_epoch_ms_utc(end_dt),
            },
        }],
    }
    return http_post_json(
        "https://api.gravyanalytics.com/v1.1/areas/devices",
        headers=HEADERS,
        payload=payload,
    )


def process_interval(interval: Interval, index_within_poly: int, dry_run: bool = False) -> Set[str]:
    polygon_name = interval.polygon_name
    name_clean = clean_label(polygon_name)
    start_dt = interval.start_dt
    end_dt   = interval.end_dt

    if dry_run:
        print(f"PLAN {polygon_name} {start_dt:%Y-%m-%d %H:%M} → {end_dt:%Y-%m-%d %H:%M} ({start_dt.tzinfo})")
        return set()

    try:
        data, status = call_api(interval.geometry, start_dt, end_dt)
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

    interval_days = (end_dt.date() - start_dt.date()).days + 1
    if len(ids) > THRESHOLD_TO_SPLIT and interval_days > CHUNK_DAYS:
        mid_dt = start_dt + timedelta(days=interval_days // 2)
        print(f"⚠️  {polygon_name} {start_dt:%Y-%m-%d}–{end_dt:%Y-%m-%d} → {len(ids):,} devices — splitting into {CHUNK_DAYS}-day chunks")
        left_ids  = process_interval(Interval(polygon_name, interval.polygon_index, start_dt, mid_dt, interval.geometry), index_within_poly, dry_run)
        right_ids = process_interval(Interval(polygon_name, interval.polygon_index, mid_dt + timedelta(days=1), end_dt, interval.geometry), index_within_poly, dry_run)
        return left_ids.union(right_ids)

    label = f"{name_clean}_{interval.polygon_index:04}_{start_dt:%Y%m%dT%H%M}_{end_dt:%Y%m%dT%H%M}"
    json_path = OUTPUT_DIR / f"{label}.json"
    with open(json_path, "w") as f:
        json.dump(data, f, indent=2)

    txt_path = TXT_DIR / f"{label}.txt"
    with open(txt_path, "w") as f:
        for aid in sorted(ids):
            f.write(f"{aid}\n")

    print(f"✅ {polygon_name} {start_dt:%Y-%m-%d %H:%M} → {end_dt:%Y-%m-%d %H:%M}: {len(ids):,} devices")
    return ids

# ----------------- Main runner -------------------------- #

def find_default_geojson() -> Optional[Path]:
    candidates = list(Path(".").glob("*.geojson"))
    pol = Path("pol.geojson")
    if pol.exists():
        return pol
    if len(candidates) == 1:
        return candidates[0]
    if candidates:
        return sorted(candidates, key=lambda p: p.stat().st_size, reverse=True)[0]
    return None


def write_summary(summary_rows: List[Dict[str, object]], out_path: Path) -> None:
    if not summary_rows:
        return
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Polygon", "Start", "End", "Unique Devices"])
        for r in summary_rows:
            writer.writerow([r.get("Polygon"), r.get("Start"), r.get("End"), r.get("Unique Devices")])


def main() -> int:
    parser = argparse.ArgumentParser(description="Run scheduled area-device queries based on event CSV")
    parser.add_argument("--csv", dest="csv_path", default="event.csv", help="Path to events CSV")
    parser.add_argument("--geojson", dest="geojson_path", default=None, help="Path to polygons GeoJSON")
    parser.add_argument("--max-workers", dest="max_workers", type=int, default=MAX_WORKERS_DEFAULT)
    parser.add_argument("--dry-run", dest="dry_run", action="store_true", help="Print planned intervals without calling API")
    parser.add_argument("--from-date", dest="from_date", default=None, help="Optional inclusive start date filter (YYYY-MM-DD)")
    parser.add_argument("--to-date", dest="to_date", default=None, help="Optional inclusive end date filter (YYYY-MM-DD)")
    args = parser.parse_args()

    csv_path = Path(args.csv_path)
    if not csv_path.exists():
        print(f"CSV not found: {csv_path}", file=sys.stderr)
        return 1

    geojson_path = Path(args.geojson_path) if args.geojson_path else find_default_geojson()
    if not geojson_path or not geojson_path.exists():
        print("GeoJSON file not found. Provide --geojson path.", file=sys.stderr)
        return 1

    events = parse_events(csv_path)
    _, name_to_feature = load_geojson(geojson_path)

    all_intervals: List[Interval] = []

    from_date_filter: Optional[date] = None
    to_date_filter: Optional[date] = None
    if args.from_date:
        from_date_filter = parse_date(args.from_date)
    if args.to_date:
        to_date_filter = parse_date(args.to_date)

    for ev in events:
        expanded = expand_event_to_intervals(ev, name_to_feature)
        if not expanded:
            print(f"⚠️  Polygon not found in GeoJSON: {ev.polygon_name}")
            continue
        if from_date_filter or to_date_filter:
            filtered: List[Interval] = []
            for it in expanded:
                date_only = it.start_dt.date()
                if from_date_filter and date_only < from_date_filter:
                    continue
                if to_date_filter and date_only > to_date_filter:
                    continue
                filtered.append(it)
            expanded = filtered
        all_intervals.extend(expanded)

    if not all_intervals:
        print("No intervals to process.")
        return 0

    print(f"🚀 Launching {len(all_intervals)} requests with {args.max_workers} threads…")

    all_intervals.sort(key=lambda x: (x.polygon_index, x.start_dt))

    if args.dry_run:
        for idx, interval in enumerate(all_intervals):
            process_interval(interval, idx, dry_run=True)
        return 0

    with ThreadPoolExecutor(max_workers=args.max_workers) as pool:
        future_to_interval = {pool.submit(process_interval, interval, i): interval for i, interval in enumerate(all_intervals)}
        for fut in as_completed(future_to_interval):
            interval = future_to_interval[fut]
            try:
                ids = fut.result()
                SUMMARY.append({
                    "Polygon": interval.polygon_name,
                    "Start": interval.start_dt.strftime("%Y-%m-%d %H:%M"),
                    "End": interval.end_dt.strftime("%Y-%m-%d %H:%M"),
                    "Unique Devices": len(ids),
                })
            except Exception as e:
                log_error(interval.polygon_name, interval.start_dt, interval.end_dt, f"worker exception: {e}")

    write_summary(SUMMARY, Path("device_counts_summary.csv"))
    print("📊 device_counts_summary.csv saved")
    if ERROR_LOG.exists():
        print(f"⚠️  Errors were logged to {ERROR_LOG}")
    else:
        print("🎉 No API errors encountered")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())