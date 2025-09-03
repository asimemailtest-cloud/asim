#!/usr/bin/env python3
"""
Query Google Places API to export Tiendas D1 locations in Colombia to CSV
(name, latitude, longitude).

Requirements:
- Google Maps Places API key with Places API enabled.

Usage:
  export GOOGLE_MAPS_API_KEY=your_key
  python3 google_places_d1.py --output /workspace/output/d1_google_places.csv

Tips to increase coverage:
- Provide multiple keywords (repeat --keyword): "d1", "tiendas d1", "almacenes d1", "d-1"
- Search multiple place types (repeat --type): supermarket, grocery_or_supermarket, store, convenience_store
- Reduce grid steps: --lat-step 0.3 --lng-step 0.3 (more overlap)
- Use rankby distance (denser in cities): --use-rankby-distance (drops radius)
"""

import argparse
import csv
import os
import sys
import time
from typing import Dict, Iterable, List, Optional, Set, Tuple
from itertools import product

import requests


PLACES_NEARBY_URL = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"


def get_api_key(cmd_key: str) -> str:
    key = cmd_key or os.environ.get("GOOGLE_MAPS_API_KEY", "").strip()
    if not key:
        print("Missing API key. Set --api-key or env GOOGLE_MAPS_API_KEY", file=sys.stderr)
        sys.exit(2)
    return key


def clamp(value: float, min_v: float, max_v: float) -> float:
    return max(min_v, min(max_v, value))


def generate_grid(
    min_lat: float,
    max_lat: float,
    min_lng: float,
    max_lng: float,
    lat_step: float,
    lng_step: float,
) -> Iterable[Tuple[float, float]]:
    lat = min_lat
    while lat <= max_lat:
        lng = min_lng
        while lng <= max_lng:
            yield (round(lat, 6), round(lng, 6))
            lng += lng_step
        lat += lat_step


def nearby_search(
    api_key: str,
    location: Tuple[float, float],
    radius_m: int,
    keyword: str,
    place_type: str,
    language: str = "es",
    rate_limit_s: float = 0.3,
    rankby: Optional[str] = None,
) -> Iterable[Dict]:
    params = {
        "key": api_key,
        "location": f"{location[0]},{location[1]}",
        "keyword": keyword,
        "type": place_type,
        "language": language,
    }
    if rankby == "distance":
        params["rankby"] = "distance"
    else:
        params["radius"] = radius_m
    session = requests.Session()

    next_page_token: Optional[str] = None
    while True:
        if next_page_token:
            # Per Places API, next_page_token may need a short wait to become valid
            time.sleep(2.0)
            params["pagetoken"] = next_page_token
        else:
            params.pop("pagetoken", None)

        time.sleep(rate_limit_s)
        resp = session.get(PLACES_NEARBY_URL, params=params, timeout=30)
        data = resp.json()
        status = data.get("status", "")
        if status in ("OK", "ZERO_RESULTS"):
            for r in data.get("results", []):
                yield r
            next_page_token = data.get("next_page_token")
            if not next_page_token:
                break
        elif status == "OVER_QUERY_LIMIT":
            # backoff
            time.sleep(3.0)
            continue
        else:
            # Other errors; stop this tile
            break


def extract_record(result: Dict) -> Optional[Tuple[str, float, float, str]]:
    try:
        name = (result.get("name") or "").strip()
        loc = (result.get("geometry") or {}).get("location") or {}
        lat = float(loc.get("lat"))
        lng = float(loc.get("lng"))
        place_id = (result.get("place_id") or "").strip()
        if not name or place_id == "":
            return None
        return name, lat, lng, place_id
    except Exception:
        return None


def write_csv(path: str, rows: List[Tuple[str, float, float]]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["name", "latitude", "longitude"])
        for name, lat, lng in rows:
            w.writerow([name, f"{lat:.6f}", f"{lng:.6f}"])


def main():
    ap = argparse.ArgumentParser(description="Export Tiendas D1 (Google Places) to CSV with name, lat, lon")
    ap.add_argument("--api-key", default="", help="Google Maps API key (or set GOOGLE_MAPS_API_KEY env)")
    ap.add_argument("--output", required=True, help="Output CSV path")
    ap.add_argument("--radius-m", type=int, default=50000, help="Nearby search radius in meters (max 50000)")
    ap.add_argument("--lat-step", type=float, default=1.0, help="Latitude grid step degrees")
    ap.add_argument("--lng-step", type=float, default=1.0, help="Longitude grid step degrees")
    ap.add_argument("--keyword", action="append", default=[], help="Search keyword(s); repeat to add more")
    ap.add_argument("--type", dest="place_type", action="append", default=[], help="Place type(s); repeat to add more")
    ap.add_argument("--use-rankby-distance", action="store_true", help="Use rankby=distance (no radius) for denser urban coverage")
    ap.add_argument("--min-lat", type=float, default=-4.5, help="Min latitude for Colombia bbox")
    ap.add_argument("--max-lat", type=float, default=13.0, help="Max latitude for Colombia bbox")
    ap.add_argument("--min-lng", type=float, default=-79.5, help="Min longitude for Colombia bbox")
    ap.add_argument("--max-lng", type=float, default=-66.5, help="Max longitude for Colombia bbox")
    ap.add_argument("--max-results", type=int, default=10000, help="Stop after collecting this many unique places")
    ap.add_argument("--country-name", default="colombia", help="Keep only results whose plus_code/vicinity mentions this country (case-insensitive)")
    ap.add_argument("--strict-country", action="store_true", help="Drop results without country match even if plus_code/vicinity missing")
    ap.add_argument("--name-token", action="append", default=[], help="Name must contain at least one of these case-insensitive tokens; repeatable")
    args = ap.parse_args()

    api_key = get_api_key(args.api_key)
    radius_m = int(clamp(args.radius_m, 1, 50000))

    seen_place_ids: Set[str] = set()
    collected: List[Tuple[str, float, float]] = []

    # Defaults for broader coverage
    keywords: List[str] = args.keyword or ["tiendas d1", "d1", "d-1", "almacenes d1"]
    place_types: List[str] = args.place_type or [
        "grocery_or_supermarket", "supermarket", "store", "convenience_store"
    ]
    name_tokens: List[str] = [t.lower() for t in (args.name_token or ["d1"]) if t]

    for lat, lng in generate_grid(
        args.min_lat, args.max_lat, args.min_lng, args.max_lng, args.lat_step, args.lng_step
    ):
        for kw, typ in product(keywords, place_types):
            for res in nearby_search(
                api_key=api_key,
                location=(lat, lng),
                radius_m=radius_m,
                keyword=kw,
                place_type=typ,
                rankby=("distance" if args.use-rankby-distance else None),
            ):
                rec = extract_record(res)
                if not rec:
                    continue
                name, plat, plng, pid = rec
                # Name token filter
                name_l = name.lower()
                if name_tokens and not any(tok in name_l for tok in name_tokens):
                    continue
                # Country filter via plus_code or vicinity when available
                cn = (args.country_name or "").strip().lower()
                if cn:
                    plus_code = ((res.get("plus_code") or {}).get("compound_code") or "").strip().lower()
                    vicinity = (res.get("vicinity") or "").strip().lower()
                    has_country = (cn in plus_code) or (cn in vicinity)
                    if args.strict_country:
                        if not has_country:
                            continue
                    else:
                        # If we have any location text but it doesn't mention country, skip; otherwise keep
                        if (plus_code or vicinity) and not has_country:
                            continue
                if pid in seen_place_ids:
                    continue
                seen_place_ids.add(pid)
                collected.append((name, plat, plng))
                if len(collected) >= args.max_results:
                    write_csv(args.output, collected)
                    print(f"Wrote {len(collected)} rows to {args.output}")
                    return

    write_csv(args.output, collected)
    print(f"Wrote {len(collected)} rows to {args.output}")


if __name__ == "__main__":
    main()

