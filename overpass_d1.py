#!/usr/bin/env python3
"""
Fetch Tiendas D1 store locations in Colombia using the OpenStreetMap Overpass API.

Outputs:
- CSV (default columns) or minimal CSV (name, latitude, longitude only)
- Optional GeoJSON with point features

Usage:
  python3 overpass_d1.py --output /workspace/output/d1_osm_stores.csv --geojson /workspace/output/d1_osm_stores.geojson
  python3 overpass_d1.py --output /workspace/output/d1_osm_min.csv --minimal

Notes:
- Uses multiple public Overpass endpoints with fallback and slot waiting.
- Restricts search to the country area for Colombia (ISO3166-1=CO).
- Includes nodes, ways, and relations; ways/relations use their computed center.
"""

import argparse
import csv
import json
import re
import time
import sys
from typing import Dict, Iterable, List, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


OVERPASS_ENDPOINTS = [
    "https://overpass.kumi.systems/api/interpreter",
    "https://overpass-api.de/api/interpreter",
    "https://z.overpass-api.de/api/interpreter",
    "https://overpass.openstreetmap.fr/api/interpreter",
]


def build_overpass_query() -> str:
    # Match D1 stores by brand/name/operator hints commonly used in OSM.
    # We focus on supermarket/shop-like POIs and include different tag combos.
    return r"""
    [out:json][timeout:300];
    area["ISO3166-1"="CO"][admin_level=2]->.co;
    (
      node["shop"="supermarket"]["brand"~"d\s*1", i](area.co);
      node["shop"="supermarket"]["name"~"\bd\s*1\b", i](area.co);
      way ["shop"="supermarket"]["brand"~"d\s*1", i](area.co);
      way ["shop"="supermarket"]["name"~"\bd\s*1\b", i](area.co);
      relation["shop"="supermarket"]["brand"~"d\s*1", i](area.co);
      relation["shop"="supermarket"]["name"~"\bd\s*1\b", i](area.co);

      node["operator"~"koba", i](area.co);
      way ["operator"~"koba", i](area.co);
      relation["operator"~"koba", i](area.co);

      node["shop"="convenience"]["name"~"\bd\s*1\b", i](area.co);
      way ["shop"="convenience"]["name"~"\bd\s*1\b", i](area.co);
      relation["shop"="convenience"]["name"~"\bd\s*1\b", i](area.co);
    );
    out center tags;
    """


def build_overpass_query_bbox(south: float, west: float, north: float, east: float) -> str:
    return (
        "[out:json][timeout:180];\n"
        "(\n"
        f"  node[\"shop\"=\"supermarket\"][\"brand\"~\"d\\s*1\", i]({south},{west},{north},{east});\n"
        f"  node[\"shop\"=\"supermarket\"][\"name\"~\"\\bd\\s*1\\b\", i]({south},{west},{north},{east});\n"
        f"  way [\"shop\"=\"supermarket\"][\"brand\"~\"d\\s*1\", i]({south},{west},{north},{east});\n"
        f"  way [\"shop\"=\"supermarket\"][\"name\"~\"\\bd\\s*1\\b\", i]({south},{west},{north},{east});\n"
        f"  relation[\"shop\"=\"supermarket\"][\"brand\"~\"d\\s*1\", i]({south},{west},{north},{east});\n"
        f"  relation[\"shop\"=\"supermarket\"][\"name\"~\"\\bd\\s*1\\b\", i]({south},{west},{north},{east});\n"
        f"  node[\"operator\"~\"koba\", i]({south},{west},{north},{east});\n"
        f"  way [\"operator\"~\"koba\", i]({south},{west},{north},{east});\n"
        f"  relation[\"operator\"~\"koba\", i]({south},{west},{north},{east});\n"
        f"  node[\"shop\"=\"convenience\"][\"name\"~\"\\bd\\s*1\\b\", i]({south},{west},{north},{east});\n"
        f"  way [\"shop\"=\"convenience\"][\"name\"~\"\\bd\\s*1\\b\", i]({south},{west},{north},{east});\n"
        f"  relation[\"shop\"=\"convenience\"][\"name\"~\"\\bd\\s*1\\b\", i]({south},{west},{north},{east});\n"
        ");\n"
        "out center tags;\n"
    )


def _session() -> requests.Session:
    s = requests.Session()
    retries = Retry(
        total=3,
        backoff_factor=1.0,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"],
    )
    s.mount("https://", HTTPAdapter(max_retries=retries, pool_maxsize=4))
    s.headers.update({
        "User-Agent": "d1-overpass-scraper/1.0 (+https://example.com)",
        "Accept": "application/json",
    })
    return s


def _wait_for_slot(endpoint: str, ses: requests.Session, max_wait_s: int = 180) -> None:
    status_url = endpoint.replace("/interpreter", "/status")
    start = time.time()
    while True:
        try:
            r = ses.get(status_url, timeout=10)
            t = (r.text or "").lower()
            if "slot available" in t or "available now" in t:
                return
            m = re.search(r"after\s+(\d+)\s+seconds", t)
            sleep_s = min(int(m.group(1)) + 1 if m else 5, 10)
            time.sleep(sleep_s)
        except Exception:
            time.sleep(5)
        if time.time() - start > max_wait_s:
            return


def fetch_overpass(query: str, endpoints: Optional[List[str]] = None, timeout_s: int = 120) -> Dict:
    last_err: Optional[BaseException] = None
    ses = _session()
    ep_list = list(endpoints) if endpoints else list(OVERPASS_ENDPOINTS)
    for url in ep_list:
        try:
            _wait_for_slot(url, ses, max_wait_s=180)
            resp = ses.post(url, data=query.encode("utf-8"), timeout=timeout_s)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            last_err = e
            try:
                resp = ses.get(url, params={"data": query}, timeout=timeout_s)
                resp.raise_for_status()
                return resp.json()
            except Exception as eg:
                last_err = eg
                continue
    if last_err:
        raise RuntimeError(f"All Overpass endpoints failed: {last_err}")
    raise RuntimeError("All Overpass endpoints failed with unknown error")


def make_osm_url(osm_type: str, osm_id: int) -> str:
    # osm_type in Overpass is one of: node/way/relation
    return f"https://www.openstreetmap.org/{osm_type}/{osm_id}"


def build_address(tags: Dict[str, str]) -> Tuple[str, str, str]:
    # Returns (address, city, department)
    street = tags.get("addr:street", "").strip()
    housenumber = tags.get("addr:housenumber", "").strip()
    neighbourhood = tags.get("addr:neighbourhood", "").strip()
    city = tags.get("addr:city", "").strip()
    state = tags.get("addr:state", "").strip()  # Colombian departments often here
    postcode = tags.get("addr:postcode", "").strip()

    parts: List[str] = []
    if street or housenumber:
        parts.append(" ".join([p for p in [street, housenumber] if p]).strip())
    if neighbourhood:
        parts.append(neighbourhood)
    if city:
        parts.append(city)
    if state:
        parts.append(state)
    if postcode:
        parts.append(postcode)
    address = ", ".join([p for p in parts if p])
    return address, city, state


def extract_elements(osm: Dict) -> List[Dict]:
    elements = osm.get("elements") or []
    rows: List[Dict] = []
    seen_ids: set = set()
    for el in elements:
        if not isinstance(el, dict):
            continue
        osm_type = el.get("type")
        osm_id = el.get("id")
        if osm_type is None or osm_id is None:
            continue
        key = (osm_type, osm_id)
        if key in seen_ids:
            continue
        seen_ids.add(key)

        tags: Dict[str, str] = el.get("tags") or {}
        name = (tags.get("name") or "Tiendas D1").strip()
        phone = (tags.get("phone") or tags.get("contact:phone") or "").strip()
        hours = (tags.get("opening_hours") or "").strip()

        lat: Optional[float] = None
        lon: Optional[float] = None
        if osm_type == "node":
            lat = el.get("lat")
            lon = el.get("lon")
        else:
            center = el.get("center") or {}
            lat = center.get("lat")
            lon = center.get("lon")

        address, city, department = build_address(tags)

        rows.append({
            "name": name,
            "address": address,
            "city": city,
            "department": department,
            "phone": phone,
            "hours": hours,
            "latitude": lat,
            "longitude": lon,
            "source_url": make_osm_url(osm_type, osm_id),
        })
    return rows


def write_csv(path: str, rows: List[Dict]) -> None:
    fieldnames = [
        "name", "address", "city", "department", "phone", "hours",
        "latitude", "longitude", "source_url",
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)


def write_csv_minimal(path: str, rows: List[Dict]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["name", "latitude", "longitude"])
        for r in rows:
            name = r.get("name")
            lat = r.get("latitude")
            lon = r.get("longitude")
            if name and lat is not None and lon is not None:
                w.writerow([name, f"{lat:.6f}", f"{lon:.6f}"])


def write_geojson(path: str, rows: List[Dict]) -> None:
    features: List[Dict] = []
    for r in rows:
        lat = r.get("latitude")
        lon = r.get("longitude")
        geom = None
        if isinstance(lat, (float, int)) and isinstance(lon, (float, int)):
            geom = {"type": "Point", "coordinates": [float(lon), float(lat)]}
        props = dict(r)
        props.pop("latitude", None)
        props.pop("longitude", None)
        features.append({
            "type": "Feature",
            "properties": props,
            "geometry": geom,
        })
    fc = {"type": "FeatureCollection", "features": features}
    with open(path, "w", encoding="utf-8") as f:
        json.dump(fc, f, ensure_ascii=False)


def main():
    ap = argparse.ArgumentParser(description="Fetch Tiendas D1 locations in Colombia via Overpass (OSM)")
    ap.add_argument("--output", required=True, help="Output CSV path")
    ap.add_argument("--geojson", default="", help="Optional output GeoJSON path")
    ap.add_argument("--endpoint", action="append", default=[], help="Custom Overpass endpoint(s); can be used multiple times")
    ap.add_argument("--timeout", type=int, default=120, help="HTTP timeout seconds per request")
    ap.add_argument("--minimal", action="store_true", help="Write only name, latitude, longitude to CSV")
    ap.add_argument("--tile", action="store_true", help="Use tiled bbox queries instead of country area")
    ap.add_argument("--lat-step", type=float, default=2.0, help="Tile latitude step (degrees)")
    ap.add_argument("--lng-step", type=float, default=2.0, help="Tile longitude step (degrees)")
    ap.add_argument("--min-lat", type=float, default=-4.5, help="Min latitude for Colombia bbox")
    ap.add_argument("--max-lat", type=float, default=13.0, help="Max latitude for Colombia bbox")
    ap.add_argument("--min-lng", type=float, default=-79.5, help="Min longitude for Colombia bbox")
    ap.add_argument("--max-lng", type=float, default=-66.5, help="Max longitude for Colombia bbox")
    args = ap.parse_args()

    endpoints = args.endpoint if args.endpoint else None
    rows: List[Dict] = []
    if args.tile:
        lat = args.min_lat
        while lat < args.max_lat:
            lng = args.min_lng
            north = min(lat + args.lat_step, args.max_lat)
            while lng < args.max_lng:
                east = min(lng + args.lng_step, args.max_lng)
                q = build_overpass_query_bbox(lat, lng, north, east)
                try:
                    osm = fetch_overpass(q, endpoints=endpoints, timeout_s=args.timeout)
                    rows.extend(extract_elements(osm))
                except Exception:
                    pass
                lng = east
            lat = north
    else:
        query = build_overpass_query()
        osm = fetch_overpass(query, endpoints=endpoints, timeout_s=args.timeout)
        rows = extract_elements(osm)

    # Simple sanity filter: keep only entries with a coordinate and D1 in name
    filtered: List[Dict] = []
    for r in rows:
        name_l = (r.get("name") or "").lower()
        if ("d1" in name_l) and (r.get("latitude") is not None) and (r.get("longitude") is not None):
            filtered.append(r)

    if args.minimal:
        write_csv_minimal(args.output, filtered)
    else:
        write_csv(args.output, filtered)
    if args.geojson:
        write_geojson(args.geojson, filtered)
    print(f"Wrote {len(filtered)} stores to {args.output}{' and ' + args.geojson if args.geojson else ''}")


if __name__ == "__main__":
    main()

