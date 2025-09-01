#!/usr/bin/env python3
"""
Fetch Tiendas D1 store locations in Colombia using the OpenStreetMap Overpass API.

Outputs:
- CSV with columns: name, address, city, department, phone, hours, latitude, longitude, source_url
- Optional GeoJSON with point features

Usage:
  python3 overpass_d1.py --output /workspace/output/d1_osm_stores.csv --geojson /workspace/output/d1_osm_stores.geojson

Notes:
- Uses multiple public Overpass endpoints with fallback.
- Restricts search to the country area for Colombia (ISO3166-1=CO).
- Includes nodes, ways, and relations; ways/relations use their computed center.
"""

import argparse
import csv
import json
import sys
from typing import Dict, Iterable, List, Optional, Tuple

import requests


OVERPASS_ENDPOINTS = [
    "https://overpass.kumi.systems/api/interpreter",
    "https://overpass-api.de/api/interpreter",
    "https://overpass.openstreetmap.ru/api/interpreter",
]


def build_overpass_query() -> str:
    # Match D1 stores by brand/name/operator hints commonly used in OSM.
    # We focus on supermarket/shop-like POIs and include different tag combos.
    return r"""
    [out:json][timeout:120];
    area["ISO3166-1"="CO"][admin_level=2]->.co;
    (
      node["shop"="supermarket"]["brand"~"(?i)^tiendas?\s*d\s*1$"](area.co);
      node["shop"="supermarket"]["name"~"(?i)^tiendas?\s*d\s*1$"](area.co);
      node["brand"~"(?i)^tiendas?\s*d\s*1$"]["name"~"(?i)d\s*1"](area.co);
      way ["shop"="supermarket"]["brand"~"(?i)^tiendas?\s*d\s*1$"](area.co);
      way ["shop"="supermarket"]["name"~"(?i)^tiendas?\s*d\s*1$"](area.co);
      relation["shop"="supermarket"]["brand"~"(?i)^tiendas?\s*d\s*1$"](area.co);
      relation["shop"="supermarket"]["name"~"(?i)^tiendas?\s*d\s*1$"](area.co);
    );
    out center tags;
    """


def fetch_overpass(query: str, endpoints: Optional[List[str]] = None, timeout_s: int = 120) -> Dict:
    last_err: Optional[BaseException] = None
    headers = {
        "User-Agent": "d1-overpass-scraper/1.0 (+https://example.com)",
        "Accept": "application/json",
    }
    ep_list = list(endpoints) if endpoints else list(OVERPASS_ENDPOINTS)
    for url in ep_list:
        try:
            resp = requests.post(url, data=query.encode("utf-8"), headers=headers, timeout=timeout_s)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            last_err = e
            # Try GET fallback with data in "data" param (some endpoints permit this)
            try:
                resp = requests.get(url, params={"data": query}, headers=headers, timeout=timeout_s)
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
    args = ap.parse_args()

    query = build_overpass_query()
    endpoints = args.endpoint if args.endpoint else None
    osm = fetch_overpass(query, endpoints=endpoints, timeout_s=args.timeout)
    rows = extract_elements(osm)

    # Simple sanity filter: keep only entries with a coordinate and D1 in name
    filtered: List[Dict] = []
    for r in rows:
        name_l = (r.get("name") or "").lower()
        if ("d1" in name_l) and (r.get("latitude") is not None) and (r.get("longitude") is not None):
            filtered.append(r)

    write_csv(args.output, filtered)
    if args.geojson:
        write_geojson(args.geojson, filtered)
    print(f"Wrote {len(filtered)} stores to {args.output}{' and ' + args.geojson if args.geojson else ''}")


if __name__ == "__main__":
    main()

