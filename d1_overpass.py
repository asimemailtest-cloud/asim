#!/usr/bin/env python3
"""
Fetch Tiendas D1 stores in Colombia from OpenStreetMap Overpass API.

This avoids scraping d1.com.co and uses public OSM data instead.
It queries nodes/ways/relations with brand/name matching common D1 tags.

Usage:
  python d1_overpass.py --output d1_osm
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional, Tuple

import requests


OVERPASS_URL = "https://overpass-api.de/api/interpreter"


@dataclass
class Store:
    osm_type: str
    osm_id: int
    name: Optional[str]
    brand: Optional[str]
    address: Optional[str]
    city: Optional[str]
    state: Optional[str]
    country: Optional[str]
    latitude: Optional[float]
    longitude: Optional[float]
    # Preserve the original source for traceability
    source: str = "osm"


def build_query() -> str:
    # Use administrative area for Colombia and case-insensitive regex flags
    # Overpass supports case-insensitive via ",i" flag and POSIX regex
    return (
        "[out:json][timeout:90];\n"
        "area[\"ISO3166-1\"=\"CO\"][admin_level=2]->.searchArea;\n"
        "(\n"
        "  node[\"brand\"~\"(^|\\b)D1(\\b|$)\",i](area.searchArea);\n"
        "  node[\"name\"~\"(^|\\b)(Tiendas?\\s*)?D1(\\b|$)\",i](area.searchArea);\n"
        "  node[\"brand:short\"~\"(^|\\b)D1(\\b|$)\",i](area.searchArea);\n"
        "  node[\"operator\"~\"Koba\\s+Colombia\",i](area.searchArea);\n"
        "  way[\"brand\"~\"(^|\\b)D1(\\b|$)\",i](area.searchArea);\n"
        "  way[\"name\"~\"(^|\\b)(Tiendas?\\s*)?D1(\\b|$)\",i](area.searchArea);\n"
        "  way[\"brand:short\"~\"(^|\\b)D1(\\b|$)\",i](area.searchArea);\n"
        "  way[\"operator\"~\"Koba\\s+Colombia\",i](area.searchArea);\n"
        "  relation[\"brand\"~\"(^|\\b)D1(\\b|$)\",i](area.searchArea);\n"
        "  relation[\"name\"~\"(^|\\b)(Tiendas?\\s*)?D1(\\b|$)\",i](area.searchArea);\n"
        "  relation[\"brand:short\"~\"(^|\\b)D1(\\b|$)\",i](area.searchArea);\n"
        "  relation[\"operator\"~\"Koba\\s+Colombia\",i](area.searchArea);\n"
        ");\n"
        "out center tags;\n"
    )


def fetch_overpass() -> Dict[str, Any]:
    q = build_query()
    resp = requests.post(OVERPASS_URL, data={"data": q}, timeout=120)
    resp.raise_for_status()
    return resp.json()


def normalize(elements: List[Dict[str, Any]]) -> List[Store]:
    stores: List[Store] = []
    for el in elements:
        el_type = el.get("type")
        el_id = int(el.get("id"))
        tags = el.get("tags") or {}
        name = tags.get("name")
        brand = tags.get("brand")
        # Coordinates: nodes -> lat/lon; ways/relations -> center.lat/center.lon
        lat = el.get("lat")
        lon = el.get("lon")
        if lat is None or lon is None:
            center = el.get("center") or {}
            lat = center.get("lat")
            lon = center.get("lon")

        # Address fields
        housenumber = tags.get("addr:housenumber")
        street = tags.get("addr:street")
        city = tags.get("addr:city") or tags.get("addr:town") or tags.get("addr:village")
        state = tags.get("addr:state") or tags.get("addr:province")
        country = tags.get("addr:country") or "CO"
        address = None
        if street and housenumber:
            address = f"{street} {housenumber}"
        elif street:
            address = street

        stores.append(
            Store(
                osm_type=str(el_type),
                osm_id=el_id,
                name=name,
                brand=brand,
                address=address,
                city=city,
                state=state,
                country=country,
                latitude=float(lat) if lat is not None else None,
                longitude=float(lon) if lon is not None else None,
                source="osm",
            )
        )
    # Deduplicate by osm_type/osm_id
    seen = set()
    unique: List[Store] = []
    for s in stores:
        key = (s.osm_type, s.osm_id)
        if key in seen:
            continue
        seen.add(key)
        unique.append(s)
    return unique


def write_json(stores: List[Store], path: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump([asdict(s) for s in stores], f, ensure_ascii=False, indent=2)


def geocode_google(address: str, api_key: str) -> Optional[Tuple[float, float]]:
    url = "https://maps.googleapis.com/maps/api/geocode/json"
    params = {"address": address, "key": api_key, "region": "co"}
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    if data.get("status") != "OK":
        return None
    results = data.get("results") or []
    if not results:
        return None
    location = results[0]["geometry"]["location"]
    return float(location["lat"]), float(location["lng"])


def enrich_with_google(stores: List[Store], api_key: str, force_google: bool, rate_limit_s: float, cache_path: Optional[str]) -> List[Store]:
    cache: Dict[str, Tuple[float, float]] = {}
    if cache_path and os.path.exists(cache_path):
        try:
            with open(cache_path, "r", encoding="utf-8") as f:
                raw = json.load(f)
                cache = {k: (float(v[0]), float(v[1])) for k, v in raw.items()}
        except Exception:
            cache = {}

    def save_cache():
        if cache_path:
            try:
                with open(cache_path, "w", encoding="utf-8") as f:
                    json.dump(cache, f, ensure_ascii=False, indent=2)
            except Exception:
                pass

    def build_address(s: Store) -> Optional[str]:
        parts = []
        if s.address:
            parts.append(s.address)
        if s.city:
            parts.append(s.city)
        if s.state:
            parts.append(s.state)
        parts.append("Colombia")
        if not parts:
            return None
        return ", ".join([p for p in parts if p])

    updated: List[Store] = []
    for s in stores:
        # Skip if we already have coordinates and not forcing
        if not force_google and s.latitude is not None and s.longitude is not None:
            updated.append(s)
            continue
        addr = build_address(s)
        if not addr:
            updated.append(s)
            continue
        if addr in cache:
            lat, lng = cache[addr]
            updated.append(Store(**{**asdict(s), "latitude": lat, "longitude": lng, "source": "google"}))
            continue
        try:
            result = geocode_google(addr, api_key)
            if result:
                lat, lng = result
                cache[addr] = (lat, lng)
                updated.append(Store(**{**asdict(s), "latitude": lat, "longitude": lng, "source": "google"}))
            else:
                updated.append(s)
        except Exception:
            updated.append(s)
        time.sleep(rate_limit_s)

    save_cache()
    return updated


def write_csv(stores: List[Store], path: str) -> None:
    fieldnames = [
        "osm_type",
        "osm_id",
        "name",
        "brand",
        "address",
        "city",
        "state",
        "country",
        "latitude",
        "longitude",
        "source",
    ]
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for s in stores:
            writer.writerow(asdict(s))


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Export Tiendas D1 from OpenStreetMap Overpass, optionally enrich with Google Geocoding")
    parser.add_argument("--output", default="d1_osm", help="Output base filename (no extension)")
    parser.add_argument("--google-api-key", default=os.environ.get("GOOGLE_API_KEY"), help="Google Maps Geocoding API key (or set GOOGLE_API_KEY)")
    parser.add_argument("--force-google", action="store_true", help="Override OSM coords with Google Geocoding")
    parser.add_argument("--google-rate", type=float, default=0.1, help="Seconds to sleep between geocode calls (default 0.1)")
    parser.add_argument("--cache", default="/workspace/d1_geocode_cache.json", help="Path to geocode cache JSON")
    args = parser.parse_args(argv)

    try:
        data = fetch_overpass()
    except Exception as exc:
        print(f"Error querying Overpass: {exc}", file=sys.stderr)
        return 1

    elements = data.get("elements") or []
    stores = normalize(elements)

    if args.google_api_key:
        stores = enrich_with_google(
            stores,
            api_key=args.google_api_key,
            force_google=args.force_google,
            rate_limit_s=max(0.0, args.google_rate),
            cache_path=args.cache,
        )
    json_path = f"{args.output}.json"
    csv_path = f"{args.output}.csv"
    write_json(stores, json_path)
    write_csv(stores, csv_path)
    print(f"Saved {len(stores)} OSM D1 stores to {json_path} and {csv_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

