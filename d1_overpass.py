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
import sys
from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional

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


def build_query() -> str:
    # Use administrative area for Colombia and case-insensitive regex flags
    # Overpass supports case-insensitive via ",i" flag and POSIX regex
    return (
        "[out:json][timeout:90];\n"
        "area[\"ISO3166-1\"=\"CO\"][admin_level=2]->.searchArea;\n"
        "(\n"
        "  node[\"brand\"~\"(^|\\b)D1(\\b|$)\",i](area.searchArea);\n"
        "  node[\"name\"~\"(^|\\b)(Tiendas?\\s*)?D1(\\b|$)\",i](area.searchArea);\n"
        "  way[\"brand\"~\"(^|\\b)D1(\\b|$)\",i](area.searchArea);\n"
        "  way[\"name\"~\"(^|\\b)(Tiendas?\\s*)?D1(\\b|$)\",i](area.searchArea);\n"
        "  relation[\"brand\"~\"(^|\\b)D1(\\b|$)\",i](area.searchArea);\n"
        "  relation[\"name\"~\"(^|\\b)(Tiendas?\\s*)?D1(\\b|$)\",i](area.searchArea);\n"
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
    ]
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for s in stores:
            writer.writerow(asdict(s))


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Export Tiendas D1 from OpenStreetMap Overpass")
    parser.add_argument("--output", default="d1_osm", help="Output base filename (no extension)")
    args = parser.parse_args(argv)

    try:
        data = fetch_overpass()
    except Exception as exc:
        print(f"Error querying Overpass: {exc}", file=sys.stderr)
        return 1

    elements = data.get("elements") or []
    stores = normalize(elements)
    json_path = f"{args.output}.json"
    csv_path = f"{args.output}.csv"
    write_json(stores, json_path)
    write_csv(stores, csv_path)
    print(f"Saved {len(stores)} OSM D1 stores to {json_path} and {csv_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

