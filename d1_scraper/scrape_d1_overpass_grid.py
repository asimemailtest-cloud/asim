#!/usr/bin/env python3
import argparse
import csv
import json
import math
import pathlib
import time
import urllib.parse
import urllib.request
from typing import Any, Dict, Generator, List, Set, Tuple

OVERPASS_URLS = (
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
)

USER_AGENT = "d1-overpass-grid/1.0 (contact: local)"

# Conservative default bounding box for Colombia mainland
DEFAULT_SOUTH = -5.0
DEFAULT_WEST = -79.0
DEFAULT_NORTH = 13.5
DEFAULT_EAST = -66.0

# Overpass query template using bbox
QUERY_TEMPLATE = """
[out:json][timeout:300];
(
  nwr["shop"~"supermarket|convenience|variety_store", i]["brand"~"(^|\\b)D\\s*-?\\s*1(\\b|$)", i]({s},{w},{n},{e});
  nwr["shop"~"supermarket|convenience|variety_store", i]["name"~"(^|\\b)(tiendas?\\s*)?d\\s*-?\\s*1(\\b|$)", i]({s},{w},{n},{e});
  nwr["shop"~"supermarket|convenience|variety_store", i]["operator"~"koba|(^|\\b)d1(\\b|$)", i]({s},{w},{n},{e});
);
out center tags;
"""


def generate_cells(south: float, west: float, north: float, east: float, step: float) -> Generator[Tuple[float, float, float, float], None, None]:
    lat = south
    while lat < north:
        lat2 = min(lat + step, north)
        lon = west
        while lon < east:
            lon2 = min(lon + step, east)
            yield (lat, lon, lat2, lon2)
            lon = lon2
        lat = lat2


def fetch_overpass(query: str, retries: int = 3, backoff: float = 1.7) -> Dict[str, Any]:
    last_error: Exception | None = None
    data = urllib.parse.urlencode({"data": query}).encode("utf-8")
    for base_url in OVERPASS_URLS:
        for attempt in range(1, retries + 1):
            req = urllib.request.Request(
                base_url,
                data=data,
                headers={"User-Agent": USER_AGENT, "Content-Type": "application/x-www-form-urlencoded"},
                method="POST",
            )
            try:
                with urllib.request.urlopen(req, timeout=180) as resp:
                    body = resp.read().decode("utf-8", errors="replace")
                    if "Too many requests" in body:
                        raise RuntimeError("overpass rate limited")
                    return json.loads(body)
            except Exception as e:
                last_error = e
                time.sleep(backoff ** attempt)
        # try next base_url
    if last_error is not None:
        raise last_error
    raise RuntimeError("Unknown Overpass error")


def elements_to_rows(elements: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for el in elements:
        tags = el.get("tags", {}) or {}
        if el.get("type") == "node":
            lat, lon = el.get("lat"), el.get("lon")
        else:
            center = el.get("center") or {}
            lat, lon = center.get("lat"), center.get("lon")
        rows.append({
            "source": "osm",
            "osm_id": el.get("id"),
            "osm_type": el.get("type"),
            "name": tags.get("name"),
            "brand": tags.get("brand"),
            "operator": tags.get("operator"),
            "addr_full": tags.get("addr:full"),
            "addr_street": tags.get("addr:street"),
            "addr_housenumber": tags.get("addr:housenumber"),
            "addr_city": tags.get("addr:city"),
            "addr_state": tags.get("addr:state"),
            "addr_postcode": tags.get("addr:postcode"),
            "opening_hours": tags.get("opening_hours"),
            "phone": tags.get("phone") or tags.get("contact:phone"),
            "website": tags.get("website"),
            "lat": lat,
            "lon": lon,
        })
    return rows


def write_csv(rows: List[Dict[str, Any]], path: pathlib.Path) -> None:
    if not rows:
        return
    fieldnames = list(rows[0].keys())
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)


def write_geojson(rows: List[Dict[str, Any]], path: pathlib.Path) -> None:
    fc = {"type": "FeatureCollection", "features": []}
    for r in rows:
        lat = r.get("lat")
        lon = r.get("lon")
        if lat is None or lon is None:
            continue
        props = dict(r)
        props.pop("lat", None)
        props.pop("lon", None)
        fc["features"].append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [lon, lat]},
            "properties": props,
        })
    with open(path, "w", encoding="utf-8") as f:
        json.dump(fc, f, ensure_ascii=False, indent=2)


def main() -> None:
    ap = argparse.ArgumentParser(description="Grid scan Overpass for Tiendas D1 in Colombia")
    ap.add_argument("--outdir", default="./output", help="Directory to write outputs")
    ap.add_argument("--south", type=float, default=DEFAULT_SOUTH)
    ap.add_argument("--west", type=float, default=DEFAULT_WEST)
    ap.add_argument("--north", type=float, default=DEFAULT_NORTH)
    ap.add_argument("--east", type=float, default=DEFAULT_EAST)
    ap.add_argument("--step", type=float, default=1.5, help="Grid step degrees")
    ap.add_argument("--sleep", type=float, default=1.0, help="Sleep seconds between tiles")
    ap.add_argument("--max_cells", type=int, default=0, help="Limit number of cells for testing")
    args = ap.parse_args()

    outdir = pathlib.Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    seen: Set[Tuple[str, int]] = set()  # (type, id)
    all_rows: List[Dict[str, Any]] = []

    cells = list(generate_cells(args.south, args.west, args.north, args.east, args.step))
    if args.max_cells and args.max_cells > 0:
        cells = cells[: args.max_cells]

    total = len(cells)
    for idx, (s, w, n, e) in enumerate(cells, start=1):
        query = QUERY_TEMPLATE.format(s=s, w=w, n=n, e=e)
        try:
            data = fetch_overpass(query)
        except Exception as e:
            # Skip this cell on error
            time.sleep(args.sleep)
            continue
        elements = data.get("elements", [])
        for el in elements:
            key = (el.get("type", ""), el.get("id", 0))
            if key in seen:
                continue
            seen.add(key)
            all_rows.extend(elements_to_rows([el]))
        if idx % 10 == 0 or idx == total:
            print(f"Processed {idx}/{total} cells; cumulative rows: {len(all_rows)}")
        time.sleep(args.sleep)

    csv_path = outdir / "d1_stores_colombia_grid.csv"
    geojson_path = outdir / "d1_stores_colombia_grid.geojson"
    write_csv(all_rows, csv_path)
    write_geojson(all_rows, geojson_path)

    print(f"Wrote {len(all_rows)} rows (grid) to {outdir}")


if __name__ == "__main__":
    main()