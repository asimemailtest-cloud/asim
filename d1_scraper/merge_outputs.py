#!/usr/bin/env python3
import argparse
import csv
import json
import math
import pathlib
from typing import Any, Dict, List, Optional, Tuple


def load_csv(path: pathlib.Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return list(reader)


def try_float(value: Any) -> Optional[float]:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def normalize(s: Optional[str]) -> str:
    if not s:
        return ""
    return " ".join(s.strip().lower().split())


def coord_bucket(lat: Optional[float], lon: Optional[float], precision: int = 5) -> str:
    if lat is None or lon is None:
        return ""
    return f"{round(lat, precision)}:{round(lon, precision)}"


def build_key(record: Dict[str, Any]) -> Tuple[str, str, str]:
    # Create a composite key using normalized address, city, and rounded coords
    addr = normalize(record.get("addr_full") or record.get("street_address"))
    city = normalize(record.get("addr_city") or record.get("city"))
    lat = try_float(record.get("lat"))
    lon = try_float(record.get("lon"))
    coord = coord_bucket(lat, lon, precision=4)
    return (addr, city, coord)


def merge_records(a: Dict[str, Any], b: Dict[str, Any]) -> Dict[str, Any]:
    merged = dict(a)
    for k, v in b.items():
        if k not in merged or merged[k] in (None, ""):
            merged[k] = v
    # preserve both sources
    sources = []
    if a.get("source"):
        sources.append(a.get("source"))
    if b.get("source") and b.get("source") not in sources:
        sources.append(b.get("source"))
    merged["source"] = ",".join(sources) if sources else None
    return merged


def write_csv(rows: List[Dict[str, Any]], path: pathlib.Path) -> None:
    if not rows:
        return
    fieldnames: List[str] = []
    for r in rows:
        for k in r.keys():
            if k not in fieldnames:
                fieldnames.append(k)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)


def write_geojson(rows: List[Dict[str, Any]], path: pathlib.Path) -> None:
    features: List[Dict[str, Any]] = []
    for r in rows:
        lat = try_float(r.get("lat"))
        lon = try_float(r.get("lon"))
        if lat is None or lon is None:
            continue
        props = dict(r)
        props.pop("lat", None)
        props.pop("lon", None)
        features.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [lon, lat]},
            "properties": props,
        })
    fc = {"type": "FeatureCollection", "features": features}
    with open(path, "w", encoding="utf-8") as f:
        json.dump(fc, f, ensure_ascii=False, indent=2)


def main() -> None:
    ap = argparse.ArgumentParser(description="Merge OSM and S24 outputs for D1 stores")
    ap.add_argument("--outdir", default="./output", help="Directory where inputs live and outputs will be written")
    args = ap.parse_args()

    outdir = pathlib.Path(args.outdir)
    osm_csv = outdir / "d1_stores_colombia.csv"
    s24_csv = outdir / "s24_d1_stores_colombia.csv"

    osm_rows = load_csv(osm_csv)
    s24_rows = load_csv(s24_csv)

    merged: Dict[Tuple[str, str, str], Dict[str, Any]] = {}

    for r in osm_rows:
        r = dict(r)
        r.setdefault("source", "osm")
        key = build_key(r)
        if key in merged:
            merged[key] = merge_records(merged[key], r)
        else:
            merged[key] = r

    for r in s24_rows:
        r = dict(r)
        r.setdefault("source", "sucursales24")
        key = build_key(r)
        if key in merged:
            merged[key] = merge_records(merged[key], r)
        else:
            merged[key] = r

    final_rows = list(merged.values())
    combined_csv = outdir / "combined_d1_stores.csv"
    combined_geojson = outdir / "combined_d1_stores.geojson"

    write_csv(final_rows, combined_csv)
    write_geojson(final_rows, combined_geojson)

    print(f"Merged {len(osm_rows)} OSM + {len(s24_rows)} S24 -> {len(final_rows)} unique records")


if __name__ == "__main__":
    main()