#!/usr/bin/env python3
import json
import math
import sys
from typing import List, Tuple


EARTH_RADIUS_M = 6_371_008.8


def ring_area_m2(coords: List[List[float]]) -> float:
    """
    Approximate polygon ring area using a locally-scaled equirectangular projection.
    coords: list of [lon, lat] (can include a 3rd Z which is ignored). First and last may or may not be equal.
    Returns signed area in m^2 (positive for CCW, negative for CW).
    """
    clean = []
    for pt in coords:
        if len(pt) >= 2:
            clean.append([float(pt[0]), float(pt[1])])
    if len(clean) < 3:
        return 0.0

    # Ensure closed
    if clean[0] != clean[-1]:
        clean.append(clean[0])

    # Reference latitude for local scaling
    lat0 = sum(p[1] for p in clean[:-1]) / max(1, (len(clean) - 1))
    cos_phi = math.cos(math.radians(lat0))

    # Project to local equirectangular
    proj = []
    for lon, lat in clean:
        x = EARTH_RADIUS_M * math.radians(lon) * cos_phi
        y = EARTH_RADIUS_M * math.radians(lat)
        proj.append((x, y))

    area2 = 0.0
    for i in range(len(proj) - 1):
        x1, y1 = proj[i]
        x2, y2 = proj[i + 1]
        area2 += (x1 * y2 - x2 * y1)
    return 0.5 * area2


def polygon_area_km2(coords: List[List[List[float]]]) -> float:
    """Compute approximate polygon area (with holes) in km^2."""
    if not coords:
        return 0.0
    outer = coords[0]
    holes = coords[1:] if len(coords) > 1 else []
    area_m2 = abs(ring_area_m2(outer))
    for hole in holes:
        area_m2 -= abs(ring_area_m2(hole))
    return max(0.0, area_m2) / 1_000_000.0


def multipolygon_area_km2(coords: List[List[List[List[float]]]]) -> float:
    return sum(polygon_area_km2(poly) for poly in coords)


def main():
    if len(sys.argv) < 3:
        print("Usage: check_and_copy.py <input.geojson> <output.geojson>", file=sys.stderr)
        sys.exit(1)

    ipath, opath = sys.argv[1], sys.argv[2]
    with open(ipath, "r", encoding="utf-8") as f:
        data = json.load(f)

    feats = data.get("features", [])
    over_threshold = []
    for idx, feat in enumerate(feats):
        geom = feat.get("geometry") or {}
        gtype = geom.get("type")
        name = (feat.get("properties") or {}).get("Name") or (feat.get("properties") or {}).get("name") or f"feature_{idx}"
        if gtype == "Polygon":
            a = polygon_area_km2(geom.get("coordinates", []))
        elif gtype == "MultiPolygon":
            a = multipolygon_area_km2(geom.get("coordinates", []))
        else:
            a = 0.0
        if a > 400.0:
            over_threshold.append((idx, name, a))

    # Write a copy regardless; splitting requires geometry ops not available without extra deps
    with open(opath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)

    if over_threshold:
        print("WARNING: Some polygons exceed 400 km^2 and would need splitting:")
        for idx, name, a in over_threshold:
            print(f" - #{idx} {name}: ~{a:.1f} km^2")
    else:
        print("No polygons exceed 400 km^2; no splitting required. Output is an identical copy.")
    print(f"Wrote: {opath}")


if __name__ == "__main__":
    main()

