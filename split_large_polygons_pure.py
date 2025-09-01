#!/usr/bin/env python3
import json
import math
import os
import sys
from typing import List, Tuple, Dict, Any


EARTH_RADIUS_M = 6_371_008.8


def deg_to_rad(deg: float) -> float:
    return deg * math.pi / 180.0


def rad_to_deg(rad: float) -> float:
    return rad * 180.0 / math.pi


def build_local_proj(lat0_deg: float, lon0_deg: float):
    cos_phi0 = math.cos(deg_to_rad(lat0_deg))
    r = EARTH_RADIUS_M

    def fwd(lon_deg: float, lat_deg: float) -> Tuple[float, float]:
        x = r * deg_to_rad(lon_deg - lon0_deg) * cos_phi0
        y = r * deg_to_rad(lat_deg - lat0_deg)
        return (x, y)

    def inv(x: float, y: float) -> Tuple[float, float]:
        lon = lon0_deg + rad_to_deg(x / (r * cos_phi0))
        lat = lat0_deg + rad_to_deg(y / r)
        return (lon, lat)

    return fwd, inv


def polygon_xy_area_m2(points_xy: List[Tuple[float, float]]) -> float:
    if len(points_xy) < 3:
        return 0.0
    area2 = 0.0
    n = len(points_xy)
    for i in range(n):
        x1, y1 = points_xy[i]
        x2, y2 = points_xy[(i + 1) % n]
        area2 += (x1 * y2 - x2 * y1)
    return 0.5 * area2


def close_ring(coords: List[Tuple[float, float]]) -> List[Tuple[float, float]]:
    if not coords:
        return coords
    if coords[0] != coords[-1]:
        return coords + [coords[0]]
    return coords


def sutherland_hodgman_clip(subject_polygon: List[Tuple[float, float]],
                             clip_rect: Tuple[float, float, float, float]) -> List[Tuple[float, float]]:
    """
    Clip a polygon by an axis-aligned rectangle (xmin, ymin, xmax, ymax).
    Returns the resulting polygon as a list of points (not explicitly closed).
    """
    xmin, ymin, xmax, ymax = clip_rect

    def inside_left(p):
        return p[0] >= xmin

    def inside_right(p):
        return p[0] <= xmax

    def inside_bottom(p):
        return p[1] >= ymin

    def inside_top(p):
        return p[1] <= ymax

    def intersect(p1, p2, boundary: str):
        x1, y1 = p1
        x2, y2 = p2
        if boundary == 'left':
            x = xmin
            if x2 != x1:
                t = (xmin - x1) / (x2 - x1)
            else:
                t = 0.0
            y = y1 + t * (y2 - y1)
            return (x, y)
        elif boundary == 'right':
            x = xmax
            if x2 != x1:
                t = (xmax - x1) / (x2 - x1)
            else:
                t = 0.0
            y = y1 + t * (y2 - y1)
            return (x, y)
        elif boundary == 'bottom':
            y = ymin
            if y2 != y1:
                t = (ymin - y1) / (y2 - y1)
            else:
                t = 0.0
            x = x1 + t * (x2 - x1)
            return (x, y)
        elif boundary == 'top':
            y = ymax
            if y2 != y1:
                t = (ymax - y1) / (y2 - y1)
            else:
                t = 0.0
            x = x1 + t * (x2 - x1)
            return (x, y)
        else:
            return p2

    def clip_polygon(poly: List[Tuple[float, float]], boundary: str, inside_fn) -> List[Tuple[float, float]]:
        if not poly:
            return []
        output: List[Tuple[float, float]] = []
        prev = poly[-1]
        for curr in poly:
            prev_inside = inside_fn(prev)
            curr_inside = inside_fn(curr)
            if curr_inside:
                if not prev_inside:
                    output.append(intersect(prev, curr, boundary))
                output.append(curr)
            elif prev_inside:
                output.append(intersect(prev, curr, boundary))
            prev = curr
        return output

    out = subject_polygon[:]
    out = clip_polygon(out, 'left', inside_left)
    if not out:
        return []
    out = clip_polygon(out, 'right', inside_right)
    if not out:
        return []
    out = clip_polygon(out, 'bottom', inside_bottom)
    if not out:
        return []
    out = clip_polygon(out, 'top', inside_top)
    return out


def approximate_polygon_area_km2(lonlat_coords: List[List[float]]) -> float:
    # Remove Z if present and ensure closed ring handling inside projection
    pts = [(float(p[0]), float(p[1])) for p in lonlat_coords if len(p) >= 2]
    if len(pts) < 3:
        return 0.0
    # centroid reference for projection
    lon0 = sum(p[0] for p in pts[:-1]) / max(1, (len(pts) - 1))
    lat0 = sum(p[1] for p in pts[:-1]) / max(1, (len(pts) - 1))
    fwd, _ = build_local_proj(lat0, lon0)
    poly_xy = [fwd(lon, lat) for lon, lat in pts]
    # drop closing duplicate for area function if present
    if poly_xy[0] == poly_xy[-1]:
        poly_xy = poly_xy[:-1]
    area_m2 = abs(polygon_xy_area_m2(poly_xy))
    return area_m2 / 1_000_000.0


def split_polygon_feature(geom: Dict[str, Any], props: Dict[str, Any],
                          area_threshold_km2: float, max_part_area_km2: float,
                          grid_cell_area_km2: float = 320.0) -> List[Dict[str, Any]]:
    gtype = geom.get("type")
    coords = geom.get("coordinates", [])

    def split_single_polygon(outer_lonlat: List[List[float]]) -> List[List[List[float]]]:
        # Prepare projection centered on polygon
        pts = [(float(p[0]), float(p[1])) for p in outer_lonlat if len(p) >= 2]
        if pts[0] == pts[-1]:
            pts = pts[:-1]
        lon0 = sum(p[0] for p in pts) / len(pts)
        lat0 = sum(p[1] for p in pts) / len(pts)
        fwd, inv = build_local_proj(lat0, lon0)

        poly_xy = [fwd(lon, lat) for lon, lat in pts]
        area_km2 = abs(polygon_xy_area_m2(poly_xy)) / 1_000_000.0
        if area_km2 <= area_threshold_km2:
            return [outer_lonlat]

        # Build grid
        xs = [p[0] for p in poly_xy]
        ys = [p[1] for p in poly_xy]
        minx, maxx = min(xs), max(xs)
        miny, maxy = min(ys), max(ys)

        cell_area_m2 = grid_cell_area_km2 * 1_000_000.0
        cell_size = math.sqrt(cell_area_m2)

        # align grid to multiples of cell_size
        start_x = math.floor(minx / cell_size) * cell_size
        start_y = math.floor(miny / cell_size) * cell_size
        end_x = math.ceil(maxx / cell_size) * cell_size
        end_y = math.ceil(maxy / cell_size) * cell_size

        parts: List[List[List[float]]] = []
        y = start_y
        while y < end_y:
            x = start_x
            while x < end_x:
                rect = (x, y, x + cell_size, y + cell_size)
                clipped_xy = sutherland_hodgman_clip(poly_xy, rect)
                if len(clipped_xy) >= 3:
                    # enforce area limit (should already be <= cell area)
                    part_area_km2 = abs(polygon_xy_area_m2(clipped_xy)) / 1_000_000.0
                    if part_area_km2 <= 0.0005:
                        x += cell_size
                        continue
                    # Back to lon/lat and close ring
                    part_lonlat = [list(inv(px, py)) for (px, py) in clipped_xy]
                    if part_lonlat[0] != part_lonlat[-1]:
                        part_lonlat.append(part_lonlat[0])
                    parts.append(part_lonlat)
                x += cell_size
            y += cell_size

        if not parts:
            # Fallback: return original
            return [outer_lonlat]

        return parts

    out_features: List[Dict[str, Any]] = []
    base_name = props.get("Name") or props.get("name") or "Unnamed"

    if gtype == "Polygon":
        # Use only outer ring for splitting; holes are ignored for splitting
        rings = coords
        if not rings:
            return []
        outer = rings[0]
        parts = split_single_polygon(outer)
        if len(parts) == 1:
            out_features.append({"type": "Feature", "properties": props, "geometry": {"type": "Polygon", "coordinates": [parts[0]]}})
        else:
            for i, ring in enumerate(parts, start=1):
                new_props = dict(props)
                new_props["Name"] = f"{base_name} - part {i:02d}"
                out_features.append({"type": "Feature", "properties": new_props, "geometry": {"type": "Polygon", "coordinates": [ring]}})
    elif gtype == "MultiPolygon":
        part_rings: List[List[List[float]]] = []
        for poly in coords:
            if not poly:
                continue
            outer = poly[0]
            part_rings.extend(split_single_polygon(outer))
        if not part_rings:
            return []
        if len(part_rings) == 1:
            out_features.append({"type": "Feature", "properties": props, "geometry": {"type": "Polygon", "coordinates": [part_rings[0]]}})
        else:
            for i, ring in enumerate(part_rings, start=1):
                new_props = dict(props)
                new_props["Name"] = f"{base_name} - part {i:02d}"
                out_features.append({"type": "Feature", "properties": new_props, "geometry": {"type": "Polygon", "coordinates": [ring]}})
    else:
        # pass through other geometry types
        out_features.append({"type": "Feature", "properties": props, "geometry": geom})

    return out_features


def process_geojson(input_path: str, output_path: str,
                    area_threshold_km2: float = 400.0,
                    max_part_area_km2: float = 350.0) -> None:
    with open(input_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if data.get("type") != "FeatureCollection":
        raise ValueError("Input GeoJSON must be a FeatureCollection")

    features = data.get("features", [])
    out_features: List[Dict[str, Any]] = []

    for feat in features:
        geom = feat.get("geometry")
        props = feat.get("properties", {})
        if not geom:
            continue
        gtype = geom.get("type")
        if gtype in ("Polygon", "MultiPolygon"):
            out_features.extend(
                split_polygon_feature(
                    geom,
                    props,
                    area_threshold_km2=area_threshold_km2,
                    max_part_area_km2=max_part_area_km2,
                )
            )
        else:
            out_features.append(feat)

    out_data: Dict[str, Any] = {
        "type": "FeatureCollection",
        **({"name": data.get("name")} if data.get("name") is not None else {}),
        **({"crs": data.get("crs")} if data.get("crs") is not None else {}),
        "features": out_features,
    }

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(out_data, f, ensure_ascii=False)


def main():
    if len(sys.argv) < 3:
        print(
            "Usage: split_large_polygons_pure.py <input.geojson> <output.geojson> [area_threshold_km2=400] [max_part_area_km2=350]",
            file=sys.stderr,
        )
        sys.exit(1)

    input_path = sys.argv[1]
    output_path = sys.argv[2]
    area_threshold_km2 = float(sys.argv[3]) if len(sys.argv) >= 4 else 400.0
    max_part_area_km2 = float(sys.argv[4]) if len(sys.argv) >= 5 else 350.0

    process_geojson(input_path, output_path, area_threshold_km2, max_part_area_km2)
    print(f"Wrote: {output_path}")


if __name__ == "__main__":
    main()

