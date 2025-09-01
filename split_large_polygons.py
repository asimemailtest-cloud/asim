#!/usr/bin/env python3
import json
import math
import os
import sys
from typing import List, Tuple

from shapely.geometry import shape, mapping, box, Polygon, MultiPolygon, BaseGeometry
from shapely.ops import transform
from pyproj import CRS, Transformer


def compute_laea_crs_for_geometry(geom_wgs84: BaseGeometry) -> CRS:
    centroid = geom_wgs84.centroid
    lon, lat = centroid.x, centroid.y
    # Lambert Azimuthal Equal Area centered on geometry for accurate area and grid sizing
    return CRS.from_proj4(
        f"+proj=laea +lat_0={lat} +lon_0={lon} +datum=WGS84 +units=m +no_defs"
    )


def project_geometry(geom: BaseGeometry, src_crs: CRS, dst_crs: CRS) -> BaseGeometry:
    transformer = Transformer.from_crs(src_crs, dst_crs, always_xy=True)
    return transform(lambda x, y, z=None: transformer.transform(x, y), geom)


def create_grid(bounds: Tuple[float, float, float, float], cell_size_m: float) -> List[Polygon]:
    minx, miny, maxx, maxy = bounds
    start_x = math.floor(minx / cell_size_m) * cell_size_m
    start_y = math.floor(miny / cell_size_m) * cell_size_m
    end_x = math.ceil(maxx / cell_size_m) * cell_size_m
    end_y = math.ceil(maxy / cell_size_m) * cell_size_m

    cells: List[Polygon] = []
    y = start_y
    while y < end_y:
        x = start_x
        while x < end_x:
            cells.append(box(x, y, x + cell_size_m, y + cell_size_m))
            x += cell_size_m
        y += cell_size_m
    return cells


def split_large_polygon_into_small_parts(
    geom_wgs84: BaseGeometry,
    area_threshold_km2: float,
    max_part_area_km2: float,
    grid_cell_area_km2: float = 320.0,
) -> List[BaseGeometry]:
    """
    Returns a list of geometries in WGS84. If the input is below threshold, returns [geom_wgs84].
    Otherwise, returns intersections against a square grid sized so that each cell area < max_part_area.
    """
    wgs84 = CRS.from_epsg(4326)
    laea = compute_laea_crs_for_geometry(geom_wgs84)

    geom_laea = project_geometry(geom_wgs84, wgs84, laea)
    area_km2 = geom_laea.area / 1_000_000.0

    if area_km2 <= area_threshold_km2:
        return [geom_wgs84]

    # Grid cell size chosen to be safely under the max part area
    cell_area_m2 = grid_cell_area_km2 * 1_000_000.0
    cell_size_m = math.sqrt(cell_area_m2)

    grid_cells = create_grid(geom_laea.bounds, cell_size_m)
    parts_laea: List[BaseGeometry] = []
    for cell in grid_cells:
        if not geom_laea.intersects(cell):
            continue
        inter = geom_laea.intersection(cell)
        if inter.is_empty:
            continue
        # Ensure polygonal outputs only
        if isinstance(inter, (Polygon, MultiPolygon)):
            parts_laea.append(inter)

    # Project parts back to WGS84
    parts_wgs84: List[BaseGeometry] = []
    back_transformer = Transformer.from_crs(laea, wgs84, always_xy=True)
    for p in parts_laea:
        p_wgs84 = transform(lambda x, y, z=None: back_transformer.transform(x, y), p)
        if not p_wgs84.is_empty:
            parts_wgs84.append(p_wgs84)

    # Sanity: filter out microscopic slivers
    def is_significant(g: BaseGeometry) -> bool:
        gl = project_geometry(g, wgs84, laea)
        return (gl.area / 1_000_000.0) > 0.001  # > 0.001 km²

    parts_wgs84 = [g for g in parts_wgs84 if is_significant(g)]

    # If something odd happened and no parts were produced, fall back to original geometry
    if not parts_wgs84:
        return [geom_wgs84]

    # If any part exceeds max_part_area_km2 (rare due to grid), recursively split that part
    final_parts: List[BaseGeometry] = []
    for g in parts_wgs84:
        gl = project_geometry(g, wgs84, laea)
        if (gl.area / 1_000_000.0) > max_part_area_km2:
            final_parts.extend(
                split_large_polygon_into_small_parts(
                    g, area_threshold_km2=0.0, max_part_area_km2=max_part_area_km2, grid_cell_area_km2=grid_cell_area_km2
                )
            )
        else:
            final_parts.append(g)

    return final_parts


def format_part_suffix(index: int) -> str:
    return f" - part {index:02d}"


def process_geojson(
    input_path: str,
    output_path: str,
    area_threshold_km2: float = 400.0,
    max_part_area_km2: float = 350.0,
) -> None:
    with open(input_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if data.get("type") != "FeatureCollection":
        raise ValueError("Input GeoJSON must be a FeatureCollection")

    features = data.get("features", [])
    out_features = []

    for feat in features:
        geom = feat.get("geometry")
        props = feat.get("properties", {})
        if not geom:
            continue

        gtype = geom.get("type")
        if gtype in ("Polygon", "MultiPolygon"):
            geom_obj = shape(geom)
            parts = split_large_polygon_into_small_parts(
                geom_obj,
                area_threshold_km2=area_threshold_km2,
                max_part_area_km2=max_part_area_km2,
            )

            if len(parts) == 1:
                # No split
                out_features.append({
                    "type": "Feature",
                    "properties": props,
                    "geometry": mapping(parts[0]),
                })
            else:
                base_name = props.get("Name") or props.get("name") or "Unnamed"
                for idx, part in enumerate(parts, start=1):
                    new_props = dict(props)
                    new_props["Name"] = f"{base_name}{format_part_suffix(idx)}"
                    out_features.append({
                        "type": "Feature",
                        "properties": new_props,
                        "geometry": mapping(part),
                    })
        else:
            # Non-polygon features are passed through unchanged
            out_features.append(feat)

    out_data = {
        "type": "FeatureCollection",
        # preserve name if present
        **({"name": data.get("name")} if data.get("name") is not None else {}),
        # preserve crs if present
        **({"crs": data.get("crs")} if data.get("crs") is not None else {}),
        "features": out_features,
    }

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(out_data, f, ensure_ascii=False)


def main():
    if len(sys.argv) < 3:
        print(
            "Usage: split_large_polygons.py <input.geojson> <output.geojson> [area_threshold_km2=400] [max_part_area_km2=350]",
            file=sys.stderr,
        )
        sys.exit(1)

    input_path = sys.argv[1]
    output_path = sys.argv[2]
    area_threshold_km2 = float(sys.argv[3]) if len(sys.argv) >= 4 else 400.0
    max_part_area_km2 = float(sys.argv[4]) if len(sys.argv) >= 5 else 350.0

    if not os.path.exists(input_path):
        raise FileNotFoundError(input_path)

    process_geojson(input_path, output_path, area_threshold_km2, max_part_area_km2)
    print(f"Wrote: {output_path}")


if __name__ == "__main__":
    main()

