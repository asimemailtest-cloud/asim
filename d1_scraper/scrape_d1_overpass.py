#!/usr/bin/env python3
import argparse
import csv
import json
import pathlib
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, List

OVERPASS_URLS = (
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
)

QUERY_TEMPLATE = """
[out:json][timeout:1800];
area["ISO3166-1"="CO"][admin_level=2]->.co;
(
  nwr["name"~"^(Tiendas? )?D1$|^D1$|^TIENDA D1$", i](area.co);
  nwr["brand"~"D1", i](area.co);
  nwr["operator"~"D1|KOBA", i](area.co);
);
out center tags;
"""


def fetch_overpass(query: str, retries: int = 4, backoff: float = 1.8) -> Dict[str, Any]:
    user_agent = "d1-scraper/1.0 (contact: local)"
    last_error: Exception | None = None
    encoded_data = urllib.parse.urlencode({"data": query}).encode("utf-8")
    for base_url in OVERPASS_URLS:
        for attempt in range(1, retries + 1):
            request = urllib.request.Request(
                base_url,
                data=encoded_data,
                headers={"User-Agent": user_agent, "Content-Type": "application/x-www-form-urlencoded"},
                method="POST",
            )
            try:
                with urllib.request.urlopen(request, timeout=180) as response:
                    status = getattr(response, "status", 200)
                    body = response.read()
                    text = body.decode("utf-8", errors="replace")
                    if status in {429, 500, 502, 503, 504}:
                        raise RuntimeError(f"overpass error: {status}")
                    if "Too many requests" in text or "rate_limited" in text:
                        raise RuntimeError("overpass rate limited")
                    return json.loads(text)
            except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError, json.JSONDecodeError, Exception) as error:  # noqa: BLE001
                last_error = error
                sleep_seconds = backoff ** attempt
                time.sleep(sleep_seconds)
        # try next server on failure
    if last_error is not None:
        raise last_error
    raise RuntimeError("Unknown Overpass error")


def elements_to_rows(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for element in data.get("elements", []):
        tags: Dict[str, Any] = element.get("tags", {}) or {}
        if element.get("type") == "node":
            latitude, longitude = element.get("lat"), element.get("lon")
        else:
            center = element.get("center") or {}
            latitude, longitude = center.get("lat"), center.get("lon")
        rows.append(
            {
                "osm_id": element.get("id"),
                "osm_type": element.get("type"),
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
                "lat": latitude,
                "lon": longitude,
            }
        )
    return rows


def write_csv(rows: List[Dict[str, Any]], path: str) -> None:
    if not rows:
        return
    fieldnames = list(rows[0].keys())
    with open(path, "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_geojson(rows: List[Dict[str, Any]], path: str) -> None:
    feature_collection: Dict[str, Any] = {"type": "FeatureCollection", "features": []}
    for record in rows:
        if record.get("lat") is None or record.get("lon") is None:
            continue
        properties = dict(record)
        latitude = properties.pop("lat")
        longitude = properties.pop("lon")
        feature_collection["features"].append(
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [longitude, latitude]},
                "properties": properties,
            }
        )
    with open(path, "w", encoding="utf-8") as file:
        json.dump(feature_collection, file, ensure_ascii=False, indent=2)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch Tiendas D1 locations in Colombia from OpenStreetMap Overpass"
    )
    parser.add_argument("--outdir", default="./output", help="Directory to write outputs")
    arguments = parser.parse_args()

    output_directory = pathlib.Path(arguments.outdir)
    output_directory.mkdir(parents=True, exist_ok=True)

    data = fetch_overpass(QUERY_TEMPLATE)
    rows = elements_to_rows(data)

    write_csv(rows, str(output_directory / "d1_stores_colombia.csv"))
    write_geojson(rows, str(output_directory / "d1_stores_colombia.geojson"))

    print(f"Wrote {len(rows)} rows to {output_directory}")


if __name__ == "__main__":
    main()