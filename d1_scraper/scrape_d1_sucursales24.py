#!/usr/bin/env python3
import argparse
import csv
import json
import pathlib
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import hashlib
from concurrent.futures import ThreadPoolExecutor, as_completed
from html.parser import HTMLParser
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

BASE = "https://www.sucursales24.com.co"
INDEX_URL = f"{BASE}/tiendas-d1/todos/"
USER_AGENT = "d1-scraper-s24/1.0 (contact: local)"

REQUEST_TIMEOUT_SECS = 45
DELAY_BETWEEN_REQUESTS_SECS = 0.2

CITY_LINK_RE = re.compile(r"href=\"(https?://www\.sucursales24\.com\.co/[^/]+/tiendas-d1/)\"", re.I)
STORE_LINK_RE = re.compile(r"<a[^>]+href=\"(https?://www\\.sucursales24\\.com\\.co/[^/]+/tiendas-d1/[^\"]+/)\"", re.I)
JSON_LD_RE = re.compile(r"<script[^>]+type=\"application/ld\+json\"[^>]*>(.*?)</script>", re.I | re.S)


def http_get(url: str) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT_SECS) as resp:
        data = resp.read()
    time.sleep(DELAY_BETWEEN_REQUESTS_SECS)
    return data.decode("utf-8", errors="replace")


def http_get_cached(url: str, cache_dir: pathlib.Path | None) -> str:
    if cache_dir is None:
        return http_get(url)
    cache_dir.mkdir(parents=True, exist_ok=True)
    key = hashlib.md5(url.encode("utf-8")).hexdigest() + ".html"
    path = cache_dir / key
    if path.exists():
        try:
            return path.read_text(encoding="utf-8")
        except Exception:
            pass
    html = http_get(url)
    try:
        path.write_text(html, encoding="utf-8")
    except Exception:
        pass
    return html


def extract_city_links(html: str) -> List[str]:
    return sorted(set(m.group(1).rstrip("/") + "/" for m in CITY_LINK_RE.finditer(html)))


class _AnchorExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.links: List[str] = []

    def handle_starttag(self, tag: str, attrs: List[Tuple[str, Optional[str]]]) -> None:
        if tag.lower() != "a":
            return
        attributes = dict(attrs)
        href = attributes.get("href")
        cls = attributes.get("class", "") or ""
        if not href:
            return
        if "/tiendas-d1/" not in href:
            return
        # Prefer anchors marked as store entries
        if "sucursal-listada" in cls or href.rstrip("/").split("/")[-2] != "tiendas-d1":
            self.links.append(href)


def extract_store_links(html: str) -> List[str]:
    parser = _AnchorExtractor()
    try:
        parser.feed(html)
    except Exception:
        pass
    links = [l for l in parser.links if l.rstrip("/") and not l.rstrip("/").endswith("/tiendas-d1")]
    if links:
        return sorted(set(links))
    # Fallback: regex-based
    links = [m.group(1) for m in STORE_LINK_RE.finditer(html)]
    links = [l for l in links if not l.rstrip("/").endswith("/tiendas-d1")]
    return sorted(set(links))


def parse_place_from_json_ld(html: str) -> Optional[Dict[str, Any]]:
    for m in JSON_LD_RE.finditer(html):
        text = m.group(1).strip()
        if not text:
            continue
        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            continue
        # Could be list or object
        candidates: List[Dict[str, Any]] = []
        if isinstance(data, dict):
            candidates = [data]
        elif isinstance(data, list):
            candidates = [obj for obj in data if isinstance(obj, dict)]
        for obj in candidates:
            obj_type = obj.get("@type")
            if obj_type in ("Place", ["Place"]):
                return obj
    return None


def extract_store_fields(store_url: str, html: str) -> Optional[Dict[str, Any]]:
    place = parse_place_from_json_ld(html)
    if not place:
        return None
    name = place.get("name")
    address = place.get("address") or {}
    street = address.get("streetAddress") if isinstance(address, dict) else None
    city = address.get("addressLocality") if isinstance(address, dict) else None
    geo = place.get("geo") or {}
    lat = geo.get("latitude") if isinstance(geo, dict) else None
    lon = geo.get("longitude") if isinstance(geo, dict) else None
    try:
        lat_f = float(lat) if lat is not None else None
        lon_f = float(lon) if lon is not None else None
    except (TypeError, ValueError):
        lat_f = None
        lon_f = None
    return {
        "source": "sucursales24",
        "name": name,
        "street_address": street,
        "city": city,
        "lat": lat_f,
        "lon": lon_f,
        "page_url": store_url,
    }


def write_csv(rows: List[Dict[str, Any]], path: str) -> None:
    if not rows:
        return
    fieldnames = list(rows[0].keys())
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)


def write_geojson(rows: List[Dict[str, Any]], path: str) -> None:
    fc: Dict[str, Any] = {"type": "FeatureCollection", "features": []}
    for r in rows:
        lat = r.get("lat")
        lon = r.get("lon")
        if lat is None or lon is None:
            continue
        props = dict(r)
        props.pop("lat", None)
        props.pop("lon", None)
        fc["features"].append(
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [lon, lat]},
                "properties": props,
            }
        )
    with open(path, "w", encoding="utf-8") as f:
        json.dump(fc, f, ensure_ascii=False, indent=2)


def main() -> None:
    parser = argparse.ArgumentParser(description="Scrape Tiendas D1 from Sucursales24 (no API keys)")
    parser.add_argument("--outdir", default="./output", help="Directory to write outputs")
    parser.add_argument("--max_cities", type=int, default=0, help="Optional limit on number of cities to crawl")
    parser.add_argument("--max_stores_per_city", type=int, default=0, help="Optional limit of stores per city")
    parser.add_argument("--workers", type=int, default=8, help="Concurrent workers for fetching")
    args = parser.parse_args()

    outdir = pathlib.Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    cache_root = outdir.parent / "cache" / "s24"
    cache_root.mkdir(parents=True, exist_ok=True)

    print("Fetching index:", INDEX_URL)
    # Fetch the index; if it fails, attempt to reuse locally saved copy if present
    index_html: Optional[str] = None
    try:
        index_html = http_get_cached(INDEX_URL, cache_root)
    except Exception as e:
        print("Failed to fetch index:", e, file=sys.stderr)
        try:
            index_html = (outdir.parent / "temp" / "s24-d1-todos.html").read_text(encoding="utf-8")
            print("Using local cached index HTML")
        except Exception:
            sys.exit(2)

    city_links = extract_city_links(index_html or "")
    if args.max_cities and args.max_cities > 0:
        city_links = city_links[: args.max_cities]
    print(f"Found {len(city_links)} city pages")

    seen_store_urls: Set[str] = set()
    rows: List[Dict[str, Any]] = []

    def fetch_city(url: str) -> Tuple[str, List[str]]:
        try:
            html = http_get_cached(url, cache_root)
        except Exception as e:
            # Try to read a local sample if exists (for testing)
            slug = url.strip("/").split("/")[-2]
            sample = outdir.parent / "temp" / f"s24-{slug}.html"
            if sample.exists():
                html = sample.read_text(encoding="utf-8")
            else:
                return (url, [])
        store_links = extract_store_links(html)
        return (url, store_links)

    print(f"Fetching {len(city_links)} city pages with {args.workers} workers...")
    city_to_storelinks: Dict[str, List[str]] = {}
    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as pool:
        futures = {pool.submit(fetch_city, cu): cu for cu in city_links}
        idx = 0
        for fut in as_completed(futures):
            idx += 1
            cu = futures[fut]
            try:
                url, store_links = fut.result()
            except Exception:
                url, store_links = (cu, [])
            if args.max_stores_per_city and args.max_stores_per_city > 0:
                store_links = store_links[: args.max_stores_per_city]
            city_to_storelinks[url] = store_links
            print(f"[{idx}/{len(city_links)}] {url} -> {len(store_links)} stores")

    # Flatten and unique store URLs
    all_store_urls: List[str] = []
    for slist in city_to_storelinks.values():
        all_store_urls.extend(slist)
    all_store_urls = [u for u in all_store_urls if u not in seen_store_urls]
    all_store_urls = sorted(set(all_store_urls))

    print(f"Fetching {len(all_store_urls)} store pages with {args.workers} workers...")

    def fetch_store(url: str) -> Optional[Dict[str, Any]]:
        try:
            html = http_get_cached(url, cache_root)
        except Exception:
            return None
        data = extract_store_fields(url, html)
        return data

    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as pool:
        futures = {pool.submit(fetch_store, su): su for su in all_store_urls}
        idx = 0
        for fut in as_completed(futures):
            idx += 1
            su = futures[fut]
            try:
                rec = fut.result()
            except Exception:
                rec = None
            if rec:
                rows.append(rec)
            if idx % 50 == 0:
                print(f"  Processed {idx}/{len(all_store_urls)} store pages")

    # Deduplicate by page_url
    unique_rows: Dict[str, Dict[str, Any]] = {r["page_url"]: r for r in rows}
    final_rows = list(unique_rows.values())

    csv_path = outdir / "s24_d1_stores_colombia.csv"
    geojson_path = outdir / "s24_d1_stores_colombia.geojson"

    write_csv(final_rows, str(csv_path))
    write_geojson(final_rows, str(geojson_path))

    print(f"Wrote {len(final_rows)} S24 rows to {outdir}")


if __name__ == "__main__":
    main()