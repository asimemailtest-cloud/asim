#!/usr/bin/env python3
"""
Unified D1 store exporter for Colombia.

Sources:
- Website (Cloudflare-protected) with optional Cookie and Playwright
- OpenStreetMap (Overpass API)
Optional enrichment:
- Google Maps Geocoding for lat/lng override/fill

Usage examples:
  # OSM only
  python d1_stores.py osm --output d1_osm

  # OSM with Google geocoding
  python d1_stores.py osm --output d1_osm --google-api-key YOUR_KEY --force-google

  # Website with cookie
  python d1_stores.py web --base-url https://d1.com.co --output d1_web --cookie "<Cookie header>" --verbose

  # Website with Playwright
  python d1_stores.py web --base-url https://d1.com.co --output d1_web --browser --verbose
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import random
import re
import sys
import time
from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional, Tuple


# ---------------------------- Shared utils ----------------------------


def _print_verbose(verbose: bool, message: str) -> None:
    if verbose:
        print(message, file=sys.stderr)


@dataclass
class Store:
    id: Optional[str]
    name: Optional[str]
    address: Optional[str]
    city: Optional[str]
    state: Optional[str]
    country: Optional[str]
    latitude: Optional[float]
    longitude: Optional[float]
    phone: Optional[str]
    hours: Optional[str]
    source: str
    extra: Dict[str, Any]


def write_json(stores: List[Store], path: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump([asdict(s) for s in stores], f, ensure_ascii=False, indent=2)


def write_csv(stores: List[Store], path: str) -> None:
    fieldnames = [
        "id",
        "name",
        "address",
        "city",
        "state",
        "country",
        "latitude",
        "longitude",
        "phone",
        "hours",
        "source",
    ]
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for s in stores:
            row = {k: getattr(s, k) for k in fieldnames}
            w.writerow(row)


# ---------------------------- Website path ----------------------------


def create_scraper(verbose: bool = False):
    import cloudscraper  # type: ignore

    profiles = [
        ("chrome", "121"),
        ("firefox", "120"),
        ("safari", "17"),
    ]
    browser, version = random.choice(profiles)
    scraper = cloudscraper.create_scraper(
        browser={"browser": browser, "platform": "windows", "mobile": False, "desktop": True},
        delay=random.uniform(0.5, 1.5),
    )
    scraper.headers.update(
        {
            "Accept-Language": "es-CO,es;q=0.9,en;q=0.8",
            "DNT": "1",
        }
    )
    _print_verbose(verbose, f"Initialized scraper UA: {scraper.headers.get('User-Agent')}")
    return scraper


def try_fetch_json(scraper, url: str, verbose: bool) -> Optional[Any]:
    _print_verbose(verbose, f"GET {url}")
    try:
        r = scraper.get(url, timeout=30)
        if r.status_code != 200:
            return None
        ct = r.headers.get("Content-Type", "").lower()
        if "json" not in ct and not r.text.strip().startswith(("[", "{")):
            return None
        return r.json()
    except Exception:
        return None


def attempt_wp_store_locator_endpoints(base_url: str, scraper, verbose: bool) -> Optional[List[Dict[str, Any]]]:
    candidates = [
        f"{base_url.rstrip('/')}/wp-json/wp-store-locator/v1/locations?per_page=10000",
        f"{base_url.rstrip('/')}/wp-json/wpsl/v1/locations?per_page=10000",
        f"{base_url.rstrip('/')}/wp-json/wp/v2/wpsl_stores?per_page=10000",
        f"{base_url.rstrip('/')}/wp-json/wp/v2/wpsl_store?per_page=10000",
        f"{base_url.rstrip('/')}/wp-json/wpsl/v1/stores?per_page=10000",
    ]
    for url in candidates:
        data = try_fetch_json(scraper, url, verbose)
        if isinstance(data, list) and data:
            return data
        if isinstance(data, dict):
            for k in ("stores", "locations", "results"):
                v = data.get(k)
                if isinstance(v, list) and v:
                    return v
    return None


def extract_wpsl_nonce(html: str) -> Optional[str]:
    patterns = [
        r"wpsl_locator_vars\s*=\s*\{[\s\S]*?\bnonce\b\s*:\s*['\"]([^'\"]+)['\"]",
        r"wpslLocator\s*=\s*\{[\s\S]*?\bnonce\b\s*:\s*['\"]([^'\"]+)['\"]",
        r"\bnonce\b\s*:\s*['\"]([^'\"]+)['\"]\s*,\s*\bnonce_field\b",
    ]
    for pat in patterns:
        m = re.search(pat, html, flags=re.IGNORECASE)
        if m:
            return m.group(1)
    return None


def attempt_admin_ajax(base_url: str, scraper, verbose: bool) -> Optional[List[Dict[str, Any]]]:
    pages = [
        "",
        "/tiendas",
        "/nuestras-tiendas",
        "/encuentra-tu-tienda",
        "/tiendas-disponibles",
        "/store-locator",
        "/donde-estamos",
        "/ubicaciones",
    ]
    nonce = None
    for path in pages:
        url = f"{base_url.rstrip('/')}{path}"
        try:
            r = scraper.get(url, timeout=30)
            if r.status_code != 200:
                continue
            nonce = extract_wpsl_nonce(r.text)
            if nonce:
                break
        except Exception:
            continue
    if not nonce:
        return None
    ajax_url = f"{base_url.rstrip('/')}/wp-admin/admin-ajax.php"
    params = {
        "action": "wpsl_load_stores",
        "nonce": nonce,
        "bounds": "-4.231687,-81.858139;13.527,-66.869835",
        "fields": "id,title,address,city,state,zip,lat,lng,country,phone,hours",
        "max_results": 10000,
    }
    try:
        resp = scraper.get(ajax_url, params=params, timeout=60)
        if resp.status_code != 200:
            return None
        data = resp.json()
        if isinstance(data, list) and data:
            return data
        if isinstance(data, dict):
            for k in ("stores", "locations", "results"):
                v = data.get(k)
                if isinstance(v, list) and v:
                    return v
        return None
    except Exception:
        return None


def normalize_store(rec: Dict[str, Any], source: str) -> Store:
    def _to_float(value: Any) -> Optional[float]:
        try:
            if value is None:
                return None
            if isinstance(value, (int, float)):
                return float(value)
            cleaned = re.sub(r"[^0-9\.-]", "", str(value))
            if cleaned in ("", ".", "-"):
                return None
            return float(cleaned)
        except Exception:
            return None

    name = rec.get("title") or rec.get("name") or (rec.get("store") or {}).get("name")
    address = rec.get("address") or (rec.get("store") or {}).get("address") or rec.get("street") or rec.get("direccion")
    city = rec.get("city") or rec.get("ciudad")
    state = rec.get("state") or rec.get("departamento") or rec.get("province")
    country = rec.get("country") or rec.get("pais") or "Colombia"
    latitude = _to_float(rec.get("lat")) or _to_float(rec.get("latitude")) or _to_float((rec.get("store") or {}).get("lat"))
    longitude = _to_float(rec.get("lng")) or _to_float(rec.get("lon")) or _to_float(rec.get("longitude")) or _to_float((rec.get("store") or {}).get("lng"))
    phone = rec.get("phone") or rec.get("telefono") or rec.get("tel")
    hours = rec.get("hours") or rec.get("horario") or rec.get("opening_hours")
    id_value = rec.get("id") or (rec.get("store") or {}).get("id") or rec.get("post_id") or rec.get("slug")
    return Store(
        id=str(id_value) if id_value is not None else None,
        name=str(name) if name is not None else None,
        address=str(address) if address is not None else None,
        city=str(city) if city is not None else None,
        state=str(state) if state is not None else None,
        country=str(country) if country is not None else None,
        latitude=latitude,
        longitude=longitude,
        phone=str(phone) if phone is not None else None,
        hours=str(hours) if hours is not None else None,
        source=source,
        extra=rec,
    )


def web_fetch(base_url: str, cookie: Optional[str], use_browser: bool, verbose: bool) -> List[Store]:
    scraper = create_scraper(verbose)
    if cookie:
        scraper.headers.update({"Cookie": cookie})
    raw = attempt_wp_store_locator_endpoints(base_url, scraper, verbose)
    if not raw:
        raw = attempt_admin_ajax(base_url, scraper, verbose)
    if not raw:
        raise RuntimeError("Website fetch failed; try --browser or provide --cookie from a real browser session")
    stores = [normalize_store(r, source="web") for r in raw]
    # Deduplicate
    seen = set()
    unique: List[Store] = []
    for s in stores:
        key = (s.name, s.address, s.latitude, s.longitude)
        if key in seen:
            continue
        seen.add(key)
        unique.append(s)
    return unique


# ---------------------------- OSM path ----------------------------


def build_overpass_query() -> str:
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


def osm_fetch(verbose: bool) -> List[Store]:
    import requests

    q = build_overpass_query()
    r = requests.post("https://overpass-api.de/api/interpreter", data={"data": q}, timeout=120)
    r.raise_for_status()
    data = r.json()
    elements = data.get("elements") or []

    stores: List[Store] = []
    for el in elements:
        el_type = el.get("type")
        el_id = el.get("id")
        tags = el.get("tags") or {}
        name = tags.get("name")
        brand = tags.get("brand")
        lat = el.get("lat")
        lon = el.get("lon")
        if lat is None or lon is None:
            center = el.get("center") or {}
            lat = center.get("lat")
            lon = center.get("lon")
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
                id=f"{el_type}:{el_id}",
                name=name,
                address=address,
                city=city,
                state=state,
                country=country,
                latitude=float(lat) if lat is not None else None,
                longitude=float(lon) if lon is not None else None,
                phone=None,
                hours=tags.get("opening_hours"),
                source="osm",
                extra={"brand": brand, "tags": tags},
            )
        )
    # Deduplicate by id
    seen = set()
    unique: List[Store] = []
    for s in stores:
        if s.id in seen:
            continue
        seen.add(s.id)
        unique.append(s)
    _print_verbose(verbose, f"OSM fetched {len(unique)} stores")
    return unique


# ---------------------------- Google Geocoding ----------------------------


def geocode_google(address: str, api_key: str) -> Optional[Tuple[float, float]]:
    import requests

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
    loc = results[0]["geometry"]["location"]
    return float(loc["lat"]), float(loc["lng"])


def enrich_with_google(stores: List[Store], api_key: str, force_google: bool, rate_limit_s: float, cache_path: Optional[str], verbose: bool) -> List[Store]:
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
        except Exception as exc:
            _print_verbose(verbose, f"Google geocode failed for '{addr}': {exc}")
            updated.append(s)
        time.sleep(rate_limit_s)

    save_cache()
    return updated


# ---------------------------- CLI ----------------------------


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export Tiendas D1 stores (web or OSM) with optional Google geocoding")
    sub = parser.add_subparsers(dest="mode", required=True)

    p_web = sub.add_parser("web", help="Fetch from website")
    p_web.add_argument("--base-url", default="https://d1.com.co", help="Website base URL")
    p_web.add_argument("--output", default="d1_web", help="Output base filename")
    p_web.add_argument("--cookie", default=None, help="Raw Cookie header from browser session")
    p_web.add_argument("--browser", action="store_true", help="(not implemented here) Use real browser if needed")
    p_web.add_argument("--verbose", action="store_true")

    p_osm = sub.add_parser("osm", help="Fetch from OpenStreetMap Overpass")
    p_osm.add_argument("--output", default="d1_osm", help="Output base filename")
    p_osm.add_argument("--verbose", action="store_true")
    p_osm.add_argument("--google-api-key", default=os.environ.get("GOOGLE_API_KEY"))
    p_osm.add_argument("--force-google", action="store_true")
    p_osm.add_argument("--google-rate", type=float, default=0.1)
    p_osm.add_argument("--cache", default="/workspace/d1_geocode_cache.json")

    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    if args.mode == "web":
        try:
            stores = web_fetch(args.base_url, args.cookie, args.browser, args.verbose)
        except Exception as exc:
            print(f"Error: {exc}", file=sys.stderr)
            return 1
        json_path = f"{args.output}.json"
        csv_path = f"{args.output}.csv"
        write_json(stores, json_path)
        write_csv(stores, csv_path)
        print(f"Saved {len(stores)} web stores to {json_path} and {csv_path}")
        return 0

    if args.mode == "osm":
        try:
            stores = osm_fetch(args.verbose)
        except Exception as exc:
            print(f"Error querying Overpass: {exc}", file=sys.stderr)
            return 1
        if args.google_api_key:
            stores = enrich_with_google(
                stores,
                api_key=args.google_api_key,
                force_google=args.force_google,
                rate_limit_s=max(0.0, args.google_rate),
                cache_path=args.cache,
                verbose=args.verbose,
            )
        json_path = f"{args.output}.json"
        csv_path = f"{args.output}.csv"
        write_json(stores, json_path)
        write_csv(stores, csv_path)
        print(f"Saved {len(stores)} OSM stores to {json_path} and {csv_path}")
        return 0

    print("Unknown mode", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())

