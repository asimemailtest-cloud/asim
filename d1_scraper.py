#!/usr/bin/env python3
"""
Scrape D1 stores in Colombia.

Primary strategy: crawl a stable directory site that lists Tiendas D1 stores
in Colombia (e.g., https://www.sucursales24.com.co/tiendas-d1/), traversing
city/department pages and extracting store details. For each store, the script
attempts to parse:
- name
- address
- city / department (best-effort from breadcrumbs and page content)
- phone (if present)
- opening hours (if present)
- latitude / longitude (from JSON-LD and/or Google Maps links if present)

Output: CSV and optional GeoJSON.

Usage:
  python3 d1_scraper.py --output d1_stores.csv --geojson d1_stores.geojson

Notes:
- This scraper is polite: configurable delay and retry logic, sets a UA.
- If the site structure changes, adapt the CSS selectors/JSON-LD parsing.
"""

import argparse
import csv
import json
import math
import re
import sys
import time
from dataclasses import dataclass, asdict
from typing import Dict, Iterable, List, Optional, Set, Tuple

import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, parse_qs, unquote


DEFAULT_BASE_URL = "https://www.sucursales24.com.co/tiendas-d1/"
USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)


@dataclass
class Store:
    name: str
    address: str
    city: str
    department: str
    phone: str
    hours: str
    latitude: Optional[float]
    longitude: Optional[float]
    source_url: str


class PoliteSession:
    def __init__(self, delay_seconds: float = 0.8, timeout_seconds: float = 20.0, max_retries: int = 3):
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT, "Accept-Language": "es-CO,es;q=0.8,en;q=0.6"})
        self.delay = delay_seconds
        self.timeout = timeout_seconds
        self.max_retries = max_retries
        self._last_request_ts = 0.0

    def get(self, url: str) -> requests.Response:
        # simple rate limit
        sleep_for = max(0.0, self.delay - (time.time() - self._last_request_ts))
        if sleep_for > 0:
            time.sleep(sleep_for)
        for attempt in range(1, self.max_retries + 1):
            try:
                resp = self.session.get(url, timeout=self.timeout, allow_redirects=True)
                self._last_request_ts = time.time()
                if resp.status_code >= 500:
                    raise requests.HTTPError(f"Server error {resp.status_code}")
                return resp
            except Exception as e:
                if attempt >= self.max_retries:
                    raise
                time.sleep(min(2.0 * attempt, 5.0))


def extract_links(base_url: str, html: str) -> List[str]:
    soup = BeautifulSoup(html, "html.parser")
    links: List[str] = []
    for a in soup.select("a[href]"):
        href = a.get("href", "").strip()
        if not href:
            continue
        url = urljoin(base_url, href)
        # keep within domain and under /tiendas-d1/
        if url.startswith(DEFAULT_BASE_URL):
            links.append(url)
    return list(dict.fromkeys(links))  # de-dup preserving order


def is_listing_page(url: str, html: str) -> bool:
    # Heuristic: listing pages often contain many links to the same base path
    return html.lower().count("/tiendas-d1/") > 10


def parse_jsonld_stores(soup: BeautifulSoup) -> List[Dict]:
    stores: List[Dict] = []
    for tag in soup.find_all("script", attrs={"type": "application/ld+json"}):
        try:
            data = json.loads(tag.string or "{}")
        except Exception:
            continue
        candidates = []
        if isinstance(data, dict):
            candidates = [data]
        elif isinstance(data, list):
            candidates = data
        else:
            continue
        for item in candidates:
            if not isinstance(item, dict):
                continue
            t = item.get("@type")
            if not t:
                continue
            t_str = ",".join(t) if isinstance(t, list) else str(t)
            if any(key in t_str.lower() for key in ["store", "localbusiness", "grocery", "departmentstore"]):
                stores.append(item)
    return stores


def parse_coords_from_gmaps_url(url: str) -> Tuple[Optional[float], Optional[float]]:
    try:
        url = unquote(url)
        # Patterns: .../@lat,lon,...  or ...?q=lat,lon ...
        m = re.search(r"/@([\-\d\.]+),([\-\d\.]+)", url)
        if m:
            return float(m.group(1)), float(m.group(2))
        qs = parse_qs(urlparse(url).query)
        if "q" in qs:
            q = qs["q"][0]
            m2 = re.match(r"\s*([\-\d\.]+)\s*,\s*([\-\d\.]+)\s*$", q)
            if m2:
                return float(m2.group(1)), float(m2.group(2))
    except Exception:
        pass
    return None, None


def parse_store_from_page(url: str, html: str) -> List[Store]:
    soup = BeautifulSoup(html, "html.parser")
    jsonld = parse_jsonld_stores(soup)

    # Attempt 1: JSON-LD
    stores: List[Store] = []
    for item in jsonld:
        name = str(item.get("name") or "Tiendas D1").strip()
        addr_node = item.get("address") or {}
        if isinstance(addr_node, dict):
            address = " ".join(
                str(addr_node.get(k) or "").strip()
                for k in ["streetAddress", "addressLocality", "addressRegion"]
                if addr_node.get(k)
            ).strip()
            city = str(addr_node.get("addressLocality") or "").strip()
            department = str(addr_node.get("addressRegion") or "").strip()
        else:
            address = str(item.get("address") or "").strip()
            city = ""
            department = ""
        phone = str(item.get("telephone") or "").strip()
        opening_hours = ""
        oh = item.get("openingHoursSpecification")
        if isinstance(oh, list):
            # Simplify
            opening_hours = "; ".join(
                [
                    f"{x.get('dayOfWeek','')}: {x.get('opens','')} - {x.get('closes','')}".strip()
                    for x in oh if isinstance(x, dict)
                ]
            ).strip("; ")
        elif isinstance(oh, dict):
            opening_hours = f"{oh.get('dayOfWeek','')}: {oh.get('opens','')} - {oh.get('closes','')}".strip()

        lat = lon = None
        geo = item.get("geo") if isinstance(item, dict) else None
        if isinstance(geo, dict):
            try:
                lat = float(geo.get("latitude")) if geo.get("latitude") is not None else None
                lon = float(geo.get("longitude")) if geo.get("longitude") is not None else None
            except Exception:
                lat = lon = None

        if lat is None or lon is None:
            # Attempt 2: Google Maps links on page
            for a in soup.select("a[href*='google.com/maps'], a[href*='goo.gl/maps']"):
                llat, llon = parse_coords_from_gmaps_url(a.get("href", ""))
                if llat is not None and llon is not None:
                    lat, lon = llat, llon
                    break

        stores.append(
            Store(
                name=name or "Tiendas D1",
                address=address,
                city=city,
                department=department,
                phone=phone,
                hours=opening_hours,
                latitude=lat,
                longitude=lon,
                source_url=url,
            )
        )

    # If no JSON-LD, attempt to parse card-like content
    if not stores:
        # Breadcrumbs might hold city/department
        bc_text = " > ".join(x.get_text(" ", strip=True) for x in soup.select(".breadcrumb, .breadcrumbs, nav.breadcrumb"))
        city_guess = ""
        dept_guess = ""
        m_city = re.search(r"\b([A-ZÁÉÍÓÚÑ][a-záéíóúñ]+)\b\s*$", bc_text or "")
        if m_city:
            city_guess = m_city.group(1)
        # Cards
        for card in soup.select(".list-group .list-group-item, .entry-content, .post, article"):
            text = card.get_text(" ", strip=True)
            if not text or "D1" not in text:
                continue
            name = "Tiendas D1"
            address = ""
            phone = ""
            hours = ""
            lat = lon = None
            # Look for address-like patterns
            m_addr = re.search(r"(Carrera|Calle|Avenida|Transversal|Diagonal|Vereda|Via)\s+[^\n,;|]+", text, flags=re.I)
            if m_addr:
                address = m_addr.group(0)
            # Phones
            m_phone = re.search(r"(\+?57\s*)?\(?0?\d{1,3}\)?[\s.-]?\d{3}[\s.-]?\d{4}", text)
            if m_phone:
                phone = m_phone.group(0)
            # Map link in this card
            a = card.select_one("a[href*='google.com/maps'], a[href*='goo.gl/maps']")
            if a:
                lat, lon = parse_coords_from_gmaps_url(a.get("href", ""))

            stores.append(
                Store(
                    name=name,
                    address=address,
                    city=city_guess,
                    department=dept_guess,
                    phone=phone,
                    hours=hours,
                    latitude=lat,
                    longitude=lon,
                    source_url=url,
                )
            )

    # Filter empty duplicates
    unique: Dict[Tuple[str, str], Store] = {}
    for s in stores:
        key = (s.name.strip().lower(), s.address.strip().lower())
        if key not in unique:
            unique[key] = s
    return list(unique.values())


def crawl(base_url: str, max_pages: int = 5000, delay: float = 0.8) -> List[Store]:
    sess = PoliteSession(delay_seconds=delay)
    to_visit: List[str] = [base_url]
    visited: Set[str] = set()
    all_stores: List[Store] = []

    while to_visit and len(visited) < max_pages:
        url = to_visit.pop(0)
        if url in visited:
            continue
        try:
            resp = sess.get(url)
        except Exception as e:
            # Skip problematic page
            continue
        visited.add(url)
        html = resp.text

        # Parse stores from current page
        stores = parse_store_from_page(url, html)
        if stores:
            all_stores.extend(stores)

        # Expand crawl for listing pages
        if is_listing_page(url, html) or url.rstrip("/") == base_url.rstrip("/"):
            for link in extract_links(url, html):
                if link not in visited and link.startswith(base_url):
                    to_visit.append(link)

    # De-dup across pages
    dedup: Dict[Tuple[str, str], Store] = {}
    for s in all_stores:
        key = (s.name.strip().lower(), s.address.strip().lower())
        if not s.address:
            # fallback to url key
            key = (s.name.strip().lower(), s.source_url.strip().lower())
        if key not in dedup:
            dedup[key] = s
    return list(dedup.values())


def write_csv(path: str, stores: List[Store]) -> None:
    fieldnames = list(asdict(stores[0]).keys()) if stores else [
        "name", "address", "city", "department", "phone", "hours", "latitude", "longitude", "source_url"
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for s in stores:
            writer.writerow(asdict(s))


def write_geojson(path: str, stores: List[Store]) -> None:
    feats = []
    for s in stores:
        if s.latitude is not None and s.longitude is not None:
            geom = {"type": "Point", "coordinates": [s.longitude, s.latitude]}
        else:
            geom = None
        props = asdict(s)
        # GeoJSON properties shouldn't duplicate geometry fields
        props.pop("latitude", None)
        props.pop("longitude", None)
        feats.append({
            "type": "Feature",
            "properties": props,
            **({"geometry": geom} if geom else {"geometry": None}),
        })
    fc = {"type": "FeatureCollection", "features": feats}
    with open(path, "w", encoding="utf-8") as f:
        json.dump(fc, f, ensure_ascii=False)


def main():
    ap = argparse.ArgumentParser(description="Scrape Tiendas D1 stores in Colombia")
    ap.add_argument("--base-url", default=DEFAULT_BASE_URL, help="Starting URL for D1 listings")
    ap.add_argument("--output", required=True, help="Output CSV path")
    ap.add_argument("--geojson", default="", help="Optional output GeoJSON path")
    ap.add_argument("--delay", type=float, default=0.8, help="Delay between requests (seconds)")
    ap.add_argument("--max-pages", type=int, default=5000, help="Max pages to crawl")
    args = ap.parse_args()

    stores = crawl(args.base_url, max_pages=args.max_pages, delay=args.delay)
    # Best-effort filter: only keep likely D1 entries
    filtered: List[Store] = []
    for s in stores:
        name_l = (s.name or "").lower()
        if "d1" in name_l or "tiendas" in name_l or "almacenes" in name_l:
            filtered.append(s)

    if not filtered:
        print("No stores parsed. Try increasing --max-pages or adjusting selectors.", file=sys.stderr)

    write_csv(args.output, filtered)
    if args.geojson:
        write_geojson(args.geojson, filtered)
    print(f"Wrote {len(filtered)} stores to {args.output}{' and ' + args.geojson if args.geojson else ''}")


if __name__ == "__main__":
    main()

