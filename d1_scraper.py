#!/usr/bin/env python3
"""
Scrape Tiendas D1 store locations in Colombia.

This script attempts multiple known WordPress Store Locator endpoints and a
fallback that extracts a WP Store Locator nonce from a locator page when needed.

Outputs both JSON and CSV with a normalized schema.

Usage:
  python d1_scraper.py --output stores --base-url https://d1.com.co

Dependencies are listed in requirements.txt.
"""

from __future__ import annotations

import argparse
import csv
import json
import random
import re
import sys
import time
from dataclasses import dataclass, asdict
from typing import Any, Dict, Iterable, List, Optional, Tuple


def _print_verbose(verbose: bool, message: str) -> None:
    if verbose:
        print(message, file=sys.stderr)


def create_scraper(verbose: bool = False):
    """
    Create a Cloudflare-aware HTTP client.
    """
    try:
        import cloudscraper  # type: ignore
    except Exception as exc:
        print(
            "cloudscraper is required. Please run: pip install -r requirements.txt",
            file=sys.stderr,
        )
        raise

    # Vary the browser profile to improve chances of passing anti-bot checks
    browser_profiles = [
        ("chrome", "121"),
        ("firefox", "120"),
        ("safari", "17"),
    ]
    browser, version = random.choice(browser_profiles)
    scraper = cloudscraper.create_scraper(
        browser={"browser": browser, "platform": "windows", "mobile": False, "desktop": True},
        delay=random.uniform(0.5, 1.5),
    )
    # Set a common Accept-Language and DNT to look more like a browser
    scraper.headers.update(
        {
            "User-Agent": scraper.headers.get(
                "User-Agent",
                f"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) {browser.capitalize()}/{version} Safari/537.36",
            ),
            "Accept-Language": "es-CO,es;q=0.9,en;q=0.8",
            "DNT": "1",
        }
    )
    _print_verbose(verbose, f"Initialized scraper with UA: {scraper.headers.get('User-Agent')}")
    return scraper


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


def _to_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return float(value)
        # Remove non-numeric characters except dot and minus
        cleaned = re.sub(r"[^0-9\.-]", "", str(value))
        if cleaned in ("", ".", "-"):
            return None
        return float(cleaned)
    except Exception:
        return None


def normalize_store(record: Dict[str, Any]) -> Store:
    """
    Normalize various known shapes from WP Store Locator and similar plugins.
    """
    # Common keys seen in WP Store Locator JSON
    name = (
        record.get("title")
        or record.get("name")
        or (record.get("store") or {}).get("name")
    )
    address = (
        record.get("address")
        or (record.get("store") or {}).get("address")
        or record.get("street")
        or record.get("direccion")
    )
    city = record.get("city") or record.get("ciudad")
    state = record.get("state") or record.get("departamento") or record.get("province")
    country = record.get("country") or record.get("pais") or "Colombia"

    latitude = (
        _to_float(record.get("lat"))
        or _to_float(record.get("latitude"))
        or _to_float((record.get("store") or {}).get("lat"))
    )
    longitude = (
        _to_float(record.get("lng"))
        or _to_float(record.get("lon"))
        or _to_float(record.get("longitude"))
        or _to_float((record.get("store") or {}).get("lng"))
    )

    phone = record.get("phone") or record.get("telefono") or record.get("tel")
    hours = record.get("hours") or record.get("horario") or record.get("opening_hours")

    # Try several id fields
    id_value = (
        record.get("id")
        or (record.get("store") or {}).get("id")
        or record.get("post_id")
        or record.get("slug")
    )

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
    )


def try_fetch_json(scraper, url: str, verbose: bool) -> Optional[Any]:
    _print_verbose(verbose, f"GET {url}")
    try:
        response = scraper.get(url, timeout=30)
        if response.status_code != 200:
            _print_verbose(verbose, f"Non-200 response: {response.status_code}")
            return None
        # Some endpoints may respond with text/html due to challenges; filter that
        content_type = response.headers.get("Content-Type", "").lower()
        if "application/json" not in content_type and not response.text.strip().startswith("[") and not response.text.strip().startswith("{"):
            _print_verbose(verbose, f"Unexpected content type: {content_type}")
            return None
        return response.json()
    except Exception as exc:
        _print_verbose(verbose, f"Request failed: {exc}")
        return None


def attempt_wp_store_locator_endpoints(base_url: str, scraper, verbose: bool) -> Optional[List[Dict[str, Any]]]:
    """
    Try a series of common WP Store Locator REST endpoints.
    """
    candidates = [
        f"{base_url.rstrip('/')}/wp-json/wp-store-locator/v1/locations?per_page=10000",
        f"{base_url.rstrip('/')}/wp-json/wpsl/v1/locations?per_page=10000",
        f"{base_url.rstrip('/')}/wp-json/wp/v2/wpsl_stores?per_page=10000",
        f"{base_url.rstrip('/')}/wp-json/wp/v2/wpsl_store?per_page=10000",
        f"{base_url.rstrip('/')}/wp-json/wpsl/v1/stores?per_page=10000",
    ]
    for url in candidates:
        data = try_fetch_json(scraper, url, verbose)
        if isinstance(data, list) and len(data) > 0:
            _print_verbose(verbose, f"Found {len(data)} stores via {url}")
            return data
        if isinstance(data, dict) and any(k in data for k in ("stores", "locations")):
            stores = data.get("stores") or data.get("locations")
            if isinstance(stores, list) and stores:
                _print_verbose(verbose, f"Found {len(stores)} stores via {url}")
                return stores
    return None


def playwright_fallback(base_url: str, verbose: bool = False) -> Optional[List[Dict[str, Any]]]:
    """
    Use Playwright (headless Chromium) to load the site, bypass Cloudflare, and
    extract a WPSL nonce or intercept requests to admin-ajax.php to obtain stores.
    Returns raw store dicts if successful.
    """
    try:
        from playwright.sync_api import sync_playwright  # type: ignore
    except Exception:
        _print_verbose(verbose, "Playwright not installed. Run: pip install playwright && playwright install chromium")
        return None

    last_ajax_response: Optional[Any] = None
    ajax_url_suffix = "/wp-admin/admin-ajax.php"

    def handle_response(response):
        nonlocal last_ajax_response
        try:
            url = response.url
            if ajax_url_suffix in url and response.status == 200:
                ct = (response.headers.get("content-type") or "").lower()
                if "json" in ct:
                    data = response.json()
                    if isinstance(data, dict):
                        stores = data.get("stores") or data.get("results") or data.get("locations")
                        if isinstance(stores, list) and stores:
                            last_ajax_response = stores
                    elif isinstance(data, list) and data:
                        last_ajax_response = data
        except Exception:
            pass

    likely_pages = [
        "",
        "/tiendas",
        "/nuestras-tiendas",
        "/encuentra-tu-tienda",
        "/tiendas-disponibles",
        "/store-locator",
        "/donde-estamos",
        "/ubicaciones",
    ]

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(locale="es-CO")
        page = context.new_page()
        page.on("response", handle_response)

        # Navigate to likely pages to trigger store loading
        for path in likely_pages:
            url = f"{base_url.rstrip('/')}{path}"
            _print_verbose(verbose, f"[browser] goto {url}")
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=60000)
                # Give scripts time to run and possibly fetch stores
                page.wait_for_timeout(4000)
                # Attempt to read nonce from JS variables
                nonce = None
                try:
                    nonce = page.evaluate("window.wpsl_locator_vars && window.wpsl_locator_vars.nonce || null")
                except Exception:
                    pass
                if not nonce:
                    try:
                        nonce = page.evaluate("window.wpslLocator && window.wpslLocator.nonce || null")
                    except Exception:
                        pass
                if nonce and not last_ajax_response:
                    # Call admin-ajax via the page context to reuse CF tokens/cookies
                    bbox = "-4.231687,-81.858139;13.527,-66.869835"
                    ajax_url = f"{base_url.rstrip('/')}{ajax_url_suffix}?action=wpsl_load_stores&nonce={nonce}&bounds={bbox}&max_results=10000&fields=id,title,address,city,state,zip,lat,lng,country,phone,hours"
                    _print_verbose(verbose, f"[browser] fetch {ajax_url}")
                    try:
                        page.evaluate(
                            "(url) => fetch(url, {credentials: 'include'}).then(r => r.json()).then(x => window.__stores = x).catch(() => null)",
                            ajax_url,
                        )
                        page.wait_for_timeout(2500)
                        data = page.evaluate("window.__stores || null")
                        if isinstance(data, dict):
                            stores = data.get("stores") or data.get("results") or data.get("locations")
                            if isinstance(stores, list) and stores:
                                last_ajax_response = stores
                        elif isinstance(data, list) and data:
                            last_ajax_response = data
                    except Exception:
                        pass

                if last_ajax_response:
                    break
            except Exception:
                continue

        browser.close()
    return last_ajax_response


def extract_wpsl_nonce(html: str) -> Optional[str]:
    """
    Extract the WP Store Locator nonce from inline JS variables.
    Common variable: wpsl_locator_vars.nonce or wpslLocator.nonce
    """
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
    """
    Attempt to load stores via WP admin-ajax action used by WP Store Locator.
    Requires discovering a nonce from a likely locator page.
    """
    likely_pages = [
        "",  # homepage might include scripts with nonce
        "/tiendas",
        "/nuestras-tiendas",
        "/encuentra-tu-tienda",
        "/tiendas-disponibles",
        "/store-locator",
        "/donde-estamos",
        "/ubicaciones",
    ]

    nonce: Optional[str] = None
    for path in likely_pages:
        url = f"{base_url.rstrip('/')}{path}"
        _print_verbose(verbose, f"Probing page for nonce: {url}")
        try:
            r = scraper.get(url, timeout=30)
            if r.status_code != 200:
                continue
            candidate = extract_wpsl_nonce(r.text)
            if candidate:
                nonce = candidate
                _print_verbose(verbose, f"Found nonce on {url}")
                break
        except Exception:
            continue

    if not nonce:
        _print_verbose(verbose, "No nonce discovered for admin-ajax.")
        return None

    ajax_url = f"{base_url.rstrip('/')}/wp-admin/admin-ajax.php"
    params = {
        "action": "wpsl_load_stores",
        "nonce": nonce,
        # Broad bounding box covering all of Colombia to return all stores
        "bounds": "-4.231687,-81.858139;13.527, -66.869835",
        "fields": "id,title,address,city,state,zip,lat,lng,country,phone,hours",
        "max_results": 10000,
    }
    _print_verbose(verbose, f"Calling admin-ajax with nonce {nonce}")
    try:
        resp = scraper.get(ajax_url, params=params, timeout=60)
        if resp.status_code != 200:
            _print_verbose(verbose, f"Admin-ajax non-200: {resp.status_code}")
            return None
        data = resp.json()
        if isinstance(data, dict):
            stores = data.get("stores") or data.get("results") or data.get("locations")
            if isinstance(stores, list) and stores:
                _print_verbose(verbose, f"Found {len(stores)} stores via admin-ajax")
                return stores
        if isinstance(data, list) and data:
            _print_verbose(verbose, f"Found {len(data)} stores via admin-ajax (list root)")
            return data
        return None
    except Exception as exc:
        _print_verbose(verbose, f"Admin-ajax failed: {exc}")
        return None


def fetch_d1_stores(base_url: str, verbose: bool = False) -> List[Store]:
    scraper = create_scraper(verbose=verbose)

    # First try known REST endpoints
    stores_raw = attempt_wp_store_locator_endpoints(base_url, scraper, verbose)
    if not stores_raw:
        # Try admin-ajax with discovered nonce
        stores_raw = attempt_admin_ajax(base_url, scraper, verbose)

    if not stores_raw:
        # Optional: Try a headless browser fallback via Playwright if installed
        try:
            browser_stores = playwright_fallback(base_url, verbose)
            if browser_stores:
                stores_raw = browser_stores
        except Exception as exc:
            _print_verbose(verbose, f"Playwright fallback failed: {exc}")

    if not stores_raw:
        raise RuntimeError(
            "Failed to retrieve stores. Site may be blocking automated access. "
            "Try re-running with --verbose, using --browser mode, or a different network."
        )

    normalized: List[Store] = []
    for rec in stores_raw:
        try:
            normalized.append(normalize_store(rec))
        except Exception:
            # Skip malformed entries
            continue
    # Deduplicate by (name, address, latitude, longitude)
    seen: set[Tuple[Optional[str], Optional[str], Optional[float], Optional[float]]] = set()
    unique: List[Store] = []
    for s in normalized:
        key = (s.name, s.address, s.latitude, s.longitude)
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
    ]
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for s in stores:
            writer.writerow(asdict(s))


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrape Tiendas D1 stores in Colombia")
    parser.add_argument(
        "--base-url",
        default="https://d1.com.co",
        help="Base URL of the D1 website",
    )
    parser.add_argument(
        "--output",
        default="d1_stores",
        help="Output file base name without extension (JSON and CSV will be created)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging",
    )
    parser.add_argument(
        "--browser",
        action="store_true",
        help="Force using a headless browser (Playwright) fallback",
    )
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    try:
        if args.browser:
            # Attempt browser mode directly
            stores_raw = playwright_fallback(args.base_url, args.verbose)
            if not stores_raw:
                raise RuntimeError("Browser mode failed to retrieve stores")
            stores = [normalize_store(r) for r in stores_raw]
        else:
            stores = fetch_d1_stores(args["base_url"] if isinstance(args, dict) else args.base_url, verbose=args.verbose)
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    json_path = f"{args.output}.json"
    csv_path = f"{args.output}.csv"
    write_json(stores, json_path)
    write_csv(stores, csv_path)
    print(f"Saved {len(stores)} stores to {json_path} and {csv_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

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


class NominatimGeocoder:
    def __init__(self, email: str = "", delay_seconds: float = 1.1, timeout_seconds: float = 20.0, max_retries: int = 3):
        self.session = requests.Session()
        ua = USER_AGENT
        if email:
            ua = f"{USER_AGENT} (contact: {email})"
        self.session.headers.update({"User-Agent": ua})
        self.delay = delay_seconds
        self.timeout = timeout_seconds
        self.max_retries = max_retries
        self._last_request_ts = 0.0
        self.cache: Dict[str, Tuple[Optional[float], Optional[float]]] = {}

    def _respect_rate_limit(self):
        sleep_for = max(0.0, self.delay - (time.time() - self._last_request_ts))
        if sleep_for > 0:
            time.sleep(sleep_for)

    def geocode_query(self, query: str, email: str = "") -> Tuple[Optional[float], Optional[float]]:
        if not query:
            return None, None
        key = query.strip().lower()
        if key in self.cache:
            return self.cache[key]

        self._respect_rate_limit()
        params = {
            "q": query,
            "format": "jsonv2",
            "limit": 1,
            "addressdetails": 0,
            "countrycodes": "co",
        }
        if email:
            params["email"] = email
        url = "https://nominatim.openstreetmap.org/search"
        for attempt in range(1, self.max_retries + 1):
            try:
                resp = self.session.get(url, params=params, timeout=self.timeout)
                self._last_request_ts = time.time()
                if resp.status_code >= 500:
                    raise requests.HTTPError(f"Server error {resp.status_code}")
                data = resp.json()
                if isinstance(data, list) and data:
                    lat = float(data[0]["lat"]) if "lat" in data[0] else None
                    lon = float(data[0]["lon"]) if "lon" in data[0] else None
                    self.cache[key] = (lat, lon)
                    return lat, lon
                self.cache[key] = (None, None)
                return None, None
            except Exception:
                if attempt >= self.max_retries:
                    self.cache[key] = (None, None)
                    return None, None
                time.sleep(min(2.0 * attempt, 5.0))

    def geocode_store(self, s: Store, email: str = "") -> Tuple[Optional[float], Optional[float]]:
        # Compose a robust query: address + city + department + country
        parts = [p for p in [s.address, s.city, s.department, "Colombia"] if p]
        query = ", ".join(parts)
        return self.geocode_query(query, email=email)


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
    ap.add_argument("--geocode-missing", action="store_true", help="Use Nominatim to geocode stores missing coordinates")
    ap.add_argument("--nominatim-email", default="", help="Contact email for Nominatim usage policy")
    ap.add_argument("--geocode-limit", type=int, default=1000, help="Max number of geocoding requests")
    ap.add_argument("--geocode-cache", default="", help="Optional JSON cache file path for geocoding results")
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

    # Optional geocoding for missing coordinates
    if args.geocode_missing and filtered:
        cache: Dict[str, Tuple[Optional[float], Optional[float]]] = {}
        if args.geocode_cache:
            try:
                with open(args.geocode_cache, "r", encoding="utf-8") as f:
                    loaded = json.load(f)
                    if isinstance(loaded, dict):
                        for k, v in loaded.items():
                            if isinstance(v, list) and len(v) == 2:
                                try:
                                    cache[k] = (float(v[0]) if v[0] is not None else None, float(v[1]) if v[1] is not None else None)
                                except Exception:
                                    pass
            except FileNotFoundError:
                pass
            except Exception:
                pass

        geocoder = NominatimGeocoder(email=args.nominatim_email)
        geocoder.cache.update(cache)

        requests_made = 0
        for s in filtered:
            if s.latitude is not None and s.longitude is not None:
                continue
            if requests_made >= args.geocode_limit:
                break
            lat, lon = geocoder.geocode_store(s, email=args.nominatim_email)
            if lat is not None and lon is not None:
                s.latitude, s.longitude = lat, lon
            requests_made += 1

        # Persist cache if requested
        if args.geocode_cache:
            try:
                with open(args.geocode_cache, "w", encoding="utf-8") as f:
                    json.dump(geocoder.cache, f, ensure_ascii=False)
            except Exception:
                pass

    write_csv(args.output, filtered)
    if args.geojson:
        write_geojson(args.geojson, filtered)
    print(f"Wrote {len(filtered)} stores to {args.output}{' and ' + args.geojson if args.geojson else ''}")


if __name__ == "__main__":
    main()

