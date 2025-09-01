Tiendas D1 Stores Scraper (Colombia)
====================================

Quick script to export Tiendas D1 store locations to JSON and CSV.

Install
-------

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
```

Usage
-----

Overpass (OSM) approach (works reliably, may be incomplete vs official):

```bash
python d1_overpass.py --output d1_osm
```

Use Google Maps Geocoding to fill or override lat/lng:

```bash
# Export GOOGLE_API_KEY or pass --google-api-key
export GOOGLE_API_KEY=YOUR_KEY
python d1_overpass.py --output d1_osm --force-google --google-rate 0.1 --cache /workspace/d1_geocode_cache.json
```
Fields include a `source` column indicating `osm` or `google` for coordinates.

Website scraper (may be blocked by protections):

```bash
python d1_scraper.py --output d1_stores --base-url https://d1.com.co --verbose
```

Website scraper with headless browser:

```bash
~/.local/bin/playwright install --with-deps chromium
python d1_scraper.py --output d1_stores --base-url https://d1.com.co --browser --verbose
```

If you can open the site in your browser and pass the challenge, copy your Cookie header from the Network tab and pass it to the scraper to reuse your session:

```bash
python d1_scraper.py --output d1_stores --base-url https://d1.com.co --cookie "<paste your Cookie header>" --verbose
```

Outputs:
- `d1_stores.json`
- `d1_stores.csv`

Notes
-----
- The site is protected by Cloudflare. The script uses `cloudscraper` to mimic a real browser.
- If requests are blocked, wait a few minutes and try again, or use a different network. You can also pass your `--cookie` from a real browser session.
- Only for lawful and approved use. Respect the website's terms.

