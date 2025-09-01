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

```bash
python d1_scraper.py --output d1_stores --base-url https://d1.com.co --verbose
```

Outputs:
- `d1_stores.json`
- `d1_stores.csv`

Notes
-----
- The site is protected by Cloudflare. The script uses `cloudscraper` to mimic a real browser.
- If requests are blocked, wait a few minutes and try again, or use a different network.
- Only for lawful and approved use. Respect the website's terms.

