# D1 Stores Scraper (Colombia)

Scrapes Tiendas D1 locations in Colombia using OpenStreetMap's Overpass API, outputs CSV and GeoJSON.

## Requirements
- Python 3.10+ (no external dependencies)

## Usage
```bash
python3 scrape_d1_overpass.py --outdir ./output
```
Outputs:
- `output/d1_stores_colombia.csv`
- `output/d1_stores_colombia.geojson`

## Notes
- Uses Overpass mirrors and exponential backoff to handle rate limits.
- Results depend on OSM data completeness; not official D1 data.