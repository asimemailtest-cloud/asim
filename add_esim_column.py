#!/usr/bin/env python3
"""
Add an `esim_compatible` column to a device CSV based on model/brand.

Usage examples:
  python3 add_esim_column.py \
    --input /path/to/athena_device_data.csv \
    --output /path/to/athena_device_data_with_esim.csv \
    --model-column model \
    --brand-column brand

Optional overrides:
  --unknown-as-no                  Treat unknown models as not compatible
  --yes-pattern "regex"           Mark any row matching regex as Yes (repeatable)
  --no-pattern  "regex"           Mark any row matching regex as No  (repeatable)
  --pattern-file patterns.json     JSON with {"yes": [..], "no": [..]} regex lists

Notes:
  - Built-in rules prioritize Apple iPhone XS/XR and newer; Google Pixel 3+; Samsung Galaxy S20+/Note20/Z Fold/Flip; Motorola Razr.
  - Rules are heuristic and may vary by market/variant. Use overrides to refine.
"""

import argparse
import csv
import json
import re
import sys
from typing import Dict, List, Optional, Tuple


def load_patterns_from_file(path: str) -> Tuple[List[str], List[str]]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        yes = data.get("yes") or []
        no = data.get("no") or []
        yes_list = [str(x) for x in yes if isinstance(x, str)]
        no_list = [str(x) for x in no if isinstance(x, str)]
        return yes_list, no_list
    except FileNotFoundError:
        print(f"Pattern file not found: {path}", file=sys.stderr)
        return [], []
    except Exception as e:
        print(f"Failed to load pattern file {path}: {e}", file=sys.stderr)
        return [], []


def normalize_text(text: Optional[str]) -> str:
    if text is None:
        return ""
    # Collapse whitespace and lowercase
    t = re.sub(r"\s+", " ", str(text)).strip().lower()
    return t


def looks_like_apple(model_brand: str) -> bool:
    return any(x in model_brand for x in ["iphone", "apple ", " apple"])  # crude heuristic


def looks_like_google(model_brand: str) -> bool:
    return "pixel" in model_brand or "google " in model_brand


def looks_like_samsung(model_brand: str) -> bool:
    return "galaxy" in model_brand or "samsung" in model_brand


def looks_like_motorola(model_brand: str) -> bool:
    return "motorola" in model_brand or "moto " in model_brand or " razr" in model_brand


def apple_esim_compatible(model_brand: str) -> Optional[bool]:
    # iPhone XS / XS Max / XR and newer; SE (2020, 2022) yes; iPhone X and earlier no.
    if "iphone" not in model_brand:
        return None
    # SE handling
    if re.search(r"\biphone\s*se\b", model_brand):
        if re.search(r"\b(2020|2022|2(nd)?|3(rd)?)\b", model_brand):
            return True
        # 2016 SE -> no eSIM
        return False
    # XS / XR
    if re.search(r"\biphone\s*(xs(\s*max)?|xr)\b", model_brand):
        return True
    # Numbered generations >= 11
    m = re.search(r"\biphone\s*(\d{2})\b", model_brand)
    if m:
        try:
            num = int(m.group(1))
            if num >= 11:
                return True
            if num <= 8 or num == 10:
                return False
        except Exception:
            pass
    # iPhone 12/13/14/15 variants (Pro/Max/Plus/Mini)
    if re.search(r"\biphone\s*(11|12|13|14|15)\s*(pro|max|mini|plus)?\b", model_brand):
        return True
    # iPhone X (no eSIM)
    if re.search(r"\biphone\s*x\b", model_brand) and not re.search(r"\biphone\s*xs\b", model_brand):
        return False
    return None


def google_esim_compatible(model_brand: str) -> Optional[bool]:
    # Pixel 3 and newer (including a-series)
    if "pixel" not in model_brand:
        return None
    m = re.search(r"\bpixel\s*(\d)\b", model_brand)
    if m:
        try:
            num = int(m.group(1))
            if num >= 3:
                return True
            return False
        except Exception:
            pass
    # Fallback: assume non-numbered Pixel (like Fold) supports eSIM
    if "fold" in model_brand:
        return True
    return None


def samsung_esim_compatible(model_brand: str) -> Optional[bool]:
    if not looks_like_samsung(model_brand):
        return None
    # Galaxy S20+ and newer S-series variants
    if re.search(r"\bgalaxy\s*s(20|21|22|23|24)\b", model_brand):
        return True
    if re.search(r"\bgalaxy\s*s(20|21|22|23|24)\s*(ultra|\+|plus|fe)?\b", model_brand):
        return True
    # Note20 series
    if re.search(r"\bgalaxy\s*note\s*20\b", model_brand):
        return True
    # Z Fold / Z Flip
    if re.search(r"\bgalaxy\s*z\s*(fold|flip)\b", model_brand):
        return True
    # A-series is mixed; avoid blanket assumptions to reduce false positives
    return None


def motorola_esim_compatible(model_brand: str) -> Optional[bool]:
    # Razr foldables typically support eSIM
    if "razr" in model_brand:
        return True
    return None


def determine_esim_compatibility(brand: str, model: str, yes_regex: List[re.Pattern], no_regex: List[re.Pattern]) -> Optional[bool]:
    combined = normalize_text(f"{brand} {model}")
    # Explicit overrides first
    for rgx in yes_regex:
        if rgx.search(combined):
            return True
    for rgx in no_regex:
        if rgx.search(combined):
            return False

    # Built-in heuristics
    res = apple_esim_compatible(combined)
    if res is not None:
        return res
    res = google_esim_compatible(combined)
    if res is not None:
        return res
    res = samsung_esim_compatible(combined)
    if res is not None:
        return res
    res = motorola_esim_compatible(combined)
    if res is not None:
        return res

    return None


def find_column_key(row_keys: List[str], wanted: str) -> Optional[str]:
    wanted_norm = wanted.strip().lower()
    for k in row_keys:
        if k.strip().lower() == wanted_norm:
            return k
    # loose fallback: contains
    for k in row_keys:
        if wanted_norm in k.strip().lower():
            return k
    return None


def detect_company_name(brand: str, model: str) -> str:
    """Detect company/brand name from model and/or brand value.

    - If brand value is present, prefer it.
    - Else, infer from model using regex heuristics.
    """
    original_brand = (brand or "").strip()
    if original_brand:
        return original_brand

    text = normalize_text(model)
    patterns: List[Tuple[str, re.Pattern]] = [
        ("Apple", re.compile(r"\b(apple|iphone|ipad|ipod|watch)\b", re.IGNORECASE)),
        ("Samsung", re.compile(r"\b(samsung|galaxy|sm[-_ ]?\w+)\b", re.IGNORECASE)),
        ("Google", re.compile(r"\b(google|pixel)\b", re.IGNORECASE)),
        ("Motorola", re.compile(r"\b(motorola|moto\s|moto$|xt\d{3,4}|razr)\b", re.IGNORECASE)),
        ("OnePlus", re.compile(r"\b(oneplus)\b", re.IGNORECASE)),
        ("Xiaomi", re.compile(r"\b(xiaomi|mi\s|mi-|redmi|poco)\b", re.IGNORECASE)),
        ("OPPO", re.compile(r"\b(oppo|cph\d{3,5})\b", re.IGNORECASE)),
        ("realme", re.compile(r"\b(realme|rmx\d{3,5})\b", re.IGNORECASE)),
        ("vivo", re.compile(r"\b(vivo)\b", re.IGNORECASE)),
        ("HUAWEI", re.compile(r"\b(huawei|mate\s|p\d{2}\b)\b", re.IGNORECASE)),
        ("Honor", re.compile(r"\b(honor)\b", re.IGNORECASE)),
        ("Nokia", re.compile(r"\b(nokia)\b", re.IGNORECASE)),
        ("Sony", re.compile(r"\b(sony|xperia)\b", re.IGNORECASE)),
        ("LG", re.compile(r"\b(lg|lm-\w+)\b", re.IGNORECASE)),
        ("ASUS", re.compile(r"\b(asus|zenfone|rog\s*phone)\b", re.IGNORECASE)),
        ("Lenovo", re.compile(r"\b(lenovo)\b", re.IGNORECASE)),
        ("ZTE", re.compile(r"\b(zte|nubia)\b", re.IGNORECASE)),
        ("TCL", re.compile(r"\b(tcl)\b", re.IGNORECASE)),
        ("Alcatel", re.compile(r"\b(alcatel)\b", re.IGNORECASE)),
        ("Nothing", re.compile(r"\b(nothing\s*phone)\b", re.IGNORECASE)),
        ("Fairphone", re.compile(r"\b(fairphone)\b", re.IGNORECASE)),
        ("Palm", re.compile(r"\b(palm)\b", re.IGNORECASE)),
        ("CAT", re.compile(r"\b(cat(\s*phone)?|caterpillar)\b", re.IGNORECASE)),
        ("Infinix", re.compile(r"\b(infinix)\b", re.IGNORECASE)),
        ("TECNO", re.compile(r"\b(tecno)\b", re.IGNORECASE)),
        ("Wiko", re.compile(r"\b(wiko)\b", re.IGNORECASE)),
        ("Meizu", re.compile(r"\b(meizu)\b", re.IGNORECASE)),
        ("BlackBerry", re.compile(r"\b(blackberry)\b", re.IGNORECASE)),
        ("Blackview", re.compile(r"\b(blackview)\b", re.IGNORECASE)),
        ("BLU", re.compile(r"\b(\bblu\b)\b", re.IGNORECASE)),
    ]
    for brand_name, rgx in patterns:
        if rgx.search(text):
            return brand_name
    return ""


def main():
    ap = argparse.ArgumentParser(description="Add eSIM compatibility column to CSV based on model/brand")
    ap.add_argument("--input", required=True, help="Input CSV path")
    ap.add_argument("--output", required=True, help="Output CSV path")
    ap.add_argument("--model-column", default="model", help="CSV column name for device model (case-insensitive)")
    ap.add_argument("--brand-column", default="", help="Optional CSV column for brand/OEM (case-insensitive)")
    ap.add_argument("--unknown-as-no", action="store_true", help="Treat unknown models as not compatible (default: leave as Unknown)")
    ap.add_argument("--yes-pattern", action="append", default=[], help="Regex to force Yes (repeatable)")
    ap.add_argument("--no-pattern", action="append", default=[], help="Regex to force No (repeatable)")
    ap.add_argument("--pattern-file", default="", help="JSON file with {yes:[], no:[]} regex lists")
    ap.add_argument("--column-name", default="esim_compatible", help="Name of the output column to add")
    ap.add_argument("--company-column", default="company", help="Name of the output brand/company column to add")
    args = ap.parse_args()

    yes_patterns: List[str] = list(args.yes_pattern or [])
    no_patterns: List[str] = list(args.no_pattern or [])
    if args.pattern_file:
        y, n = load_patterns_from_file(args.pattern_file)
        yes_patterns.extend(y)
        no_patterns.extend(n)

    yes_regex = []
    no_regex = []
    try:
        yes_regex = [re.compile(p, re.IGNORECASE) for p in yes_patterns]
    except re.error as e:
        print(f"Invalid yes-pattern regex: {e}", file=sys.stderr)
        sys.exit(2)
    try:
        no_regex = [re.compile(p, re.IGNORECASE) for p in no_patterns]
    except re.error as e:
        print(f"Invalid no-pattern regex: {e}", file=sys.stderr)
        sys.exit(2)

    # Stream CSV
    total = 0
    yes_count = 0
    no_count = 0
    unknown_count = 0

    with open(args.input, "r", encoding="utf-8", newline="") as f_in, open(args.output, "w", encoding="utf-8", newline="") as f_out:
        reader = csv.DictReader(f_in)
        fieldnames = list(reader.fieldnames or [])
        if not fieldnames:
            print("Input CSV appears to have no header.", file=sys.stderr)
            sys.exit(1)

        model_key = find_column_key(fieldnames, args.model_column)
        if not model_key:
            print(f"Model column not found (looked for: {args.model_column}). Available: {', '.join(fieldnames)}", file=sys.stderr)
            sys.exit(1)
        brand_key = find_column_key(fieldnames, args.brand_column) if args.brand_column else None

        out_fieldnames = list(fieldnames)
        if args.column_name not in out_fieldnames:
            out_fieldnames.append(args.column_name)
        if args.company_column not in out_fieldnames:
            out_fieldnames.append(args.company_column)
        writer = csv.DictWriter(f_out, fieldnames=out_fieldnames)
        writer.writeheader()

        for row in reader:
            total += 1
            brand_val = row.get(brand_key) if brand_key else ""
            model_val = row.get(model_key) or ""

            decision = determine_esim_compatibility(str(brand_val), str(model_val), yes_regex, no_regex)
            if decision is True:
                row[args.column_name] = "Yes"
                yes_count += 1
            elif decision is False:
                row[args.column_name] = "No"
                no_count += 1
            else:
                if args.unknown_as_no:
                    row[args.column_name] = "No"
                    no_count += 1
                else:
                    row[args.column_name] = "Unknown"
                    unknown_count += 1

            # Company/brand detection
            detected = detect_company_name(str(brand_val), str(model_val))
            row[args.company_column] = detected

            writer.writerow(row)

    print(f"Processed {total} rows -> Yes: {yes_count}, No: {no_count}, Unknown: {unknown_count}")


if __name__ == "__main__":
    main()

