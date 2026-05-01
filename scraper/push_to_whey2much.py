"""
push_to_whey2much.py

Reads data/scraped_data.json (produced by scrape.py) and pushes prices
to the Whey2Much ingest API at POST /api/ingest/prices.

Environment variables required:
  WHEY2MUCH_API_URL   e.g. https://whey2much.in
  WHEY2MUCH_API_KEY   the INGEST_API_KEY value set in Vercel
"""

import json
import os
import sys
from datetime import datetime, timezone
from urllib.request import urlopen, Request
from urllib.error import HTTPError, URLError

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")

SCRAPED_DATA_PATH   = os.path.join(DATA_DIR, "scraped_data.json")
PRODUCTS_PATH       = os.path.join(DATA_DIR, "products.json")
METADATA_PATH       = os.path.join(DATA_DIR, "product_metadata.json")


def load_json(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def build_payload(scraped: dict, products: dict, metadata: dict) -> list:
    """
    Transform scraper output into the Whey2Much ingest API payload format.

    Skips items where:
      - status is "no_url"         (product has no URL for that site)
      - status starts with "error:" (scraping failed; don't overwrite good data)
      - price_value is None        (no price could be found)
    """
    now_iso = datetime.now(timezone.utc).isoformat()
    payload = []

    for product_key, product_data in scraped.items():
        product_name = product_data.get("product_name", "")
        sites        = product_data.get("sites", {})

        meta         = metadata.get(product_key, {})
        brand        = meta.get("brand", "Unknown")
        category     = meta.get("category", "Supplement")
        sub_category = meta.get("sub_category", None)
        image_url    = meta.get("image_url", None)

        product_urls = {}
        if product_key in products:
            product_urls = products[product_key].get("websites", {})

        for site_name, info in sites.items():
            status      = info.get("status", "")
            price_value = info.get("price_value")
            currency    = info.get("currency", "INR")
            scraped_url = info.get("link", "")

            # Skip — no URL configured for this site
            if status == "no_url":
                continue

            # Skip — scraping error (network/timeout); don't overwrite good data
            if status.startswith("error:"):
                continue

            # Skip — no valid price found
            if price_value is None:
                continue

            affiliate_url = product_urls.get(site_name, "").strip() or None
            product_url   = scraped_url.strip()

            if not product_url and not affiliate_url:
                continue

            # out_of_stock status still gets pushed so we can mark it correctly
            in_stock = (status == "ok")

            # Pass through original_price (MRP) if the scraper captured it
            original_price = info.get("original_price")

            payload.append({
                "product_key":    product_key,
                "product_name":   product_name,
                "brand":          brand,
                "category":       category,
                "sub_category":   sub_category,
                "site_name":      site_name,
                "price":          price_value,
                "original_price": original_price,
                "currency":       currency,
                "product_url":    product_url or affiliate_url,
                "affiliate_url":  affiliate_url,
                "image_url":      image_url,
                "in_stock":       in_stock,
                "last_seen":      now_iso,
            })

    return payload


def push(payload: list, api_url: str, api_key: str) -> dict:
    body = json.dumps(payload).encode("utf-8")
    req  = Request(
        f"{api_url.rstrip('/')}/api/ingest/prices",
        data=body,
        headers={
            "Content-Type": "application/json",
            "x-api-key":    api_key,
        },
        method="POST",
    )
    try:
        with urlopen(req, timeout=120) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except HTTPError as e:
        body_text = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {e.code}: {body_text}") from e
    except URLError as e:
        raise RuntimeError(f"Network error: {e.reason}") from e


def main():
    api_url = os.environ.get("WHEY2MUCH_API_URL", "").strip()
    api_key = os.environ.get("WHEY2MUCH_API_KEY", "").strip()

    if not api_url or not api_key:
        print("Missing WHEY2MUCH_API_URL or WHEY2MUCH_API_KEY environment variables.", file=sys.stderr)
        sys.exit(1)

    if not os.path.exists(SCRAPED_DATA_PATH):
        print(f"{SCRAPED_DATA_PATH} not found. Run scrape.py first.", file=sys.stderr)
        sys.exit(1)

    scraped  = load_json(SCRAPED_DATA_PATH)
    products = load_json(PRODUCTS_PATH) if os.path.exists(PRODUCTS_PATH) else {}
    metadata = load_json(METADATA_PATH) if os.path.exists(METADATA_PATH) else {}

    payload = build_payload(scraped, products, metadata)

    if not payload:
        print("No valid price entries to push. Check scraped_data.json.")
        sys.exit(0)

    print(f"Pushing {len(payload)} price entries to {api_url} ...")

    try:
        result = push(payload, api_url, api_key)
    except RuntimeError as e:
        print(f"Push failed: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"Done. total={result.get('total')} successful={result.get('successful')} failed={result.get('failed')}")
    if result.get("errors"):
        print("Errors:")
        for err in result["errors"]:
            print(f"   - {err}")

    if result.get("failed", 0) > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
