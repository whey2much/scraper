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

BATCH_SIZE = 30  # Push in chunks to avoid timeouts


def load_json(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def build_payload(scraped: dict, products: dict, metadata: dict) -> list:
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

            if status == "no_url":
                continue
            if status.startswith("error:"):
                continue
            if price_value is None:
                continue

            affiliate_url = product_urls.get(site_name, "").strip() or None
            product_url   = scraped_url.strip()

            if not product_url and not affiliate_url:
                continue

            in_stock = (status == "ok")
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


def push_batch(batch: list, api_url: str, api_key: str) -> dict:
    body = json.dumps(batch).encode("utf-8")
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
        with urlopen(req, timeout=60) as resp:
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

    total     = len(payload)
    batches   = [payload[i:i + BATCH_SIZE] for i in range(0, total, BATCH_SIZE)]
    print(f"Pushing {total} price entries in {len(batches)} batches of {BATCH_SIZE} to {api_url} ...")

    successful = 0
    failed     = 0
    all_errors = []

    for i, batch in enumerate(batches, 1):
        print(f"  Batch {i}/{len(batches)} ({len(batch)} entries)...", end=" ", flush=True)
        try:
            result = push_batch(batch, api_url, api_key)
            batch_ok  = result.get("successful", 0)
            batch_fail = result.get("failed", 0)
            successful += batch_ok
            failed     += batch_fail
            print(f"ok={batch_ok} fail={batch_fail}")
            if result.get("errors"):
                all_errors.extend(result["errors"])
        except RuntimeError as e:
            print(f"FAILED — {e}")
            failed += len(batch)

    print(f"\nDone. total={total} successful={successful} failed={failed}")
    if all_errors:
        print("Errors:")
        for err in all_errors:
            print(f"   - {err}")

    if failed > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
