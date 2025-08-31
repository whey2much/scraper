# scrape.py
import json
import pandas as pd
import re
import os
import sys
from dataclasses import dataclass
from time import sleep
from typing import Optional, List, Dict, Tuple

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# ------------------------------- #
# Config & Paths
# ------------------------------- #

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
LOCATORS_DIR = os.path.join(BASE_DIR, "locators")

PRODUCTS_PATH = os.path.join(DATA_DIR, "products.json")
LOCATORS_PATH = os.path.join(LOCATORS_DIR, "websites.json")
OUTPUT_JSON_PATH = os.path.join(DATA_DIR, "scraped_data.json")
OUTPUT_CSV_PATH = os.path.join(DATA_DIR, "scraped_data.csv")
DEBUG_DIR = os.path.join(DATA_DIR, "debug")

DEFAULT_CURRENCY = "INR"
PAGE_LOAD_TIMEOUT = 45
WAIT_TIMEOUT = 20
MAX_RETRIES = 3
VARIANT_WAIT_AFTER_CLICK = 0.8

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(LOCATORS_DIR, exist_ok=True)
os.makedirs(DEBUG_DIR, exist_ok=True)

# ------------------------------- #
# Helpers
# ------------------------------- #

def load_json(file_path: str):
    with open(file_path, "r", encoding="utf-8") as f:
        return json.load(f)

def slugify(s: str) -> str:
    s = re.sub(r"[^\w\s-]", "", s)
    s = re.sub(r"[\s]+", "_", s.strip())
    return s[:100]

def normalize_price_text(txt: str) -> str:
    if not txt:
        return ""
    txt = txt.replace("\u00A0", " ").replace("\u202F", " ").replace("\u2009", " ")
    txt = re.sub(r"[^\d,.\s]", "", txt)  # strip currency symbols & letters
    txt = re.sub(r"\s+", "", txt)
    return txt

def parse_price(txt: str) -> Optional[float]:
    if not txt:
        return None
    t = normalize_price_text(txt)
    if not t:
        return None

    if "," in t and "." in t:
        last_sep = max(t.rfind(","), t.rfind("."))
        integer = re.sub(r"[.,]", "", t[:last_sep])
        decimal = t[last_sep + 1:]
        t = f"{integer}.{decimal}" if decimal.isdigit() else integer
    else:
        if "," in t:
            if re.search(r",\d{2}$", t):
                t = t.replace(".", "").replace(",", ".")
            else:
                t = t.replace(",", "")

    try:
        return float(t)
    except ValueError:
        return None

# ---- JSON-LD / meta / page-source fallbacks ---- #

def find_price_in_obj(obj):
    if isinstance(obj, dict):
        if (obj.get("@type") == "Offer" or "offers" in obj) and "price" in obj:
            p = parse_price(str(obj.get("price", "")))
            if p is not None:
                return p, str(obj.get("price"))
        if "price" in obj:
            p = parse_price(str(obj.get("price", "")))
            if p is not None:
                return p, str(obj.get("price"))
        for v in obj.values():
            got = find_price_in_obj(v)
            if got:
                return got
    elif isinstance(obj, list):
        for it in obj:
            got = find_price_in_obj(it)
            if got:
                return got
    return None

def get_price_from_jsonld(driver) -> Tuple[Optional[float], Optional[str]]:
    try:
        scripts = driver.find_elements(By.CSS_SELECTOR, "script[type='application/ld+json']")
        for s in scripts:
            raw = s.get_attribute("innerText") or ""
            if not raw.strip():
                continue
            try:
                data = json.loads(raw)
            except Exception:
                m = re.search(r"\{.*\}", raw, flags=re.S)
                if not m:
                    continue
                try:
                    data = json.loads(m.group(0))
                except Exception:
                    continue
            got = find_price_in_obj(data)
            if got:
                return got
    except Exception:
        pass
    return None, None

def get_price_from_meta(driver) -> Tuple[Optional[float], Optional[str]]:
    sels = [
        "meta[itemprop='price']",
        "meta[property='product:price:amount']",
        "meta[name='twitter:data1']"
    ]
    for sel in sels:
        try:
            for el in driver.find_elements(By.CSS_SELECTOR, sel):
                val = el.get_attribute("content") or el.get_attribute("value") or el.get_attribute("data-price") or ""
                p = parse_price(val)
                if p is not None:
                    return p, val
        except Exception:
            continue
    return None, None

def get_price_from_pagesource(driver) -> Tuple[Optional[float], Optional[str]]:
    html = driver.page_source or ""
    m = re.search(r"(selling|offer|final|deal)[^₹]{0,80}₹\s*([\d,]+(?:\.\d{1,2})?)", html, flags=re.I | re.S)
    if not m:
        m = re.search(r"₹\s*([\d,]+(?:\.\d{1,2})?)", html)
    if m:
        num = m.group(1)
        p = parse_price(num)
        if p and p > 100:
            return p, f"₹{num}"
    return None, None

# ---- Variant selection ---- #

def click_variant_if_found(driver, *needles: str) -> None:
    needles = [n for n in needles if n]
    if not needles:
        return
    lower_needles = [n.lower() for n in needles]
    xpath = "//*[@role='button' or self::button or self::a or self::span or self::div]"
    try:
        elems = WebDriverWait(driver, 5).until(EC.presence_of_all_elements_located((By.XPATH, xpath)))
    except Exception:
        elems = []
    tried = 0
    for el in elems:
        if tried >= 5:
            break
        try:
            txt = (el.text or el.get_attribute("innerText") or "").strip().lower()
            if any(n in txt for n in lower_needles):
                driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
                el.click()
                tried += 1
                sleep(VARIANT_WAIT_AFTER_CLICK)
        except Exception:
            continue

def extract_variant_needles(product_name: str) -> List[str]:
    needles: List[str] = []
    if re.search(r"\b2\s*kg\b", product_name, re.I):
        needles += ["2 kg", "2kg", "4.4 lb", "4.4lb"]
    if re.search(r"\b1\s*kg\b", product_name, re.I):
        needles += ["1 kg", "1kg", "2.2 lb", "2.2lb"]
    if re.search(r"\b5\s*lb\b", product_name, re.I):
        needles += ["5 lb", "5lb", "2.27 kg", "2.27kg"]
    parts = [p.strip() for p in product_name.split("|")]
    for token in reversed(parts):
        if any(k in token.lower() for k in ["choc", "van", "cookie", "straw", "hazel", "mango", "coffee"]):
            needles.append(token.strip())
            break
    return needles

# ------------------------------- #
# Scraper Core
# ------------------------------- #

LOCATOR_MAP = {"xpath": By.XPATH, "css": By.CSS_SELECTOR}

@dataclass
class SiteLocator:
    website_name: str
    locators: List[Dict]
    wait_visible: bool = True

class WebsiteScraper:
    def __init__(self, site: SiteLocator):
        self.site = site

    def _get_element_text(self, elem) -> str:
        txt = elem.text or ""
        if not txt:
            txt = elem.get_attribute("textContent") or elem.get_attribute("innerText") or ""
        return txt.strip()

    def get_price(self, driver, timeout=WAIT_TIMEOUT) -> Tuple[Optional[float], Optional[str]]:
        wait = WebDriverWait(driver, timeout)
        for loc in self.site.locators:
            by = LOCATOR_MAP.get((loc.get("locator_type") or "").lower())
            val = loc.get("locator_value") or ""
            if not (by and val):
                continue
            try:
                elem = wait.until(
                    EC.visibility_of_element_located((by, val))
                    if self.site.wait_visible
                    else EC.presence_of_element_located((by, val))
                )
                raw = self._get_element_text(elem)
                price = parse_price(raw)
                if price is not None:
                    return price, raw
            except Exception:
                continue

        p, raw = get_price_from_jsonld(driver)
        if p is not None:
            return p, raw
        p, raw = get_price_from_meta(driver)
        if p is not None:
            return p, raw
        p, raw = get_price_from_pagesource(driver)
        if p is not None:
            return p, raw
        return None, None

# ------------------------------- #
# Driver & schema
# ------------------------------- #

def build_driver() -> webdriver.Chrome:
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--log-level=3")
    options.add_argument("--window-size=1366,768")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    options.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36"
    )
    options.page_load_strategy = "eager"
    options.add_experimental_option("prefs", {"profile.managed_default_content_settings.images": 2})

    chrome_bin = os.environ.get("CHROME_BIN")
    if chrome_bin:
        options.binary_location = chrome_bin

    # Selenium Manager auto-installs a matching driver:
    driver = webdriver.Chrome(options=options)
    driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
    return driver

def ensure_locators_schema(site_cfg: dict) -> List[Dict]:
    if isinstance(site_cfg.get("locators"), list):
        return site_cfg["locators"]
    if site_cfg.get("locator_type") and site_cfg.get("locator_value"):
        return [{"locator_type": site_cfg["locator_type"], "locator_value": site_cfg["locator_value"]}]
    return []

# ------------------------------- #
# Main Scraping Function
# ------------------------------- #

def scrape_all():
    products = load_json(PRODUCTS_PATH)
    locator_cfg = load_json(LOCATORS_PATH)

    results: Dict[str, Dict] = {}
    driver = build_driver()

    try:
        for product_name, websites in products.items():
            print(f"\n[INFO] Scraping product: {product_name}")
            product_data: Dict[str, Dict] = {}

            for website_name, site_cfg in locator_cfg.items():
                print(f"    [INFO] -> {website_name}")
                product_url = (websites.get(website_name) or "").strip()

                if not product_url:
                    product_data[website_name] = {
                        "status": "no_url",
                        "currency": DEFAULT_CURRENCY,
                        "price_value": None,
                        "price_display": "Information Unavailable",
                        "raw": "",
                        "link": ""
                    }
                    continue

                locs = ensure_locators_schema(site_cfg)
                scraper = WebsiteScraper(SiteLocator(website_name, locs))

                status = "ok"
                price_value: Optional[float] = None
                raw_text: Optional[str] = None

                for attempt in range(1, MAX_RETRIES + 1):
                    try:
                        driver.get(product_url)

                        if website_name in ("Flipkart", "MuscleBlaze"):
                            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
                            needles = extract_variant_needles(product_name)
                            click_variant_if_found(driver, *needles)
                            try:
                                WebDriverWait(driver, 8).until(lambda d: "₹" in (d.page_source or ""))
                            except Exception:
                                pass

                        # Fast path: JSON-LD / meta first, then DOM, then page-source
                        p, raw = get_price_from_jsonld(driver)
                        if p is None:
                            p, raw = get_price_from_meta(driver)
                        if p is None:
                            p2, raw2 = scraper.get_price(driver, timeout=WAIT_TIMEOUT)
                            p, raw = (p2, raw2) if p2 is not None else (None, raw)
                        if p is None:
                            p3, raw3 = get_price_from_pagesource(driver)
                            p, raw = (p3, raw3) if p3 is not None else (None, raw)

                        price_value, raw_text = p, raw
                        status = "ok" if price_value is not None else "price_not_found"
                        break
                    except Exception as e:
                        status = f"error:{type(e).__name__}"
                        sleep(1.5 * attempt)
                else:
                    try:
                        fn_base = f"{slugify(product_name)}__{slugify(website_name)}"
                        driver.save_screenshot(os.path.join(DEBUG_DIR, f"{fn_base}.png"))
                        with open(os.path.join(DEBUG_DIR, f"{fn_base}.html"), "w", encoding="utf-8") as fp:
                            fp.write(driver.page_source)
                    except Exception:
                        pass

                price_display = f"₹{price_value:.2f}" if price_value is not None else "Price Not Available"

                product_data[website_name] = {
                    "status": status,
                    "currency": DEFAULT_CURRENCY,
                    "price_value": price_value,
                    "price_display": price_display,
                    "raw": raw_text,
                    "link": product_url
                }

            results[product_name] = product_data

    finally:
        driver.quit()

    with open(OUTPUT_JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)

    flat_rows = []
    for product, sites in results.items():
        for site, info in sites.items():
            flat_rows.append({
                "Product": product,
                "Website": site,
                "Status": info.get("status"),
                "Currency": info.get("currency"),
                "Price": info.get("price_value"),
                "Link": info.get("link"),
                "Raw": info.get("raw"),
            })
    pd.DataFrame(flat_rows).to_csv(OUTPUT_CSV_PATH, index=False)

    print("\nScraping completed. Output saved to:")
    print(f"- {OUTPUT_JSON_PATH}")
    print(f"- {OUTPUT_CSV_PATH}")
    print(f"- Debug artifacts (if any): {DEBUG_DIR}")

# ------------------------------- #
# Entry Point
# ------------------------------- #

if __name__ == "__main__":
    if len(sys.argv) == 3:
        test_site = sys.argv[1]
        test_url = sys.argv[2]
        test_products = {"Ad-hoc Test": {test_site: test_url}}
        with open(PRODUCTS_PATH, "w", encoding="utf-8") as f:
            json.dump(test_products, f, indent=2)
    scrape_all()
