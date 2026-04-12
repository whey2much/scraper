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

PRODUCTS_PATH = os.path.join(DATA_DIR, "updated_product_list.json")
LOCATORS_PATH = os.path.join(LOCATORS_DIR, "websitesnew.json")
OUTPUT_JSON_PATH = os.path.join(DATA_DIR, "scraped_data.json")
OUTPUT_CSV_PATH = os.path.join(DATA_DIR, "scraped_data.csv")
DEBUG_DIR = os.path.join(DATA_DIR, "debug")

DEFAULT_CURRENCY = "INR"
PAGE_LOAD_TIMEOUT = 40
WAIT_TIMEOUT = 15
MAX_RETRIES = 3
VARIANT_WAIT_AFTER_CLICK = 0.8

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(LOCATORS_DIR, exist_ok=True)
os.makedirs(DEBUG_DIR, exist_ok=True)

# ------------------------------- #
# Helpers
# ------------------------------- #

def load_json(file_path: str):
    print("\n================ DEBUG ================")
    print("Trying to load JSON from:", file_path)

    if not os.path.exists(file_path):
        print("❌ FILE DOES NOT EXIST")
        raise FileNotFoundError(file_path)

    with open(file_path, "r", encoding="utf-8") as f:
        data = f.read()

    print("\n--- FILE SIZE ---")
    print(len(data), "characters")

    print("\n--- FIRST 500 CHARS ---")
    print(data[:500])

    print("\n--- LAST 500 CHARS ---")
    print(data[-500:])

    print("\n--- RAW CONTENT START ---")
    print(data)
    print("--- RAW CONTENT END ---\n")

    try:
        parsed = json.loads(data)
        print("✅ JSON PARSED SUCCESSFULLY\n")
        return parsed
    except Exception as e:
        print("❌ JSON PARSE ERROR:", str(e))
        raise

def slugify(s: str) -> str:
    s = re.sub(r"[^\w\s-]", "", s)
    s = re.sub(r"[\s]+", "_", s.strip())
    return s[:100]

def normalize_price_text(txt: str) -> str:
    if not txt:
        return ""
    txt = txt.replace("\u00A0", " ").replace("\u202F", " ").replace("\u2009", " ")
    txt = re.sub(r"[^\d,.\s]", "", txt)
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

