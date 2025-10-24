#!/usr/bin/env python3
import time
import random
import re
import requests
import pandas as pd
import openpyxl
import datetime
from itertools import cycle
from collections import defaultdict
from playwright.sync_api import sync_playwright
from difflib import SequenceMatcher

# ---------------- Configuration ----------------
SEARCH_QUERY = "symprove"
BASE_URL = "https://www.amazon.co.uk"
SEARCH_URL = f"{BASE_URL}/s?k={SEARCH_QUERY}&i=drugstore&rh=p_89:Symprove"
MAX_PAGES = 3
HEADLESS_MODE = True
POSTCODE_UK = "SW1A1AA"

PROXIES = [None]
proxy_pool = cycle(PROXIES)

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0",
]

# ---------------- Utilities ----------------
def safe_action(action, retries=3, delay=3, label=""):
    for attempt in range(1, retries + 1):
        try:
            return action()
        except Exception as e:
            print(f"⚠️ Attempt {attempt} failed for {label}: {e}")
            time.sleep(delay * attempt)
    return None

def parse_price_str(price_str):
    if price_str is None:
        return None
    try:
        s = str(price_str)
        # Remove non-numeric except dot and comma, then standardize comma removal
        m = re.search(r'(\d{1,3}(?:[,\d]*)(?:\.\d+)?)', s.replace(',', ''))
        if not m:
            return None
        return float(m.group(1))
    except Exception:
        return None

def similar(a, b):
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()

# ---------------- Force UK postcode ----------------
def force_uk_postcode(context, postcode=POSTCODE_UK):
    try:
        # set typical cookies used by Amazon UK site to reflect delivery postcode/language
        cookies = [
            {"name": "lc-main", "value": "en_GB", "domain": ".amazon.co.uk", "path": "/"},
            {"name": "glow-destination-postcode", "value": postcode, "domain": ".amazon.co.uk", "path": "/"},
            # sometimes setting the "sp" cookie helps presentation in right rail
            {"name": "sp-cc-gp", "value": "1", "domain": ".amazon.co.uk", "path": "/"},
        ]
        context.add_cookies(cookies)
        return True
    except Exception as e:
        print(f"⚠️ Cookie injection failed: {e}")
        return False

# ---------------- Identity rotation ----------------
def rotate_identity(playwright):
    proxy = next(proxy_pool)
    ua = random.choice(USER_AGENTS)
    print(f"🔁 Rotating identity → Proxy: {proxy or 'DIRECT'}, UA: {ua[:60]}...")
    browser = playwright.chromium.launch(headless=HEADLESS_MODE, slow_mo=0)
    context = browser.new_context(
        user_agent=ua,
        locale="en-GB",
        proxy={"server": proxy} if proxy else None,
        extra_http_headers={"Accept-Language": "en-GB,en;q=0.9"}
    )
    page = context.new_page()
    return browser, context, page

# ---------------- Price extraction (robust) ----------------
def extract_price(page):
    """
    Extracts the Amazon price from the product page.
    Prioritizes the visible 'One-time purchase' price & main displayed price slots.
    Returns the full string (e.g., '£18.85') for later parsing.
    """
    # Helper to try selector and return inner_text if present and contains a currency symbol
    def try_selector(sel):
        try:
            el = page.query_selector(sel)
            if el:
                txt = el.inner_text().strip()
                if txt and re.search(r'£\s*\d', txt):
                    # return the first currency-like match
                    m = re.search(r'£\s*\d{1,3}(?:[,\d]*)(?:\.\d+)?', txt)
                    return m.group(0) if m else txt
        except Exception:
            pass
        return None

    # 1) Check explicit "One-time purchase" block (right rail). This often contains the shown single-buy price
    try:
        # Search for price near "One-time purchase" text
        one_time_xpath = "//text()[contains(normalize-space(.), 'One-time purchase') or contains(normalize-space(.), 'one-time purchase')]/ancestor::*[1]"
        el = page.query_selector(one_time_xpath)
        if el:
            # try to find a nearby span with class a-offscreen or priceblock inside that block
            candidates = [
                ".//span[contains(@class,'a-offscreen')]",
                ".//span[contains(@id,'price') or contains(@class,'a-color-price') or contains(@class,'a-price')]",
                ".//span[contains(@class,'price')]", 
            ]
            for cand in candidates:
                try:
                    found = el.query_selector(cand)
                    if found:
                        txt = found.inner_text().strip()
                        m = re.search(r'£\s*\d{1,3}(?:[,\d]*)(?:\.\d+)?', txt)
                        if m:
                            return m.group(0)
                except Exception:
                    continue
    except Exception:
        pass

    # 2) Check common price selectors (desktop and inside buy box)
    selectors = [
        "span.a-price > span.a-offscreen",
        "span#priceblock_ourprice",            # legacy
        "span#priceblock_dealprice",           # deals
        "span#price_inside_buybox",
        "#corePrice_feature_div span.a-price > span.a-offscreen",
        "#corePrice_desktop span.a-price > span.a-offscreen",
        "div#corePrice_feature_div span.a-offscreen",
        "div#corePrice_feature_div .a-price-whole", 
        "div#price span.a-offscreen",
        "span.a-color-price",
        "div.a-section.a-spacing-none span.a-price > span.a-offscreen",
        "div.a-section.a-spacing-small .a-price > span.a-offscreen",
    ]
    for sel in selectors:
        val = try_selector(sel)
        if val:
            return val

    # 3) If product page shows 'See buying options' link, return that
    try:
        see_options = page.query_selector("a[href*='/gp/offer-listing']") or page.query_selector("span.a-size-medium.a-color-price")
        if see_options:
            txt = see_options.inner_text().strip()
            if 'See buying options' in txt or 'See buying options' in (txt or ""):
                return "See Buying Options"
    except Exception:
        pass

    # 4) Fallback: try to fetch any £ value on page (but prefer the one with surrounding '£' sign)
    try:
        body = page.content()
        m = re.search(r'£\s*\d{1,3}(?:[,\d]*)(?:\.\d+)?', body)
        if m:
            return m.group(0)
    except Exception:
        pass

    return "N/A"

# ---------------- Amazon scraping ----------------
def extract_amazon_products(playwright):
    scraped = []
    failed_asins = []
    browser, context, page = rotate_identity(playwright)

    try:
        print("🔎 Opening Amazon UK homepage and forcing UK postcode...")
        safe_action(lambda: page.goto("https://www.amazon.co.uk", timeout=60000), label="open amazon")
        time.sleep(2)
        force_uk_postcode(context, POSTCODE_UK)
        time.sleep(1)

        print(f"🔍 Loading search page: {SEARCH_URL}")
        safe_action(lambda: page.goto(SEARCH_URL, timeout=60000), label="open search")
        safe_action(lambda: page.wait_for_selector("div.s-main-slot", timeout=30000), label="wait search slot")
        time.sleep(random.uniform(1.5, 2.5))

        asin_elements = page.query_selector_all("div[data-asin]")
        asins = list({el.get_attribute("data-asin") for el in asin_elements if el.get_attribute("data-asin")})
        print(f"🔹 Found {len(asins)} ASINs on search page")

        for asin in asins:
            if not asin or len(asin.strip()) < 5:
                continue
            product_url = f"{BASE_URL}/dp/{asin}"
            print(f"\n➡️ Scraping ASIN {asin} -> {product_url}")
            try:
                ua2 = random.choice(USER_AGENTS)
                context2 = browser.new_context(user_agent=ua2, locale="en-GB",
                                               extra_http_headers={"Accept-Language": "en-GB,en;q=0.9"})
                force_uk_postcode(context2, POSTCODE_UK)
                page2 = context2.new_page()
                safe_action(lambda: page2.goto(product_url, timeout=60000), label=f"goto {asin}")
                # wait up to 20s for typical price nodes to appear (if they exist)
                try:
                    page2.wait_for_selector("span.a-price > span.a-offscreen", timeout=10000)
                except Exception:
                    # not fatal, continue and let extract_price handle the rest
                    pass
                safe_action(lambda: page2.wait_for_load_state("domcontentloaded"), label="domcontentloaded")
                time.sleep(random.uniform(1.0, 2.0))

                brand = "N/A"
                for selector in ["#bylineInfo", "a#brand", "tr.po-brand td:nth-child(2)", "th:has-text('Brand') + td", "a[href*='/stores/']"]:
                    try:
                        el = page2.query_selector(selector)
                        if el:
                            brand = el.inner_text().strip()
                            break
                    except Exception:
                        continue

                title_el = page2.query_selector("#productTitle")
                title = title_el.inner_text().strip() if title_el else ""
                price_raw = extract_price(page2)
                price = parse_price_str(price_raw)

                star_el = page2.query_selector("span.a-icon-alt")
                star_rating = star_el.inner_text().strip() if star_el else "N/A"
                total_rating_el = page2.query_selector("#acrCustomerReviewText")
                total_ratings = total_rating_el.inner_text().strip() if total_rating_el else "0"
                bullets = [b.inner_text().strip() for b in page2.query_selector_all("#feature-bullets ul li span")]

                flavour = size = number_of_items = None
                rows = page2.query_selector_all("tr")
                for row in rows:
                    try:
                        label = row.query_selector("td.a-span3 span") or row.query_selector("th")
                        value = row.query_selector("td.a-span9 span") or row.query_selector("td")
                        if label and value:
                            key = label.inner_text().strip().lower()
                            val = value.inner_text().strip()
                            if "flavour" in key or "flavor" in key:
                                flavour = val
                            elif "size" in key or "unit" in key:
                                size = val
                            elif "number of items" in key or "item count" in key:
                                number_of_items = val
                    except Exception:
                        continue

                try:
                    image_el = page2.query_selector("#landingImage")
                    images = image_el.get_attribute("src") if image_el else None
                except Exception:
                    images = None

                scraped.append({
                    "ASIN": asin,
                    "URL": product_url,
                    "Title": title,
                    "Brand": brand,
                    "Price Raw": price_raw,
                    "Price (GBP)": price,
                    "Star Rating": star_rating,
                    "Total Ratings": total_ratings,
                    "Flavour": flavour,
                    "Size": size,
                    "No. of Products": number_of_items,
                    "Description": " | ".join(bullets) if bullets else "",
                    "Product Images": images or "N/A"
                })

                try:
                    page2.close()
                except:
                    pass
                try:
                    context2.close()
                except:
                    pass

                time.sleep(random.uniform(2, 4))

            except Exception as e:
                print(f"❌ Failed to scrape {asin}: {e}")
                failed_asins.append(asin)
    finally:
        try:
            page.close()
        except:
            pass
        try:
            context.close()
        except:
            pass
        try:
            browser.close()
        except:
            pass

    print(f"🔚 Amazon scraping finished. Scraped {len(scraped)} products, failed {len(failed_asins)}")
    return scraped, failed_asins

# ---------------- Symprove scraper ----------------
def parse_float_safe(v):
    try:
        if v is None or v == "":
            return None
        return float(v)
    except:
        try:
            v2 = re.sub(r'[^\d\.]', '', str(v))
            return float(v2) if v2 else None
        except:
            return None

def normalize_pack_text(text: str) -> str:
    if not text:
        return "N/A"
    t = text.lower()
    if re.search(r'\btwin\b', t) or re.search(r'\b2-?pack\b', t) or re.search(r'\bdouble\b', t):
        return "Twin Pack"
    if re.search(r'\bsingle\b', t) or re.search(r'\b1-?pack\b', t) or re.search(r'pack of 1', t):
        return "Single Pack"
    m = re.search(r'(single|twin|2x|2 x|1x|1 x)', t)
    if m:
        if 'twin' in m.group(0) or '2' in m.group(0):
            return "Twin Pack"
        else:
            return "Single Pack"
    return text.strip()

def is_explicit_subscription(text, option_map):
    SUB_KEYWORDS = ["subscribe", "subscription", "subscribe & save", "save", "autoship", "recurring"]
    t = text.lower()
    if any(k in t for k in SUB_KEYWORDS):
        return True
    for k, v in option_map.items():
        if v and any(s in (k + " " + v).lower() for s in SUB_KEYWORDS):
            return True
    return False

def is_explicit_onetime(text, option_map):
    ONE_KEYWORDS = ["one-time", "one time", "one-off", "single", "non-sub", "oneoff"]
    t = text.lower()
    if any(k in t for k in ONE_KEYWORDS):
        return True
    for k, v in option_map.items():
        if v and any(s in (k + " " + v).lower() for s in ONE_KEYWORDS):
            return True
    return False

def extract_symprove_products():
    print("\n🔎 Scraping Symprove.com product feed...")
    catalog_url = "https://www.symprove.com/collections/all/products.json"
    try:
        r = requests.get(catalog_url, timeout=30)
    except Exception as e:
        print("⚠️ Error fetching Symprove API:", e)
        return []
    if r.status_code != 200:
        print(f"⚠️ Failed to fetch Symprove API (status {r.status_code})")
        return []

    products = r.json().get("products", [])
    print(f"   → Found {len(products)} products on Symprove.com")
    rows = []

    for prod in products:
        pname = prod.get("title", "N/A")
        handle = prod.get("handle", "")
        if re.search(r'\b(foc|marketing|pr)\b', pname, re.I) or re.search(r'\b(foc|marketing|pr)\b', handle, re.I):
            continue
        raw_desc = prod.get("body_html") or ""
        desc = re.sub("<.*?>", "", raw_desc).strip()
        if not desc:
            desc = pname
        if len(desc) > 400:
            desc = desc[:400] + "..."
        images = [img.get("src") for img in prod.get("images", []) if img.get("src")]
        images_str = ", ".join(images)

        raw_options = prod.get("options", []) or []
        option_names = []
        for opt in raw_options:
            if isinstance(opt, dict):
                option_names.append(opt.get("name") or "Option")
            else:
                option_names.append(str(opt))

        variant_list = []
        for variant in prod.get("variants", []):
            option_map = {}
            for idx in range(1, 4):
                val = variant.get(f"option{idx}") or ""
                name = option_names[idx-1] if idx-1 < len(option_names) else f"option{idx}"
                option_map[name] = val
            vtitle = (variant.get("title") or "").strip()
            combined = " ".join([pname, vtitle] + list(option_map.values())).strip()
            price = parse_float_safe(variant.get("price"))
            compare = parse_float_safe(variant.get("compare_at_price"))
            explicit_sub = is_explicit_subscription(combined, option_map)
            explicit_one = is_explicit_onetime(combined, option_map)

            variant_list.append({
                "id": variant.get("id"),
                "combined": combined,
                "options": option_map,
                "title": vtitle,
                "price": price,
                "compare": compare,
                "explicit_sub": explicit_sub,
                "explicit_one": explicit_one
            })

        if re.search(r'shot\s*glass', pname, re.I):
            for v in variant_list:
                flavour = None
                for oname, oval in v["options"].items():
                    if oval and re.search(r'flavour|flavor|taste|variant', oname, re.I):
                        flavour = oval
                        break
                flavour = flavour or "Default"
                pack_candidate = None
                for val in v["options"].values():
                    if val and re.search(r'(single|twin|pack|\d+\s*x|\d+\s*ml)', str(val), re.I):
                        pack_candidate = val.strip()
                        break
                if not pack_candidate and v["title"]:
                    pack_candidate = v["title"]
                pack = normalize_pack_text(pack_candidate)
                one_price = v["price"]
                subs_price = None
                purchase_type = "Pack-based"
                def fmt(p):
                    if p is None:
                        return "N/A"
                    try:
                        return f"£{float(p):.2f}"
                    except:
                        return str(p)
                rows.append({
                    "Product Name": pname,
                    "Description": desc,
                    "Flavour Name": flavour,
                    "One-time Price": fmt(one_price),
                    "Subscription Price": fmt(subs_price),
                    "Purchase Type": purchase_type,
                    "Pack": pack,
                    "Product Images": images_str,
                    "URL": f"https://www.symprove.com/products/{handle}"
                })
            continue

        groups = defaultdict(list)
        for v in variant_list:
            flavour = "Default"
            for oname, oval in v["options"].items():
                if oval and re.search(r'flavour|flavor|taste|variant', oname, re.I):
                    flavour = oval
                    break
            if flavour == "Default":
                m = re.search(r'(Original|Mango|Strawberry|Raspberry|Pineapple|Lemon|Tropical|Berry|Apple|Orange)', v["combined"], re.I)
                if m:
                    flavour = m.group(0)
            groups[flavour].append(v)

        for flavour_name, gvars in groups.items():
            subs_price, one_price = None, None
            explicit_subs = [v for v in gvars if v["explicit_sub"]]
            explicit_ones = [v for v in gvars if v["explicit_one"]]

            if explicit_subs:
                prices = [v["price"] for v in explicit_subs if v["price"]]
                subs_price = max(prices) if prices else None
            if explicit_ones:
                prices = [v["price"] for v in explicit_ones if v["price"]]
                one_price = min(prices) if prices else None

            product_is_daily = bool(re.search(r'\bdaily\b|\bdaily-essential\b', pname, re.I) or re.search(r'\bdaily\b|\bdaily-essential\b', handle, re.I))
            group_prices = sorted(set([v["price"] for v in gvars if v["price"] is not None]))
            if product_is_daily and len(group_prices) == 2:
                subs_price, one_price = group_prices[-1], group_prices[0]

            if re.search(r'\bon\s*the\s*go\b', pname, re.I):
                single_prices = sorted(set([v["price"] for v in gvars if v["price"]]))
                one_price = single_prices[0] if single_prices else None
                subs_price = None
                purchase_type = "One-time (On The Go)"
            else:
                purchase_type = (
                    "Both"
                    if one_price and subs_price
                    else "Subscription only"
                    if subs_price
                    else "One-time only"
                )

            pack = normalize_pack_text("".join([v["title"] for v in gvars]))
            def fmt(p):
                if p is None:
                    return "N/A"
                try:
                    return f"£{float(p):.2f}"
                except:
                    return str(p)

            rows.append({
                "Product Name": pname,
                "Description": desc,
                "Flavour Name": flavour_name,
                "One-time Price": fmt(one_price),
                "Subscription Price": fmt(subs_price),
                "Purchase Type": purchase_type,
                "Pack": pack,
                "Product Images": images_str,
                "URL": f"https://www.symprove.com/products/{handle}"
            })
    return rows

# ---------------- Main execution ----------------
if __name__ == "__main__":
    with sync_playwright() as playwright:
        amazon_data, amazon_fail = extract_amazon_products(playwright)

    symprove_data = extract_symprove_products()

    print("\n✅ Scraping complete!")
    print(f"Amazon products scraped: {len(amazon_data)} (failed {len(amazon_fail)})")
    print(f"Symprove products scraped: {len(symprove_data)}")

    # Export to Excel
    now = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    amazon_df = pd.DataFrame(amazon_data)
    symprove_df = pd.DataFrame(symprove_data)

    excel_file = f"amazon_symprove_{now}.xlsx"
    with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
        amazon_df.to_excel(writer, sheet_name="Amazon", index=False)
        symprove_df.to_excel(writer, sheet_name="Symprove", index=False)

    print(f"💾 Data exported to {excel_file}")
