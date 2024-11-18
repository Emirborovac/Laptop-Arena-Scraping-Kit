import sqlite3
from playwright.sync_api import sync_playwright
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import closing
import json
import os
from datetime import datetime
import traceback

# Global Database Names
BRANDS_DB = "Brands.db"
PRODUCTS_DB = "Products.db"
progress_file = "progress.json"
error_log_file = "error_log.txt"

# Proxy Configuration
username = ""
password = ""
proxy_base = ""
starting_port = 8001
max_port = 9000

# Utility Functions
def log_error(error_message, url_id=None, url=None):
    """Log errors to error_log.txt with timestamp and details."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(error_log_file, "a", encoding='utf-8') as f:
        error_entry = f"""
[{timestamp}]
URL ID: {url_id if url_id else 'N/A'}
URL: {url if url else 'N/A'}
Error: {error_message}
Stack Trace: {traceback.format_exc()}
{'=' * 80}
"""
        f.write(error_entry)

def get_proxies(port):
    proxy = f"{proxy_base}:{port}"
    return {
        "https": f"https://user-{username}:{password}@{proxy}",
    }

def load_progress():
    if os.path.exists(progress_file):
        with open(progress_file, "r") as f:
            return json.load(f)
    return {}

def save_progress(progress):
    with open(progress_file, "w") as f:
        json.dump(progress, f)

def get_db_connection(db_name):
    """Create a new database connection."""
    conn = sqlite3.connect(db_name, timeout=30.0)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn

# Database Setup
def setup_databases():
    """Set up databases for brands and products."""
    # Brands database
    conn = sqlite3.connect(BRANDS_DB)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS models (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            brand TEXT NOT NULL,
            url TEXT NOT NULL UNIQUE
        )
    """)
    conn.commit()
    conn.close()

    # Products database
    conn = sqlite3.connect(PRODUCTS_DB)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            brand TEXT NOT NULL,
            product_name TEXT NOT NULL,
            url TEXT NOT NULL UNIQUE,
            specs TEXT,
            images TEXT
        )
    """)
    conn.commit()
    conn.close()

# Scrape Brand URLs
def scrape_brand_urls():
    """Scrape brand URLs from the main website."""
    brands = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        CATEGORY_PAGE_URL = "https://www.laptoparena.net/"
        page.goto(CATEGORY_PAGE_URL)

        brand_elements = page.query_selector_all(".brand a.brand-link")
        for element in brand_elements:
            brand_name = element.inner_text().strip()
            relative_url = element.get_attribute("href")
            full_url = f"https://www.laptoparena.net{relative_url}"
            brands.append((brand_name, full_url))

        browser.close()
    return brands

# Load All Models
def load_all_models(page, brand_name):
    """Load all models for a brand."""
    model_data = []
    unique_urls = set()
    while True:
        model_elements = page.query_selector_all(".product_container .product a")
        for element in model_elements:
            model_name = element.inner_text().strip()
            relative_url = element.get_attribute("href")
            full_url = f"https://www.laptoparena.net{relative_url}"
            if full_url not in unique_urls:
                unique_urls.add(full_url)
                model_data.append((brand_name, full_url))

        load_more_button = page.query_selector(".load_more_products")
        if load_more_button:
            load_more_button.click()
            page.wait_for_timeout(2000)
        else:
            break
    return model_data

# Scrape Models for Each Brand
def scrape_models_for_brands(brand_data):
    """Scrape models for each brand."""
    all_models = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        for brand_name, brand_url in brand_data:
            page = browser.new_page()
            page.goto(brand_url)
            models = load_all_models(page, brand_name)
            all_models.extend(models)
            page.close()
        browser.close()
    return all_models

# Fetch Specifications
def fetch_and_store_to_db(url, row_id, port, retries=10):
    """Fetch product specifications and store them in the Products DB."""
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
        )
    }
    base_domain = "https://www.laptoparena.net"

    for attempt in range(retries):
        try:
            proxies = get_proxies(port)
            response = requests.get(url, headers=headers, proxies=proxies, timeout=10)
            if response.status_code != 200:
                if attempt == retries - 1:
                    log_error(f"Failed with status code: {response.status_code}", row_id, url)
                raise requests.RequestException(f"Status code: {response.status_code}")
            
            soup = BeautifulSoup(response.content, "html.parser")
            table = soup.find("table", class_="specs responsive")
            gallery = soup.find("div", class_="gallery")
            
            if not table:
                log_error("Specified table not found", row_id, url)
                return False
            
            rows = table.find_all("tr")
            product_data = {row.find_all("td")[0].get_text(strip=True): row.find_all("td")[1].get_text(strip=True) for row in rows if row.find_all("td")}

            images = [f"{base_domain}{img.get('src')}" for img in gallery.find_all("img", class_="gallery-image")] if gallery else []
            images_json = json.dumps(images)

            product_name = product_data.get("Model Name", "Unknown")
            brand = product_data.get("Brand", "Unknown")

            with closing(get_db_connection(PRODUCTS_DB)) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT OR IGNORE INTO products (brand, product_name, url, specs, images)
                    VALUES (?, ?, ?, ?, ?)
                """, (brand, product_name, url, json.dumps(product_data), images_json))
                conn.commit()

            return True

        except Exception as e:
            if attempt == retries - 1:
                log_error(f"Error fetching specifications: {e}", row_id, url)
            port += 1
            if port > max_port:
                port = starting_port

    return False

# Process URLs from Models DB
def process_urls_from_db():
    """Fetch unprocessed URLs and scrape specifications."""
    conn = get_db_connection(BRANDS_DB)
    cursor = conn.cursor()
    cursor.execute("SELECT id, url FROM models")
    urls = cursor.fetchall()
    conn.close()

    progress = load_progress()
    already_processed = progress.get("processed", [])

    urls = [url for url in urls if url[0] not in already_processed]

    batch_start = starting_port
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_url = {
            executor.submit(fetch_and_store_to_db, url, row_id, batch_start + (i % 100)): (row_id, url)
            for i, (row_id, url) in enumerate(urls)
        }
        for future in as_completed(future_to_url):
            row_id, url = future_to_url[future]
            try:
                if future.result():
                    already_processed.append(row_id)
                    progress["processed"] = already_processed
                    save_progress(progress)
            except Exception as e:
                log_error(f"Unhandled error during URL processing: {e}", row_id, url)

# Main Execution
if __name__ == "__main__":
    setup_databases()

    print("Scraping brand URLs...")
    brands = scrape_brand_urls()

    print("Scraping models for each brand...")
    models = scrape_models_for_brands(brands)

    print("Saving models to database...")
    conn = get_db_connection(BRANDS_DB)
    cursor = conn.cursor()
    cursor.executemany("INSERT OR IGNORE INTO models (brand, url) VALUES (?, ?)", models)
    conn.commit()
    conn.close()

    print("Fetching product specifications...")
    process_urls_from_db()

    print("All tasks completed!")
