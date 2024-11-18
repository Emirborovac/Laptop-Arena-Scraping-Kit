import sqlite3
import requests
from bs4 import BeautifulSoup
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
from contextlib import closing
from datetime import datetime
import traceback

# Proxy settings remain the same
username = "Yusuf_iV5xx"
password = "AmirAmir994994+"
proxy_base = "dc.oxylabs.io"
starting_port = 8001
max_port = 9000
progress_file = "progress.json"
error_log_file = "error_log.txt"

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
    print("[DEBUG] Loading progress from progress.json")
    if os.path.exists(progress_file):
        with open(progress_file, "r") as f:
            return json.load(f)
    print("[DEBUG] No progress file found. Starting fresh.")
    return {}

def save_progress(progress):
    print(f"[DEBUG] Saving progress: {progress}")
    with open(progress_file, "w") as f:
        json.dump(progress, f)

def get_db_connection(db_name):
    """Create a new database connection."""
    try:
        conn = sqlite3.connect(db_name, timeout=30.0)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=30000")
        return conn
    except sqlite3.Error as e:
        log_error(f"Database connection error: {e}")
        raise

def fetch_and_store_to_db(url, row_id, port, products_db="products.db", retries=10):
    """Fetch product data and store it in the database."""
    print(f"[DEBUG] Starting to process URL ID {row_id} on port {port}")
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
    }
    base_domain = "https://www.laptoparena.net"

    for attempt in range(retries):
        try:
            proxies = get_proxies(port)
            print(f"[DEBUG] Using proxy on port: {port} for URL ID: {row_id} "
                  f"(Attempt {attempt + 1}/{retries})")
            
            response = requests.get(url, headers=headers, proxies=proxies, timeout=10)
            
            if response.status_code != 200:
                error_msg = f"Failed with status code: {response.status_code}"
                print(f"[DEBUG] {error_msg}. Retrying...")
                if attempt == retries - 1:  # Log only on last attempt
                    log_error(error_msg, row_id, url)
                raise requests.RequestException(error_msg)
            
            soup = BeautifulSoup(response.content, "html.parser")
            table = soup.find("table", class_="specs responsive")
            gallery = soup.find("div", class_="gallery")
            
            if not table:
                error_msg = "Specified table not found"
                print(f"[DEBUG] {error_msg} for URL ID: {row_id}. Skipping...")
                log_error(error_msg, row_id, url)
                return False
            
            rows = table.find_all("tr")
            product_data = {}
            brand = model_name = part_number = "Unknown"

            for row in rows:
                if row.find("td"):
                    cells = row.find_all("td")
                    if len(cells) == 2:
                        label = cells[0].get_text(strip=True)
                        value = cells[1].get_text(strip=True)
                        product_data[label] = value

                        if label.lower() == "brand":
                            brand = value
                        elif label.lower() == "model name":
                            model_name = value
                        elif label.lower() == "part number":
                            part_number = value

            product_name = f"{brand} {model_name} {part_number}"

            image_urls = []
            if gallery:
                for img in gallery.find_all("img", class_="gallery-image"):
                    src = img.get("data-src") or img.get("src")
                    if src:
                        full_url = f"{base_domain}{src}"
                        image_urls.append(full_url)

            images_json = json.dumps(image_urls)

            try:
                with closing(get_db_connection(products_db)) as conn:
                    with conn:
                        cursor = conn.cursor()
                        
                        # Create base table with Url after ProductName
                        cursor.execute("""
                            CREATE TABLE IF NOT EXISTS products (
                                ID INTEGER PRIMARY KEY AUTOINCREMENT,
                                Brand TEXT,
                                ProductName TEXT,
                                Url TEXT,
                                Images TEXT
                            )
                        """)

                        # Get existing columns
                        cursor.execute("PRAGMA table_info(products)")
                        existing_columns = {column[1] for column in cursor.fetchall()}

                        # Add new columns if they don't exist
                        new_columns = []
                        for column in product_data.keys():
                            if column not in existing_columns:
                                try:
                                    cursor.execute(f'ALTER TABLE products ADD COLUMN "{column}" TEXT')
                                    new_columns.append(column)
                                    print(f"[DEBUG] Added new column: {column}")
                                except sqlite3.OperationalError as e:
                                    if "duplicate column name" not in str(e):
                                        raise
                                    print(f"[DEBUG] Column already exists: {column}")

                        # Prepare insert statement using all columns (new and existing)
                        all_columns = ['"Brand"', '"ProductName"', '"Url"', '"Images"']
                        values = [brand, product_name, url, images_json]
                        
                        # Add product data columns and values
                        for column, value in product_data.items():
                            if column in existing_columns or column in new_columns:
                                all_columns.append(f'"{column}"')
                                values.append(value)

                        placeholders = ', '.join(['?'] * len(values))
                        columns_str = ', '.join(all_columns)
                        
                        # Insert the data
                        cursor.execute(
                            f"INSERT INTO products ({columns_str}) VALUES ({placeholders})", 
                            values
                        )

                print(f"[DEBUG] Product '{product_name}' has been saved to the database.")
                return True

            except sqlite3.Error as e:
                error_msg = f"Database error while storing product: {e}"
                log_error(error_msg, row_id, url)
                raise

        except requests.RequestException as e:
            print(f"[DEBUG] Request failed on port {port} for URL ID: {row_id}. Error: {e}")
            port += 1
            if port > max_port:
                port = starting_port
            
            if attempt == retries - 1:  # Log only on last attempt
                log_error(f"Request failed after {retries} attempts: {e}", row_id, url)

        except Exception as e:
            error_msg = f"Unexpected error: {e}"
            log_error(error_msg, row_id, url)
            raise

    print(f"[DEBUG] All retries failed for URL ID: {row_id}. Skipping...")
    return False

def process_urls_from_db(url_db_name="Models_urls-2.db", url_table="models_urls", 
                        url_column="url", batch_size=100):
    print("[DEBUG] Connecting to the database")
    
    try:
        with closing(get_db_connection(url_db_name)) as conn:
            with conn:
                cursor = conn.cursor()
                
                cursor.execute(f"PRAGMA table_info({url_table})")
                columns = {row[1] for row in cursor.fetchall()}
                if "processed" not in columns:
                    print("[DEBUG] Adding 'processed' column to the table")
                    cursor.execute(f"""
                        ALTER TABLE {url_table} ADD COLUMN processed INTEGER DEFAULT 0
                    """)

                cursor.execute(f"SELECT id, {url_column} FROM {url_table} "
                             f"WHERE processed = 0 ORDER BY id ASC")
                urls = cursor.fetchall()
                print(f"[DEBUG] Fetched {len(urls)} unprocessed URLs")

    except sqlite3.Error as e:
        log_error(f"Database error in process_urls_from_db: {e}")
        raise

    # Load progress
    progress = load_progress()
    already_processed = progress.get("processed", [])

    # Filter unprocessed URLs
    urls = [url for url in urls if url[0] not in already_processed]
    print(f"[DEBUG] Remaining URLs to process: {len(urls)}")

    batch_start = starting_port

    with ThreadPoolExecutor(max_workers=batch_size) as executor:
        future_to_url = {}
        for i, (row_id, url) in enumerate(urls):
            port = batch_start + (i % 100)
            future = executor.submit(fetch_and_store_to_db, url, row_id, port)
            future_to_url[future] = (row_id, url)

        for future in as_completed(future_to_url):
            row_id, url = future_to_url[future]
            try:
                if future.result():
                    with closing(get_db_connection(url_db_name)) as conn:
                        with conn:
                            cursor = conn.cursor()
                            cursor.execute(f"UPDATE {url_table} SET processed = 1 WHERE id = ?", 
                                        (row_id,))
                    
                    already_processed.append(row_id)
                    progress["processed"] = already_processed
                    save_progress(progress)
            except Exception as e:
                error_msg = f"Error processing URL ID {row_id}: {e}"
                print(f"[DEBUG] {error_msg}")
                log_error(error_msg, row_id, url)

    print("[DEBUG] All URLs have been processed.")

if __name__ == "__main__":
    print("[DEBUG] Starting URL processing")
    try:
        process_urls_from_db(url_db_name="Models.db", url_table="models_urls", 
                           url_column="url", batch_size=100)
    except Exception as e:
        error_msg = f"Unhandled error during execution: {e}"
        print(f"[DEBUG] {error_msg}")
        log_error(error_msg)
