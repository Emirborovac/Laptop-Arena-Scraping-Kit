import sqlite3
import requests
from bs4 import BeautifulSoup
import json

# Proxy settings
username = "Yusuf_iV5xx"
password = "AmirAmir994994+"
proxy_base = "dc.oxylabs.io"
starting_port = 8052
max_port = 9000

def get_proxies(port):
    """Generate proxy settings for a specific port."""
    proxy = f"{proxy_base}:{port}"
    return {
        "https": f"https://user-{username}:{password}@{proxy}",
    }

def fetch_and_store_to_db(url, db_name="products.db", retries=5):
    """Fetch product data and store it in the database."""
    # Define headers with user agent
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/114.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
    }

    # Define the base domain
    base_domain = "https://www.laptoparena.net"

    port = starting_port  # Start with the first port

    for attempt in range(retries):
        try:
            proxies = get_proxies(port)
            print(f"Using proxy on port: {port}")
            
            # Send GET request with the proxy
            response = requests.get(url, headers=headers, proxies=proxies, timeout=10)
            
            if response.status_code != 200:
                print(f"Failed with status code: {response.status_code}. Retrying...")
                raise requests.RequestException
            
            # Parse HTML content
            soup = BeautifulSoup(response.content, "html.parser")
            table = soup.find("table", class_="specs responsive")
            gallery = soup.find("div", class_="gallery")
            
            if not table:
                print("Specified table not found on the page. Skipping...")
                return False
            
            # Extract rows from the table
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

            # Combine Brand, Model Name, and Part Number to create Product Name
            product_name = f"{brand} {model_name} {part_number}"

            # Extract image URLs and prepend the base domain
            image_urls = []
            if gallery:
                for img in gallery.find_all("img", class_="gallery-image"):
                    src = img.get("data-src") or img.get("src")
                    if src:
                        full_url = f"{base_domain}{src}"  # Prepend base domain to the URL
                        image_urls.append(full_url)

            # Convert image URLs to JSON for storage
            images_json = json.dumps(image_urls)

            # Connect to SQLite database
            conn = sqlite3.connect(db_name)
            cursor = conn.cursor()
            
            # Ensure the products table exists
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS products (
                    ID INTEGER PRIMARY KEY AUTOINCREMENT,
                    Brand TEXT,
                    ProductName TEXT,
                    Images TEXT
                )
            """)
            
            # Check existing columns
            cursor.execute("PRAGMA table_info(products)")
            existing_columns = {column[1] for column in cursor.fetchall()}

            for column in product_data.keys():
                if column not in existing_columns:
                    cursor.execute(f"ALTER TABLE products ADD COLUMN \"{column}\" TEXT")

            # Insert product data
            placeholders = ', '.join(['?'] * (len(product_data) + 3))
            columns = ', '.join(['"Brand"', '"ProductName"', '"Images"'] + [f'"{column}"' for column in product_data.keys()])
            values = [brand, product_name, images_json] + list(product_data.values())
            cursor.execute(f"INSERT INTO products ({columns}) VALUES ({placeholders})", values)
            
            # Commit and close
            conn.commit()
            conn.close()
            print(f"Product '{product_name}' has been saved to the database.")
            return True

        except requests.RequestException:
            print(f"Request failed on port {port}. Trying next port...")
            port += 1
            if port > max_port:
                port = starting_port  # Wrap around to the first port

    print("All retries failed. Skipping this URL.")
    return False


def process_urls_from_db(url_db_name="Models_urls-2.db", url_table="models_urls", url_column="url"):
    """Process URLs from the database and fetch product data."""
    # Connect to the database containing URLs
    conn = sqlite3.connect(url_db_name)
    cursor = conn.cursor()
    
    # Check if the 'processed' column already exists
    cursor.execute(f"PRAGMA table_info({url_table})")
    columns = {row[1] for row in cursor.fetchall()}  # Get column names
    if "processed" not in columns:
        # Add the processed column only if it doesn't exist
        cursor.execute(f"""
            ALTER TABLE {url_table} ADD COLUMN processed INTEGER DEFAULT 0
        """)
        conn.commit()
    
    # Fetch unprocessed URLs
    cursor.execute(f"SELECT id, {url_column} FROM {url_table} WHERE processed = 0 ORDER BY id ASC")
    urls = cursor.fetchall()
    
    for url_row in urls:
        row_id, url = url_row
        print(f"Processing URL (ID {row_id}): {url}")
        
        # Attempt to process and store the product data
        if fetch_and_store_to_db(url):
            # Mark the URL as processed
            cursor.execute(f"UPDATE {url_table} SET processed = 1 WHERE id = ?", (row_id,))
            conn.commit()
        else:
            print(f"Failed to process URL: {url}. Skipping...")
    
    conn.close()
    print("All URLs have been processed.")

# Example usage
process_urls_from_db(url_db_name="Models_urls-2.db", url_table="models_urls", url_column="url")
