
# LaptopArena.net Scraper - README

## Overview

This repository contains a set of Python scripts to scrape laptop product details from LaptopArena.net. The scripts include functionalities for collecting brand and product URLs, extracting detailed specifications, storing data in SQLite databases, and exporting results to Excel files.

---

## Prerequisites

- Python 3.8 or higher
- Required libraries: 
  - `requests`
  - `beautifulsoup4`
  - `sqlite3` (built-in)
  - `concurrent.futures`
  - `openpyxl`
  - `pandas`
  - `playwright`
  - `math`

Install missing libraries with:
```bash
pip install requests beautifulsoup4 openpyxl pandas playwright
```

---

## Script Descriptions

### 1. `complete.py`

- **Purpose**: Performs a complete scrape process, from collecting brand URLs to storing product specifications and images in SQLite databases.
- **Key Features**:
  - Uses Playwright to navigate the website.
  - Implements multi-threaded scraping for efficiency.
  - Stores results in `Brands.db` and `Products.db`.

---

### 2. `prducts-fast.py`

- **Purpose**: A faster alternative for scraping product details.
- **Key Features**:
  - Utilizes proxy rotation for large-scale scraping.
  - Extracts detailed product specifications and images.
  - Dynamically updates database schema for new specifications.

---

### 3. `products.py`

- **Purpose**: Scrapes individual product pages for specifications and stores the data in SQLite.
- **Key Features**:
  - Handles missing data gracefully.
  - Dynamically adds new columns for specifications in the database.

---

### 4. `Sqlite-To-Excel.py`

- **Purpose**: Exports data from the SQLite database (`Products.db`) to an Excel file (`Products_Export.xlsx`).
- **Key Features**:
  - Splits large tables into multiple sheets if rows exceed Excel’s limit.
  - Handles multiple tables and exports them individually.

---

## Execution Steps

1. **Setup Database**:
   - Run `complete.py` to scrape brand URLs and populate `Brands.db` and `Products.db`.

2. **Scrape Product Details**:
   - Use `prducts-fast.py` or `products.py` to scrape individual product pages.

3. **Export Data**:
   - Run `Sqlite-To-Excel.py` to export the data into an Excel file for analysis.

---

## Notes

- **Proxy Configuration**:
  - Update proxy credentials (`username`, `password`, and `proxy_base`) in scripts using proxies (`prducts-fast.py` and `products.py`).

- **Database Schema**:
  - Ensure database paths and schema in the scripts match your setup.

- **Performance**:
  - Multi-threading is used to enhance scraping speed but may need to be adjusted based on your system's capabilities.

---

## Output

- SQLite databases:
  - `Brands.db`: Stores brand information and URLs.
  - `Products.db`: Stores product specifications and images.
- Excel file:
  - `Products_Export.xlsx`: Final export of all scraped data.

---

## Troubleshooting

- **Missing Data**:
  - Ensure the selectors for HTML elements match the current website structure.
- **Connection Issues**:
  - Check proxy configurations and ensure proxies are active.
