import csv
import glob
import os
import threading
from concurrent.futures import ThreadPoolExecutor
from modules.extraction import extract_item_name

FIELDNAMES = ["month", "url", "item_count", "item_name", "error"]

def process_row(row):
    month = row.get("month", "")
    url = row.get("item_url", "")
    item_count = row.get("item_count", "")
    item_name = ""
    error = ""
    print(f"Processing URL: {url}")
    try:
        item_name = extract_item_name(url)
    except Exception as e:
        error = str(e)

    result = {
        "month": month,
        "url": url,
        "item_count": item_count,
        "item_name": item_name,
        "error": error,
    }

    worker = threading.get_ident()
    out_file = f"data/output/item_names_{worker}.csv"
    is_new = not os.path.exists(out_file)
    with open(out_file, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        if is_new:
            writer.writeheader()
        writer.writerow(result)

    return result

def main():
    try:
        with open("data/input.csv", newline="") as f:
            rows = list(csv.DictReader(f))
    except FileNotFoundError:
        print("data/input.csv not found")
        return

    max_workers = 2 # limited to 2 concurrent due to Firecrawl API free plan rate limit
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(process_row, row) for row in rows]
        for future in futures:
            future.result()

    results = []
    for path in glob.glob("data/output/item_names_*.csv"):
        with open(path, newline="") as f:
            reader = csv.DictReader(f)
            results.extend(reader)

    with open("data/output/item_names.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(results)

if __name__ == "__main__":
    main()
