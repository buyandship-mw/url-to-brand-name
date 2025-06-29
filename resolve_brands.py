import csv
import json
import os
from concurrent.futures import ThreadPoolExecutor
from modules.prompting import build_prompt
from modules.model_client import prompt_model


def process_row(row: dict) -> dict:
    """Process a single CSV row and return the brand extraction result."""
    month = row.get("month", "")
    url = row.get("url", "")
    item_count = row.get("item_count", "")

    item_name = row.get("item_name", "").strip()
    input_text = item_name if item_name else url
    prompt = build_prompt(input_text)
    print(prompt)

    brand = ""
    brand_error = ""
    try:
        raw = prompt_model(prompt)
        data = json.loads(raw)
        brand = data.get("name", "")
    except Exception as e:
        brand_error = str(e)

    return {
        "month": month,
        "url": url,
        "item_count": item_count,
        "brand": brand,
        "brand_error": brand_error,
    }

def worker(worker_id: int, rows: list[dict]):
    """Process a chunk of rows and write results to a worker specific file."""
    outfile = f"data/output/brands_{worker_id}.csv"
    fieldnames = ["month", "url", "item_count", "brand", "brand_error"]
    with open(outfile, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            result = process_row(row)
            writer.writerow(result)


def main():
    try:
        with open("data/output/item_names.csv", newline="") as f:
            rows = list(csv.DictReader(f))
    except FileNotFoundError:
        print("item_names.csv not found")
        return

    num_workers = os.cpu_count() or 1
    chunks = [rows[i::num_workers] for i in range(num_workers)]

    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = [executor.submit(worker, i, chunk) for i, chunk in enumerate(chunks)]
        for future in futures:
            future.result()

    fieldnames = ["month", "url", "item_count", "brand", "brand_error"]
    with open("data/output/brands.csv", "w", newline="") as f_out:
        writer = csv.DictWriter(f_out, fieldnames=fieldnames)
        writer.writeheader()
        for i in range(num_workers):
            part_file = f"data/output/brands_{i}.csv"
            if not os.path.exists(part_file):
                continue
            with open(part_file, newline="") as pf:
                reader = csv.DictReader(pf)
                writer.writerows(reader)

if __name__ == "__main__":
    main()
