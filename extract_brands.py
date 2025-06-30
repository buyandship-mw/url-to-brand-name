import csv
import json
import os
import argparse
from concurrent.futures import ThreadPoolExecutor
from modules.prompting import build_prompt
from modules.llm_client import prompt_model


def process_row(row: dict) -> dict:
    """Process a single CSV row and return the brand extraction result."""
    month = row.get("month", "")
    url = row.get("url", "")
    item_count = row.get("item_count", "")
    image_url = row.get("image_url", "")

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
        "image_url": image_url,
        "brand": brand,
        "brand_error": brand_error,
    }

def worker(worker_id: int, rows: list[dict]):
    """Process a chunk of rows and write results to a worker specific file."""
    outfile = f"data/output/brands_{worker_id}.csv"
    fieldnames = ["month", "url", "item_count", "image_url", "brand", "brand_error"]
    with open(outfile, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            result = process_row(row)
            writer.writerow(result)


def main():
    parser = argparse.ArgumentParser(description="Extract brand names")
    parser.add_argument("--start", type=int, default=1, help="First row to process (1-indexed)")
    parser.add_argument("--end", type=int, default=None, help="Last row to process (inclusive)")
    args = parser.parse_args()

    try:
        with open("data/output/item_names.csv", newline="") as f:
            all_rows = list(csv.DictReader(f))
    except FileNotFoundError:
        print("item_names.csv not found")
        return

    start = max(args.start - 1, 0)
    end = args.end if args.end is not None else len(all_rows)
    rows = all_rows[start:end]
    print(f"Processing rows {start + 1} to {min(end, len(all_rows))} of {len(all_rows)}")

    num_workers = os.cpu_count() or 1
    chunks = [rows[i::num_workers] for i in range(num_workers)]

    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = [executor.submit(worker, i, chunk) for i, chunk in enumerate(chunks)]
        for future in futures:
            future.result()

    fieldnames = ["month", "url", "item_count", "image_url", "brand", "brand_error"]
    with open("data/output/brands.csv", "a", newline="") as f_out:
        writer = csv.DictWriter(f_out, fieldnames=fieldnames)
        writer.writeheader()
        for i in range(num_workers):
            part_file = f"data/output/brands_{i}.csv"
            if not os.path.exists(part_file):
                continue
            with open(part_file, newline="") as pf:
                reader = csv.DictReader(pf)
                writer.writerows(reader)
            try:
                os.remove(part_file)
            except OSError as e:
                print(f"Error deleting {part_file}: {e}")

if __name__ == "__main__":
    main()
