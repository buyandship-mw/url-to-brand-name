import csv
import json
import os
import argparse
from modules.prompting import build_prompt
from modules.llm_client import prompt_model
from modules.extraction import _thread_map


def cleanup_brand_name(name: str) -> str:
    """Return a cleaned brand name for easier consolidation."""
    if not name:
        return ""
    cleaned = name.replace("_", " ").replace("-", " ")
    cleaned = " ".join(cleaned.split())
    return cleaned.title()


def process_row(row: dict) -> dict:
    """Process a single CSV row and return the brand extraction result."""
    month = row.get("month", "")
    url = row.get("url", "")
    item_count = row.get("item_count", "")
    image_url = row.get("image_url", "")

    item_name = row.get("item_name", "").strip()
    error = row.get("error", "").strip()
    if error:
        return {
            "month": month,
            "url": url,
            "item_count": item_count,
            "item_name": item_name,
            "image_url": image_url,
            "brand": "",
            "brand_error": error,
        }

    input_text = item_name if item_name else url
    prompt = build_prompt(input_text)
    print(prompt)

    brand = ""
    brand_error = ""
    try:
        raw = prompt_model(prompt)
        data = json.loads(raw)
        brand = cleanup_brand_name(data.get("name", ""))
    except Exception as e:
        brand_error = str(e)

    return {
        "month": month,
        "url": url,
        "item_count": item_count,
        "item_name": item_name,
        "image_url": image_url,
        "brand": brand,
        "brand_error": brand_error,
    }

def batch_process(rows, max_workers: int | None = None) -> list[dict]:
    """Process rows concurrently and return brand extraction results."""
    if max_workers is None:
        max_workers = os.cpu_count() or 1
    return _thread_map(process_row, rows, max_workers)


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

    results = batch_process(rows)

    fieldnames = [
        "month",
        "url",
        "item_count",
        "item_name",
        "image_url",
        "brand",
        "brand_error",
    ]
    with open("data/output/brands.csv", "a", newline="") as f_out:
        writer = csv.DictWriter(f_out, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)

if __name__ == "__main__":
    main()
