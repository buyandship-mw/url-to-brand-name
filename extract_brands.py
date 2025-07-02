import csv
import json
import os
import argparse
import tempfile
from pathlib import Path
from modules.prompting import build_prompt
from modules.llm_client import prompt_model
from modules.extraction import _thread_map, write_thread_csv_row, merge_thread_csvs

FIELDNAMES = [
    "month",
    "url",
    "item_count",
    "item_name",
    "image_url",
    "brand",
    "brand_error",
]


def cleanup_brand_name(name: str) -> str:
    """Return a cleaned brand name for easier consolidation."""
    if not name:
        return ""
    cleaned = name.replace("_", " ").replace("-", " ")
    cleaned = " ".join(cleaned.split())
    return cleaned.upper()

def process_row(row: dict) -> dict:
    """Process a single CSV row and return the brand extraction result."""
    month = row.get("month", "")
    url = row.get("url", "")
    item_count = row.get("item_count", "")
    image_url = row.get("image_url", "")
    fallback = str(row.get("used_fallback", "False")).lower() == "true"

    item_name = row.get("item_name", "").strip()
    input_text = url if fallback else item_name
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
    tmp_dir = Path(tempfile.mkdtemp())

    def _worker(row: dict) -> dict:
        result = process_row(row)
        write_thread_csv_row(result, FIELDNAMES, tmp_dir)
        return result

    results = _thread_map(_worker, rows, max_workers)
    merge_thread_csvs(tmp_dir, Path("data/output/brands.csv"), FIELDNAMES)
    return results


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

if __name__ == "__main__":
    main()
