import csv
import json
import os
import argparse
from modules.prompting import (
    build_prompt,
    build_url_assess_prompt,
    build_url_extract_prompt,
)
from modules.llm_client import prompt_model, prompt_openai
from modules.extraction import _thread_map, extract_item_name


def process_row(row: dict) -> dict:
    """Process a single CSV row and return the brand extraction result."""
    month = row.get("month", "")
    url = row.get("item_url", "")
    item_count = row.get("item_count", "")
    image_url = row.get("image_url", "")

    item_name = row.get("item_name", "").strip()
    brand = ""
    brand_error = ""

    # Step 1: Can brand be extracted from URL directly?
    try:
        assess_prompt = build_url_assess_prompt(url)
        print(assess_prompt)
        assessment = prompt_openai(assess_prompt)
    except Exception as e:  # noqa: BLE001
        assessment = ""
        brand_error = str(e)

    if assessment.strip().lower().startswith("yes"):
        try:
            extract_prompt = build_url_extract_prompt(url)
            print(extract_prompt)
            raw = prompt_openai(extract_prompt)
            brand = json.loads(raw).get("name", "")
        except Exception as e:  # noqa: BLE001
            brand_error = str(e)

    if not brand:
        if not item_name:
            try:
                item_name = extract_item_name(url)
            except Exception as e:  # noqa: BLE001
                brand_error = str(e)
                item_name = ""

        for text in filter(None, [item_name, url]):
            prompt = build_prompt(text)
            print(prompt)
            try:
                raw = prompt_model(prompt)
                data = json.loads(raw)
                brand = data.get("name", "")
                if brand:
                    break
            except Exception as e:  # noqa: BLE001
                brand_error = str(e)

    return {
        "month": month,
        "item_url": url,
        "item_count": item_count,
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

    fieldnames = ["month", "item_url", "item_count", "image_url", "brand", "brand_error"]
    with open("data/output/brands.csv", "a", newline="") as f_out:
        writer = csv.DictWriter(f_out, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)

if __name__ == "__main__":
    main()
