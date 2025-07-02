# extract_pipeline.py

"""Run the full URL -> brand pipeline in one step."""

import csv
import json
import argparse
from modules.prompting import (
    build_url_assess_prompt,
    build_url_extract_prompt,
)
from modules.llm_client import prompt_openai
import extract_names
import extract_brands


def process_rows(rows, max_workers: int = 2) -> list[dict]:
    """Return brand info for each row using the new URL-first logic."""
    direct = []
    remaining = []

    for row in rows:
        url = row.get("item_url", "")
        month = row.get("month", "")
        item_count = row.get("item_count", "")
        image_url = ""
        brand = ""
        brand_error = ""

        try:
            prompt = build_url_assess_prompt(url)
            print(prompt)
            assessment = prompt_openai(prompt)
        except Exception as e:  # noqa: BLE001
            assessment = ""
            brand_error = str(e)
        
        response = assessment.strip().lower()
        print(response)

        if response.startswith("no"):
            try:
                prompt = build_url_extract_prompt(url)
                print(prompt)
                raw = prompt_openai(prompt)
                brand = json.loads(raw).get("name", "")
            except Exception as e:  # noqa: BLE001
                brand_error = str(e)

        if brand:
            direct.append(
                {
                    "month": month,
                    "item_url": url,
                    "item_count": item_count,
                    "image_url": image_url,
                    "brand": brand,
                    "brand_error": brand_error,
                }
            )
        else:
            remaining.append(row)

    if remaining:
        name_rows = extract_names.batch_process(remaining, max_workers=max_workers)
        brand_rows = extract_brands.batch_process(name_rows, max_workers=max_workers)
        direct.extend(brand_rows)

    return direct


def main() -> None:
    parser = argparse.ArgumentParser(description="Extract brands directly from URLs")
    parser.add_argument("--start", type=int, default=1, help="First row to process (1-indexed)")
    parser.add_argument("--end", type=int, default=None, help="Last row to process (inclusive)")
    args = parser.parse_args()

    try:
        with open("data/input.csv", newline="") as f:
            all_rows = list(csv.DictReader(f))
    except FileNotFoundError:
        print("data/input.csv not found")
        return

    start = max(args.start - 1, 0)
    end = args.end if args.end is not None else len(all_rows)
    rows = all_rows[start:end]
    print(f"Processing rows {start + 1} to {min(end, len(all_rows))} of {len(all_rows)}")

    results = process_rows(rows)

    fieldnames = ["month", "item_url", "item_count", "image_url", "brand", "brand_error"]
    with open("data/output/brands.csv", "a", newline="") as f_out:
        writer = csv.DictWriter(f_out, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)


if __name__ == "__main__":
    main()
