import argparse
import csv
import os

from modules.brand_extraction import (
    BRAND_FIELDNAMES,
    batch_process,
    process_row,
)
from modules.llm_client import prompt_model  # re-exported for tests


def main() -> None:
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

    csv_prefix = "data/output/brands"
    output_path = "data/output/brands.csv"
    results = batch_process(rows, csv_prefix=csv_prefix, merge_path=output_path)

    os.makedirs("data/output", exist_ok=True)
    write_header = not os.path.exists(output_path) or os.stat(output_path).st_size == 0
    with open(output_path, "a", newline="") as f_out:
        writer = csv.DictWriter(f_out, fieldnames=BRAND_FIELDNAMES)
        if write_header:
            writer.writeheader()
        writer.writerows(results)


if __name__ == "__main__":
    main()
