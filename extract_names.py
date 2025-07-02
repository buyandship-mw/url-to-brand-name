import csv
import argparse
import os
from modules.extraction import batch_extract, merge_thread_csvs

FIELDNAMES = [
    "month",
    "url",
    "item_count",
    "image_url",
    "item_name",
    "error",
    "used_fallback",
]

def batch_process(rows, max_workers: int = 2, csv_prefix: str | None = None):
    """Return processed rows with extracted item names."""
    return batch_extract(rows, max_workers=max_workers, csv_prefix=csv_prefix)

def main():
    parser = argparse.ArgumentParser(description="Extract item names from URLs")
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

    csv_prefix = "data/output/item_names"
    output_path = "data/output/item_names.csv"
    results = batch_process(rows, max_workers=2, csv_prefix=csv_prefix)

    merge_thread_csvs(csv_prefix, output_path, FIELDNAMES)

    os.makedirs("data/output", exist_ok=True)
    write_header = not os.path.exists(output_path) or os.stat(output_path).st_size == 0
    with open(output_path, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        if write_header:
            writer.writeheader()
        writer.writerows(results)

if __name__ == "__main__":
    main()
