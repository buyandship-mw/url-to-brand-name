import csv
from modules.extraction import extract_item_name

def main():
    results = []
    try:
        with open("data/input.csv", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
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

                results.append({
                    "month": month,
                    "url": url,
                    "item_count": item_count,
                    "item_name": item_name,
                    "error": error,
                })
    except FileNotFoundError:
        print("data/input.csv not found")
        return

    with open("data/output/item_names.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["month", "url", "item_count", "item_name", "error"])
        writer.writeheader()
        writer.writerows(results)

if __name__ == "__main__":
    main()
