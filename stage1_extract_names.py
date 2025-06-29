import csv
from modules.extraction import extract_item_name


def main():
    results = []
    try:
        with open("urls.csv", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                month = row.get("month", "")
                url = row.get("url", "")
                item_name = ""
                error = ""
                try:
                    item_name = extract_item_name(url)
                except Exception as e:
                    error = str(e)
                results.append({
                    "month": month,
                    "url": url,
                    "item_name": item_name,
                    "error": error,
                })
    except FileNotFoundError:
        print("urls.csv not found")
        return

    with open("stage1_item_names.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["month", "url", "item_name", "error"])
        writer.writeheader()
        writer.writerows(results)


if __name__ == "__main__":
    main()
