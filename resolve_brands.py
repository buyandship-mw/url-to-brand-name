import csv
import json
from modules.prompting import build_prompt
from modules.model_client import prompt_model

def main():
    results = []
    try:
        with open("data/output/item_names.csv", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                month = row.get("month", "")
                url = row.get("url", "")
                item_count = row.get("item_count", "")

                item_name = row.get("item_name", "").strip()
                input_text = item_name if item_name else url
                prompt = build_prompt(input_text)
                brand = ""
                brand_error = ""

                try:
                    raw = prompt_model(prompt)
                    data = json.loads(raw)
                    brand = data.get("name", "")
                except Exception as e:
                    brand_error = str(e)

                results.append({
                    "month": month,
                    "url": url,
                    "item_count": item_count,
                    "brand": brand,
                    "brand_error": brand_error,
                })
    except FileNotFoundError:
        print("item_names.csv not found")
        return

    with open("data/output/brands.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["month", "url", "item_count", "brand", "brand_error"])
        writer.writeheader()
        writer.writerows(results)

if __name__ == "__main__":
    main()
