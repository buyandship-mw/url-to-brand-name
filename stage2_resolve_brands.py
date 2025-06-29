import csv
import json
from modules.prompting import build_prompt
from modules.model_client import prompt_model


def main():
    results = []
    try:
        with open("stage1_item_names.csv", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                url = row.get("url", "")
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
                    "url": url,
                    "brand": brand,
                    "brand_error": brand_error,
                })
    except FileNotFoundError:
        print("stage1_item_names.csv not found")
        return

    with open("final_brands.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["url", "brand", "brand_error"])
        writer.writeheader()
        writer.writerows(results)


if __name__ == "__main__":
    main()
