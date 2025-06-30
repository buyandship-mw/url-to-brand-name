# URL to Brand Name

This project extracts item names and corresponding brand information from a list of URLs.
It uses the [Firecrawl](https://firecrawl.dev/) scraping API to obtain page metadata and
then queries Azure OpenAI to determine the brand.

## Setup

1. Install dependencies

```bash
pip install -r requirements.txt
```

2. Create a `.env` file or export the following environment variables:

- `FIRECRAWL_API_KEY` – API key for Firecrawl.
- `AZURE_OPENAI_API_KEY` – API key for Azure OpenAI.
- `AZURE_OPENAI_ENDPOINT` – Base endpoint URL for Azure OpenAI (e.g. `https://your-instance.openai.azure.com/`).
- `AZURE_OPENAI_DEPLOYMENT` – Name of the model deployment.
- `OPENAI_MAX_WORKERS` – *(optional)* number of threads used when calling OpenAI. Defaults to `2`.

## Usage

1. **Extract item names**

```python
import csv, extract_names

rows = list(csv.DictReader(open("data/input.csv")))
extract_names.batch_process(rows, max_workers=2)
```

This reads `data/input.csv`, fetches metadata with Firecrawl and writes the results to `data/output/item_names.csv`.

2. **Extract brand names**

```python
import csv, extract_brands

rows = list(csv.DictReader(open("data/output/item_names.csv")))
extract_brands.batch_process(rows)
```

This reads the item names file produced above and writes `data/output/brands.csv`.

## Notes

Both API helper functions include retry logic **only** when a `429` rate limit
response is received. The metadata extraction helper still retries up to **two**
times by default. Adjust the `retries` parameter in `modules/extraction.py` and
`modules/llm_client.py` if you need more attempts.
