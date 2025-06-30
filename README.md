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

```bash
python extract_names.py --start 1 --end 100
```

This reads `data/input.csv`, fetches metadata with Firecrawl and writes the results to `data/output/item_names.csv`.
Use `--start` and `--end` to limit which rows are processed (both inclusive).

2. **Extract brand names**

```bash
python extract_brands.py --start 1 --end 100
```

This reads the item names file produced above and writes `data/output/brands.csv`.
Again, `--start` and `--end` let you select a subset of rows.

## Notes

Both API helper functions include retry logic and will back off when a `429` rate limit
response is received. They now retry up to **four** times by default. Adjust the
`retries` parameter in `modules/extraction.py` and `modules/llm_client.py` if you need
more attempts.
