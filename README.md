# URL to Brand Name

This project extracts product names and brand names from a list of URLs.

## Setup

1. Install the required packages:

```bash
pip install -r requirements.txt
```

2. Copy `.env.sample` to `.env` and fill in the API keys:

```
FIRECRAWL_API_KEY=<your-firecrawl-key>
AZURE_OPENAI_API_KEY=<your-openai-key>
AZURE_OPENAI_ENDPOINT=<https://your-resource.azure.com/>
AZURE_OPENAI_DEPLOYMENT=<deployment-name>
AZURE_OPENAI_API_VERSION=<api-version>
```

3. Prepare `data/input.csv` with the columns `month`, `item_url` and `item_count`.

## Running `extract_names.py`

Run the script to fetch item names from the URLs:

```bash
python extract_names.py
```

It will create `data/output/item_names.csv`.

### Parallel execution

You can split the input and run several workers in parallel. For example, using GNU
`parallel`:

```bash
split -l 1000 data/input.csv part_
ls part_* | parallel --halt soon,fail=1 'mkdir -p worker_{#}/data && \
  cp {} worker_{#}/data/input.csv && \
  (cd worker_{#} && python ../extract_names.py)'
```

Each worker directory now contains its own `data/output/item_names.csv`.

## Running `resolve_brands.py`

After `extract_names.py`, run the brand resolver:

```bash
python resolve_brands.py
```

It will read `data/output/item_names.csv` and produce `data/output/brands.csv`.
The same parallel strategy can be applied here by running the script in each
worker directory.

## Merging worker results

Combine the CSV files from multiple workers to create a single output. Example
for item names:

```bash
(head -n 1 worker_1/data/output/item_names.csv && \
  tail -n +2 -q worker_*/data/output/item_names.csv) > data/output/item_names.csv
```

Repeat the process for `brands.csv`. The first file provides the header, while
the rest contribute rows. The final merged files can then be used for further
analysis.
