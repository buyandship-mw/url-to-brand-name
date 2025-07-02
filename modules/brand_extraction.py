import csv
import json
import os
from typing import List

from .prompting import build_prompt
from .llm_client import prompt_model
from .extraction import _thread_map, _append_thread_csv, merge_thread_csvs

BRAND_FIELDNAMES: List[str] = [
    "month",
    "url",
    "item_count",
    "item_name",
    "image_url",
    "brand",
    "brand_error",
]


def process_row(row: dict, csv_prefix: str | None = None) -> dict:
    """Return brand extraction result for a single row."""
    month = row.get("month", "")
    url = row.get("url", "")
    item_count = row.get("item_count", "")
    image_url = row.get("image_url", "")

    item_name = row.get("item_name", "").strip()
    error = row.get("error", "").strip()
    if error:
        result = {
            "month": month,
            "url": url,
            "item_count": item_count,
            "item_name": item_name,
            "image_url": image_url,
            "brand": "",
            "brand_error": error,
        }
        if csv_prefix:
            _append_thread_csv(csv_prefix, BRAND_FIELDNAMES, result)
        return result

    input_text = item_name if item_name else url
    prompt = build_prompt(input_text)

    brand = ""
    brand_error = ""
    try:
        raw = prompt_model(prompt)
        data = json.loads(raw)
        brand = data.get("name", "")
    except Exception as e:  # noqa: BLE001
        brand_error = str(e)

    result = {
        "month": month,
        "url": url,
        "item_count": item_count,
        "item_name": item_name,
        "image_url": image_url,
        "brand": brand,
        "brand_error": brand_error,
    }

    if csv_prefix:
        _append_thread_csv(csv_prefix, BRAND_FIELDNAMES, result)

    return result


def batch_process(
    rows: list[dict],
    max_workers: int | None = None,
    csv_prefix: str | None = None,
    merge_path: str | None = None,
) -> list[dict]:
    """Process rows concurrently and return brand extraction results.

    When ``csv_prefix`` is provided, each worker will append to a per-thread CSV
    using this prefix. If ``merge_path`` is also given, those temporary CSVs are
    merged into ``merge_path`` once processing completes.
    """
    if max_workers is None:
        max_workers = os.cpu_count() or 1

    results = _thread_map(lambda r: process_row(r, csv_prefix), rows, max_workers)

    if csv_prefix and merge_path:
        merge_thread_csvs(csv_prefix, merge_path, BRAND_FIELDNAMES)

    return results
