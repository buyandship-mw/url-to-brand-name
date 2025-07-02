import os
import sys
import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

os.environ.setdefault("FIRECRAWL_API_KEY", "test")
os.environ.setdefault("AZURE_OPENAI_API_KEY", "test")
os.environ.setdefault("AZURE_OPENAI_ENDPOINT", "https://example.com/")
os.environ.setdefault("AZURE_OPENAI_DEPLOYMENT", "test")

import extract_brands as eb
import modules.extraction as extraction


def test_access_denied_error_propagates(monkeypatch):
    row = {
        "month": "2025-05-01",
        "url": "http://example.com",
        "item_count": "18",
    }

    monkeypatch.setattr(
        extraction,
        "fetch_metadata",
        lambda *args, **kwargs: {"title": "Access Denied"},
    )

    results = extraction.batch_extract([row], max_workers=1)
    item_row = results[0]
    assert item_row["item_name"] == ""
    assert item_row["error"] == "Access Denied"

    def fail_prompt(*args, **kwargs):  # pragma: no cover - should not be called
        raise AssertionError("prompt_model should not be called")

    monkeypatch.setattr(eb, "prompt_model", fail_prompt)

    brand_row = eb.process_row(item_row)

    assert brand_row["brand"] == ""
    assert brand_row["brand_error"] == "Access Denied"
