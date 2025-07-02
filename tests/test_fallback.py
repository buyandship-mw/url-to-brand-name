import os
import sys
import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

os.environ.setdefault("FIRECRAWL_API_KEY", "test")

import modules.extraction as extraction


def test_fallback_item_name_used(monkeypatch):
    row = {
        "month": "2025-06-01",
        "url": "http://example.com",
        "item_count": "1",
        "item_name": "Fallback Name",
    }

    def fail_extract(*args, **kwargs):
        raise Exception("fail")

    monkeypatch.setattr(extraction, "extract_item_data", fail_extract)

    results = extraction.batch_extract([row], max_workers=1)
    item_row = results[0]

    assert item_row["item_name"] == "Fallback Name"
    assert item_row["error"] == "fail"
    assert item_row["used_fallback"] is True

