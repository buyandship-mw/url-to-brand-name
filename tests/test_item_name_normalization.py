import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

os.environ.setdefault("FIRECRAWL_API_KEY", "test")

import modules.extraction as extraction


def test_item_name_newlines_removed(monkeypatch):
    row = {
        "month": "2025-06-01",
        "url": "http://example.com",
        "item_count": "1",
    }

    def fake_extract_item_data(url):
        return ("\nHello\r\nWorld\t", "http://img.com")

    monkeypatch.setattr(extraction, "extract_item_data", fake_extract_item_data)

    results = extraction.batch_extract([row], max_workers=1)
    assert results[0]["item_name"] == "Hello World"
