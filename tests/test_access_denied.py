import os
import sys
import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

os.environ.setdefault("FIRECRAWL_API_KEY", "test")
os.environ.setdefault("AZURE_OPENAI_API_KEY", "test")
os.environ.setdefault("AZURE_OPENAI_ENDPOINT", "https://example.com/")
os.environ.setdefault("AZURE_OPENAI_DEPLOYMENT", "test")

import extract_brands as eb


def test_process_row_handles_access_denied(monkeypatch):
    row = {
        "month": "2025-05-01",
        "url": "http://example.com",
        "item_count": "18",
        "image_url": "",
        "item_name": "Access Denied",
    }

    def fail_prompt(*args, **kwargs):  # pragma: no cover - should not be called
        raise AssertionError("prompt_model should not be called")

    monkeypatch.setattr(eb, "prompt_model", fail_prompt)

    result = eb.process_row(row)

    assert result["brand"] == ""
    assert result["brand_error"] == "Access Denied"
