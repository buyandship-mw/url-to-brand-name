import os
import sys
import json

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

os.environ.setdefault("FIRECRAWL_API_KEY", "test")
os.environ.setdefault("AZURE_OPENAI_API_KEY", "test")
os.environ.setdefault("AZURE_OPENAI_ENDPOINT", "https://example.com/")
os.environ.setdefault("AZURE_OPENAI_DEPLOYMENT", "test")

import extract_brands as eb


def test_brand_name_cleanup(monkeypatch):
    row = {
        "month": "2025-07-01",
        "url": "http://example.com",
        "item_count": "1",
        "item_name": "Some Item",
        "image_url": "",
    }

    monkeypatch.setattr(eb, "prompt_model", lambda *a, **k: json.dumps({"name": "mega-brand_name"}))

    result = eb.process_row(row)
    assert result["brand"] == "MEGA BRAND NAME"
