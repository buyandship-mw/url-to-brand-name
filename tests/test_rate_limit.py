import os
import sys
import pytest
import requests

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

os.environ.setdefault("FIRECRAWL_API_KEY", "test")

import modules.extraction as extraction


def test_fetch_metadata_waits_for_next_allowed_time(monkeypatch):
    extraction.NEXT_ALLOWED_TIME = 0.0
    calls = []

    class Resp:
        metadata = {}

    def fake_scrape_url(*args, **kwargs):
        if not calls:
            calls.append("first")
            resp = type("R", (), {"status_code": 429})()
            raise requests.exceptions.HTTPError(response=resp)
        calls.append("second")
        return Resp()

    monkeypatch.setattr(extraction.APP, "scrape_url", fake_scrape_url)

    sleep_calls = []
    monkeypatch.setattr(extraction.time, "sleep", lambda s: sleep_calls.append(s))

    with pytest.raises(RuntimeError):
        extraction.fetch_metadata("http://example.com", retries=0)

    extraction.fetch_metadata("http://example.com", retries=0)

    assert sleep_calls[-1] == pytest.approx(60.0, abs=0.01)
