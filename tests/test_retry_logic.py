import os
import sys
import json
import pytest
import requests

# Ensure repository root is on path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Set environment variables so module imports succeed without real credentials
os.environ.setdefault("FIRECRAWL_API_KEY", "test")
os.environ.setdefault("AZURE_OPENAI_API_KEY", "test")
os.environ.setdefault("AZURE_OPENAI_ENDPOINT", "https://example.com/")
os.environ.setdefault("AZURE_OPENAI_DEPLOYMENT", "test")

import modules.extraction as extraction
import modules.llm_client as llm_client
import extract_brands as eb


def test_fetch_metadata_attempts_once(monkeypatch):
    calls = []

    def fake_scrape_url(*args, **kwargs):
        calls.append(True)
        raise Exception("fail")

    monkeypatch.setattr(extraction.APP, "scrape_url", fake_scrape_url)
    with pytest.raises(RuntimeError):
        extraction.fetch_metadata("http://example.com", retries=2)
    assert len(calls) == 1


def test_prompt_model_attempts_once(monkeypatch):
    calls = []

    def fake_create(*args, **kwargs):
        calls.append(True)
        raise Exception("fail")

    monkeypatch.setattr(llm_client._client.responses, "create", fake_create)
    with pytest.raises(RuntimeError):
        llm_client.prompt_model("hi", retries=2)
    assert len(calls) == 1


def test_fetch_metadata_does_not_retry_on_api_error(monkeypatch):
    class Resp:
        def __init__(self):
            self.metadata = {"error": "bad"}

    calls = []

    def fake_scrape_url(*args, **kwargs):
        calls.append(True)
        return Resp()

    monkeypatch.setattr(extraction.APP, "scrape_url", fake_scrape_url)
    with pytest.raises(RuntimeError):
        extraction.fetch_metadata("http://example.com")
    assert len(calls) == 1


def test_prompt_model_retries_on_rate_limit(monkeypatch):
    class FakeRateLimitError(Exception):
        pass

    monkeypatch.setattr(llm_client, "RateLimitError", FakeRateLimitError)

    calls = []

    def fake_create(*args, **kwargs):
        calls.append(True)
        raise FakeRateLimitError("429")

    sleeps = []
    monkeypatch.setattr(llm_client._client.responses, "create", fake_create)
    monkeypatch.setattr(llm_client.time, "sleep", lambda s: sleeps.append(s))

    with pytest.raises(RuntimeError):
        llm_client.prompt_model("hi", retries=1)

    assert len(calls) == 2
    assert sleeps


def test_fetch_metadata_parses_retry_after_message(monkeypatch):
    class FakeHTTPError(requests.exceptions.HTTPError):
        def __init__(self, message):
            super().__init__(message)
            self.response = type("R", (), {"status_code": 429})()

    def fake_scrape_url(*args, **kwargs):
        raise FakeHTTPError("Too many requests, retry after 7s")

    sleeps = []
    monkeypatch.setattr(extraction.APP, "scrape_url", fake_scrape_url)
    monkeypatch.setattr(extraction.time, "sleep", lambda s: sleeps.append(s))

    with pytest.raises(RuntimeError):
        extraction.fetch_metadata("http://example.com", retries=0)

    assert sleeps == [7]


def test_process_row_retries_on_json_decode_error(monkeypatch):
    row = {
        "month": "2025-08-01",
        "url": "http://example.com",
        "item_count": "1",
        "item_name": "Some Item",
        "image_url": "",
    }

    calls = []

    def fake_prompt_model(*args, **kwargs):
        calls.append(True)
        if len(calls) == 1:
            return "bad json"
        return json.dumps({"name": "BrandX"})

    monkeypatch.setattr(eb, "prompt_model", fake_prompt_model)

    result = eb.process_row(row)

    assert len(calls) == 2
    assert result["brand"] == "Brandx"
    assert result["brand_error"] == ""
