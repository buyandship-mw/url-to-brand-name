import os
import sys
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


def test_fetch_metadata_attempts_once(monkeypatch):
    calls = []
    def fake_scrape_url(*args, **kwargs):
        calls.append(True)
        raise Exception("fail")
    monkeypatch.setattr(extraction.APP, "scrape_url", fake_scrape_url)
    with pytest.raises(RuntimeError):
        extraction.fetch_metadata("http://example.com", retries=0)
    assert len(calls) == 1


def test_retry_after_seconds_parsed(monkeypatch):
    resp = requests.Response()
    resp.status_code = 429
    message = (
        "Unexpected error during scrape URL: Status code 429. Rate limit exceeded. "
        "please retry after 7s, resets at Mon Jun 30 2025 08:48:45 GMT+0000 (Coordinated Universal Time)"
    )
    err = requests.exceptions.HTTPError(message, response=resp)

    sleeps = []

    def fake_scrape_url(*args, **kwargs):
        raise err

    monkeypatch.setattr(extraction.APP, "scrape_url", fake_scrape_url)
    monkeypatch.setattr(extraction.time, "sleep", lambda s: sleeps.append(s))

    with pytest.raises(RuntimeError):
        extraction.fetch_metadata("http://example.com", retries=0)

    assert sleeps == [7]


def test_prompt_model_attempts_once(monkeypatch):
    calls = []
    def fake_create(*args, **kwargs):
        calls.append(True)
        raise Exception("fail")
    monkeypatch.setattr(llm_client._client.responses, "create", fake_create)
    with pytest.raises(RuntimeError):
        llm_client.prompt_model("hi", retries=0)
    assert len(calls) == 1
