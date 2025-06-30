import os
import sys
import pytest

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


def test_prompt_model_attempts_once(monkeypatch):
    calls = []
    def fake_create(*args, **kwargs):
        calls.append(True)
        raise Exception("fail")
    monkeypatch.setattr(llm_client._client.responses, "create", fake_create)
    with pytest.raises(RuntimeError):
        llm_client.prompt_model("hi", retries=0)
    assert len(calls) == 1


def test_fetch_metadata_retries_on_api_error(monkeypatch):
    class Resp:
        def __init__(self):
            self.metadata = {"error": "bad"}

    calls = []

    def fake_scrape_url(*args, **kwargs):
        calls.append(True)
        return Resp()

    monkeypatch.setattr(extraction.APP, "scrape_url", fake_scrape_url)
    with pytest.raises(RuntimeError):
        extraction.fetch_metadata("http://example.com", retries=2)
    assert len(calls) == 3
