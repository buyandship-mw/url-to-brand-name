# llm_client.py

import os
import time
from dotenv import load_dotenv
from openai import OpenAI, RateLimitError

load_dotenv()
_client = OpenAI(
    api_key=os.getenv("AZURE_OPENAI_API_KEY"),
    base_url=os.getenv("AZURE_OPENAI_ENDPOINT") + "openai/v1/",
    default_query={"api-version": "preview"},
)

# Client for regular OpenAI API (gpt-4.1 URL extraction)
_openai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY", "test"))


def prompt_model(prompt: str, timeout: int = 3, retries: int = 3) -> str:
    """Send a prompt to OpenAI with retry logic and report the request duration."""
    last_error = None
    for attempt in range(retries + 1):
        try:
            start = time.perf_counter()
            resp = _client.responses.create(
                model=os.getenv("AZURE_OPENAI_DEPLOYMENT"),
                input=prompt,
                timeout=timeout,
            )
            duration = time.perf_counter() - start
            print(f"OpenAI request took {duration:.2f} seconds")
            return resp.output[0].content[0].text
        except RateLimitError as e:
            last_error = e
            wait = min(2**attempt, 60)
            print(f"OpenAI rate limit hit. Sleeping for {wait} seconds")
            time.sleep(wait)
            if attempt < retries:
                continue
            print(f"OpenAI failed after {retries} attempts: {e}")
        except Exception as e:
            last_error = e
            print(f"OpenAI failed: {e}")
            break
    raise RuntimeError(f"OpenAI API error: {last_error}")


def prompt_openai(prompt: str, timeout: int = 3, retries: int = 3, model: str = "gpt-4.1") -> str:
    """Call the public OpenAI API with retry logic."""
    last_error = None
    for attempt in range(retries + 1):
        try:
            start = time.perf_counter()
            resp = _openai_client.responses.create(
                model=model,
                input=prompt,
                timeout=timeout,
            )
            duration = time.perf_counter() - start
            print(f"OpenAI request took {duration:.2f} seconds")
            return resp.output[0].content[0].text
        except RateLimitError as e:
            last_error = e
            wait = min(2**attempt, 60)
            print(f"OpenAI rate limit hit. Sleeping for {wait} seconds")
            time.sleep(wait)
            if attempt < retries:
                continue
            print(f"OpenAI failed after {retries} attempts: {e}")
        except Exception as e:
            last_error = e
            print(f"OpenAI failed: {e}")
            break
    raise RuntimeError(f"OpenAI API error: {last_error}")
