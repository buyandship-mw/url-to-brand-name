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
        except Exception as e:
            last_error = e
            if isinstance(e, RateLimitError):
                wait = min(2 ** attempt, 60)
                print(f"OpenAI rate limit hit. Sleeping for {wait} seconds")
                time.sleep(wait)
            if attempt < retries:
                print(f"OpenAI error: {e}. Retrying ({attempt + 1}/{retries})...")
            else:
                print(f"OpenAI failed after {retries} attempts: {e}")
    raise RuntimeError(f"OpenAI API error: {last_error}")
