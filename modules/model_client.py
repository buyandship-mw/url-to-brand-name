# model_client.py

import os
import time
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()
_client = OpenAI(
    api_key=os.getenv("AZURE_OPENAI_API_KEY"),
    base_url=os.getenv("AZURE_OPENAI_ENDPOINT") + "openai/v1/",
    default_query={"api-version": "preview"},
)

def prompt_model(prompt: str) -> str:
    """Send a prompt to OpenAI and report how long the request took."""
    try:
        start = time.perf_counter()
        resp = _client.responses.create(
            model=os.getenv("AZURE_OPENAI_DEPLOYMENT"),
            input=prompt
        )
        duration = time.perf_counter() - start
        print(f"OpenAI request took {duration:.2f} seconds")
        return resp.output[0].content[0].text
    except Exception as e:
        raise RuntimeError(f"OpenAI API error: {e}")
