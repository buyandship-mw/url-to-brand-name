# extraction.py

import os
import time
from dotenv import load_dotenv
from firecrawl import FirecrawlApp
import requests

load_dotenv()
API_KEY = os.getenv("FIRECRAWL_API_KEY")
if not API_KEY:
    raise EnvironmentError("FIRECRAWL_API_KEY not found")
APP = FirecrawlApp(api_key=API_KEY)

def parse_metadata(meta: dict) -> str:
    """Return the best item name from metadata."""
    for key in ["og:title", "twitter:title", "title", "ogTitle", "name"]:
        v = meta.get(key)
        if isinstance(v, str) and v.strip():
            return v
    return None


def parse_image_url(meta: dict) -> str | None:
    """Return an image URL from metadata if available."""
    for key in ["og:image", "ogImage", "twitter:image:src", "image"]:
        v = meta.get(key)
        if isinstance(v, str) and v.strip():
            return v
    return None

def fetch_metadata(url: str, timeout: int = 20000, retries: int = 2) -> dict:
    """Call Firecrawl to fetch page metadata with retry and log how long it took."""
    last_error = None
    for attempt in range(retries + 1):
        try:
            start = time.perf_counter()
            resp = APP.scrape_url(
                url=url,
                only_main_content=False,
                timeout=timeout,
                proxy="auto",
            )
            duration = time.perf_counter() - start
            print(f"Firecrawl request for {url} took {duration:.2f} seconds")
            return resp.metadata
        except Exception as e:
            last_error = e
            # Handle rate limit errors (HTTP 429)
            if isinstance(e, requests.exceptions.HTTPError) and getattr(e.response, "status_code", None) == 429:
                wait = min(2 ** attempt, 60)
                print(f"Firecrawl rate limit hit. Sleeping for {wait} seconds")
                time.sleep(wait)
            if attempt < retries:
                print(f"Firecrawl error: {e}. Retrying ({attempt + 1}/{retries})...")
            else:
                print(f"Firecrawl failed after {retries} attempts: {e}")
    raise RuntimeError(f"Firecrawl API error: {last_error}")

def extract_item_data(url: str) -> tuple[str, str | None]:
    """Return item name and image URL for a given page."""
    meta = fetch_metadata(url)
    if "error" in meta:
        raise RuntimeError(meta["error"])
    name = parse_metadata(meta)
    if not name:
        raise ValueError("No valid item name found in metadata")
    image_url = parse_image_url(meta)
    return name, image_url


def extract_item_name(url: str) -> str:
    """Backward compatible wrapper that only returns the item name."""
    name, _ = extract_item_data(url)
    return name