# extraction.py

import os
import time
from dotenv import load_dotenv
from firecrawl import FirecrawlApp

load_dotenv()
API_KEY = os.getenv("FIRECRAWL_API_KEY")
if not API_KEY:
    raise EnvironmentError("FIRECRAWL_API_KEY not found")
APP = FirecrawlApp(api_key=API_KEY)

def parse_metadata(meta: dict) -> str:
    for key in ["og:title", "twitter:title", "title", "ogTitle", "name"]:
        v = meta.get(key)
        if isinstance(v, str) and v.strip():
            return v
    return None


def get_first(meta: dict, keys: list[str]):
    """Return the first non-empty value for the given keys."""
    for key in keys:
        v = meta.get(key)
        if isinstance(v, str) and v.strip():
            return v
    return None


def strip_query(url: str | None) -> str | None:
    """Remove the query string from a URL."""
    if not url:
        return url
    return url.split("?", 1)[0]


def parse_image_url(meta: dict) -> str | None:
    """Extract the best image URL from page metadata."""
    url = get_first(meta, ["og:image", "ogImage", "twitter:image:src", "image"])
    if url:
        return strip_query(url)
    return None

def fetch_metadata(url: str, timeout: int = 10000, retries: int = 0) -> dict:
    """Call Firecrawl to fetch page metadata with retry and log how long it took."""
    last_error = None
    for attempt in range(1, retries + 1):
        try:
            start = time.perf_counter()
            resp = APP.scrape_url(
                url=url,
                only_main_content=False,
                timeout=timeout,
            )
            duration = time.perf_counter() - start
            print(f"Firecrawl request for {url} took {duration:.2f} seconds")
            return resp.metadata
        except Exception as e:
            last_error = e
            if attempt < retries:
                print(f"Firecrawl error: {e}. Retrying ({attempt}/{retries})...")
            else:
                print(f"Firecrawl failed after {retries} attempts: {e}")
    raise RuntimeError(f"Firecrawl API error: {last_error}")

def extract_item_name(url: str) -> tuple[str, str | None]:
    """Return the item name and representative image URL for the given page."""
    meta = fetch_metadata(url)
    if "error" in meta:
        raise RuntimeError(meta["error"])
    name = parse_metadata(meta)
    if not name:
        raise ValueError("No valid item name found in metadata")
    image_url = parse_image_url(meta)
    return name, image_url
