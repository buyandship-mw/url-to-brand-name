# extraction.py

import os
import time
import re
import csv
import threading
from dotenv import load_dotenv
from firecrawl import FirecrawlApp
import requests
from concurrent.futures import ThreadPoolExecutor

# Load environment variables from a .env file
load_dotenv()

# --- Configuration and Initialization ---
API_KEY = os.getenv("FIRECRAWL_API_KEY")
if not API_KEY:
    raise EnvironmentError("FIRECRAWL_API_KEY not found in environment variables or .env file")
APP = FirecrawlApp(api_key=API_KEY)

# --- Global Concurrency Control State ---
# A lock to protect access to the shared rate limit timestamp
RATE_LIMIT_LOCK = threading.Lock()
# The timestamp (from time.time()) after which the next request is allowed
NEXT_ALLOWED_TIME = 0.0


def parse_metadata(meta: dict) -> str | None:
    """Return the best item name from metadata."""
    # A list of potential keys for the item title, in order of preference
    for key in ["og:title", "twitter:title", "title", "ogTitle", "name"]:
        v = meta.get(key)
        if isinstance(v, str) and v.strip():
            return v
    return None


def parse_image_url(meta: dict) -> str | None:
    """Return an image URL from metadata if available, stripping query parameters."""
    # A list of potential keys for the item image
    for key in ["og:image", "ogImage", "twitter:image:src", "image"]:
        v = meta.get(key)
        if isinstance(v, str) and v.strip():
            # Return the URL part before any '?' query parameters
            return v.split("?", 1)[0]
    return None


def _normalize_whitespace(text: str) -> str:
    """Return ``text`` collapsed into a single line."""
    return " ".join(text.split()) if text else ""


def fetch_metadata(url: str, timeout: int = 20000, retries: int = 2) -> dict:
    """
    Call Firecrawl to fetch page metadata with concurrent-safe rate limiting.

    This function allows multiple threads to call it concurrently. If one thread
    hits a rate limit, it sets a global wait time. All other threads will then
    pause before their next request to respect this limit.
    """
    global NEXT_ALLOWED_TIME
    last_error = None
    for attempt in range(retries + 1):
        # --- Step 1: Check and wait if a rate limit is active ---
        # The lock is held only for a moment to get a consistent value.
        with RATE_LIMIT_LOCK:
            delay = NEXT_ALLOWED_TIME - time.time()

        if delay > 0:
            print(f"Rate limit active. Thread for {url} waiting {delay:.2f} seconds.")
            time.sleep(delay)

        # --- Step 2: Perform the API call (outside the lock) ---
        try:
            start = time.perf_counter()
            resp = APP.scrape_url(
                url=url,
                only_main_content=False,
                timeout=timeout,
                proxy="basic",
            )
            duration = time.perf_counter() - start
            print(f"Firecrawl request for {url} took {duration:.2f} seconds")
            meta = resp.metadata
            if "error" in meta:
                raise RuntimeError(meta["error"])
            
            # On success, return the metadata and exit the function
            return meta

        except requests.exceptions.HTTPError as e:
            last_error = e
            # --- Step 3: Handle Rate Limit Error (HTTP 429) ---
            if getattr(e.response, "status_code", None) == 429:
                # Extract wait time from the error message, default to 60s
                match = re.search(r"retry after (\d+)s", str(e), re.I)
                wait = int(match.group(1)) if match else 60
                
                print(f"Firecrawl rate limit hit for {url}. Setting wait time for {wait} seconds.")

                # --- Atomically update the shared NEXT_ALLOWED_TIME ---
                # The lock prevents a race condition where two threads overwrite the
                # wait time with a shorter duration.
                with RATE_LIMIT_LOCK:
                    new_next_allowed_time = time.time() + wait
                    NEXT_ALLOWED_TIME = max(NEXT_ALLOWED_TIME, new_next_allowed_time)
                
                # If we have retries left, continue to the next loop iteration.
                # The check at the top of the loop will now handle the sleep.
                if attempt < retries:
                    print(f"Retrying for {url} after wait.")
                    continue
                else:
                    print(f"Rate limit hit on final attempt for {url}.")
                    # Fall through to the final error raise

            else:
                # For other HTTP errors (e.g., 404, 500), log and stop retrying.
                print(f"Firecrawl failed for {url} with HTTPError: {e}")
                break
        
        except Exception as e:
            last_error = e
            print(f"Firecrawl failed for {url} with an unexpected error: {e}")
            break # Stop retrying on other unexpected errors

    # If the loop completes without a successful return, raise an error.
    raise RuntimeError(f"Firecrawl API failed for {url}. Last error: {last_error}")


def extract_item_data(url: str) -> tuple[str, str | None]:
    """Return item name and image URL for a given page."""
    meta = fetch_metadata(url)
    name = parse_metadata(meta)
    
    if name and "access denied" in name.lower():
        raise ValueError(f"Access Denied for URL: {url}")
    if not name:
        raise ValueError(f"No valid item name found in metadata for URL: {url}")
        
    image_url = parse_image_url(meta)
    return _normalize_whitespace(name), image_url


def extract_item_name(url: str) -> str:
    """Backward compatible wrapper that only returns the item name."""
    name, _ = extract_item_data(url)
    return name


def _append_to_csv(path: str, row: dict, fieldnames: list[str]):
    """Append a single row to a CSV file writing headers if needed."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    write_header = not os.path.exists(path)
    with open(path, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()
        writer.writerow(row)


def _thread_map(
    fn,
    items,
    max_workers: int = 2,
    *,
    fieldnames: list[str] | None = None,
    final_csv: str | None = None,
    tmp_dir: str | None = None,
):
    """Run tasks in a thread pool with optional CSV output."""
    results = []
    thread_files: dict[int, str] = {}
    if final_csv:
        tmp_dir = tmp_dir or os.path.join(os.path.dirname(final_csv), "tmp")
        os.makedirs(tmp_dir, exist_ok=True)

    def wrapper(item):
        res = fn(item)
        if final_csv:
            tid = threading.get_ident()
            path = thread_files.get(tid)
            if not path:
                path = os.path.join(tmp_dir, f"thread_{tid}.csv")
                thread_files[tid] = path
            _append_to_csv(path, res, fieldnames or list(res.keys()))
        return res

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(wrapper, item) for item in items]
        for fut in futures:
            # .result() will re-raise exceptions from the worker threads
            results.append(fut.result())

    if final_csv:
        write_header = not os.path.exists(final_csv)
        with open(final_csv, "a", newline="") as fout:
            writer = csv.DictWriter(fout, fieldnames=fieldnames)
            if write_header:
                writer.writeheader()
            for path in thread_files.values():
                with open(path, newline="") as fin:
                    for row in csv.DictReader(fin):
                        writer.writerow(row)
                os.remove(path)
        if tmp_dir:
            try:
                os.rmdir(tmp_dir)
            except OSError:
                pass

    return results


def batch_extract(
    rows: list[dict],
    max_workers: int = 2,
    *,
    final_csv: str | None = None,
    tmp_dir: str | None = None,
    fieldnames: list[str] | None = None,
) -> list[dict]:
    """Extract item names for multiple rows concurrently."""

    def _worker(row: dict) -> dict:
        # Safely get values from the input row dictionary
        url = row.get("item_url") or row.get("url", "")
        original_item_name = row.get("item_name", "")
        
        # Ensure there's a URL to process
        if not url:
            print("Skipping row with no URL.")
            return {**row, "error": "Missing URL", "image_url": ""}

        print(f"Processing URL: {url}")
        try:
            item_name, image_url = extract_item_data(url)
            error = ""
            used_fallback = False
        except Exception as e:
            print(f"Could not extract data for {url}. Error: {e}")
            error = str(e)
            item_name = original_item_name  # Fallback to original name on error
            image_url = ""
            used_fallback = bool(item_name)

        item_name = _normalize_whitespace(item_name)

        # Return a new dictionary with the extracted data
        return {
            "month": row.get("month", ""),
            "url": url,
            "item_count": row.get("item_count", ""),
            "image_url": image_url,
            "item_name": item_name,
            "error": error,
            "used_fallback": used_fallback,
        }

    return _thread_map(
        _worker,
        rows,
        max_workers,
        fieldnames=fieldnames
        or [
            "month",
            "url",
            "item_count",
            "image_url",
            "item_name",
            "error",
            "used_fallback",
        ],
        final_csv=final_csv,
        tmp_dir=tmp_dir,
    )