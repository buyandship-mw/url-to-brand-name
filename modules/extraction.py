# extraction.py

import os
import time
import re
import csv
import threading
from dotenv import load_dotenv
from firecrawl import FirecrawlApp
import requests

load_dotenv()
API_KEY = os.getenv("FIRECRAWL_API_KEY")
if not API_KEY:
    raise EnvironmentError("FIRECRAWL_API_KEY not found")
APP = FirecrawlApp(api_key=API_KEY)

# Global rate limit state shared across threads
RATE_LIMIT_LOCK = threading.Lock()
NEXT_ALLOWED_TIME = 0.0


def parse_metadata(meta: dict) -> str:
    """Return the best item name from metadata."""
    for key in ["og:title", "twitter:title", "title", "ogTitle", "name"]:
        v = meta.get(key)
        if isinstance(v, str) and v.strip():
            return v
    return None


def parse_image_url(meta: dict) -> str | None:
    """Return an image URL from metadata if available without query parameters."""
    for key in ["og:image", "ogImage", "twitter:image:src", "image"]:
        v = meta.get(key)
        if isinstance(v, str) and v.strip():
            return v.split("?", 1)[0]
    return None


def fetch_metadata(url: str, timeout: int = 20000, retries: int = 2) -> dict:
    """Call Firecrawl to fetch page metadata with retry and log how long it took."""
    global NEXT_ALLOWED_TIME
    last_error = None
    for attempt in range(retries + 1):
        RATE_LIMIT_LOCK.acquire()
        try:
            delay = NEXT_ALLOWED_TIME - time.time()
            if delay > 0:
                time.sleep(delay)
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
            RATE_LIMIT_LOCK.release()
            return meta
        except requests.exceptions.HTTPError as e:
            last_error = e
            if getattr(e.response, "status_code", None) == 429:
                match = re.search(r"retry after (\d+)s", str(e), re.I)
                if match:
                    wait = int(match.group(1))
                else:
                    wait = 60
                print(f"Firecrawl rate limit hit. Sleeping for {wait} seconds")
                NEXT_ALLOWED_TIME = time.time() + wait
                RATE_LIMIT_LOCK.release()
                time.sleep(wait)
                if attempt < retries:
                    continue
            else:
                RATE_LIMIT_LOCK.release()
            print(f"Firecrawl failed: {e}")
            break
        except Exception as e:
            last_error = e
            RATE_LIMIT_LOCK.release()
            print(f"Firecrawl failed: {e}")
            break
    raise RuntimeError(f"Firecrawl API error: {last_error}")


def extract_item_data(url: str) -> tuple[str, str | None]:
    """Return item name and image URL for a given page."""
    meta = fetch_metadata(url)
    name = parse_metadata(meta)
    if name and "access denied" in name.lower():
        raise ValueError("Access Denied")
    if not name:
        raise ValueError("No valid item name found in metadata")
    image_url = parse_image_url(meta)
    return name, image_url


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
    from concurrent.futures import ThreadPoolExecutor

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
        month = row.get("month", "")
        url = row.get("item_url") or row.get("url", "")
        item_count = row.get("item_count", "")
        original_item_name = row.get("item_name", "")
        item_name = ""
        image_url = ""
        error = ""
        used_fallback = False
        print(f"Processing URL: {url}")
        try:
            item_name, image_url = extract_item_data(url)
        except Exception as e:  # noqa: BLE001
            error = str(e)
            item_name = original_item_name
            used_fallback = bool(item_name)

        return {
            "month": month,
            "url": url,
            "item_count": item_count,
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
