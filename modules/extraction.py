# extraction.py

import os
import time
import re
import csv
import tempfile
import threading
from pathlib import Path
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


def write_thread_csv_row(row: dict, fieldnames: list[str], tmp_dir: Path) -> None:
    """Append a row to a CSV specific to the current thread."""
    tmp_dir.mkdir(parents=True, exist_ok=True)
    path = tmp_dir / f"thread_{threading.get_ident()}.csv"
    write_header = not path.exists()
    with path.open("a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()
        writer.writerow(row)


def merge_thread_csvs(tmp_dir: Path, output_path: Path, fieldnames: list[str]) -> None:
    """Merge thread CSVs into one file and remove the temporary files."""
    with output_path.open("w", newline="") as f_out:
        writer = csv.DictWriter(f_out, fieldnames=fieldnames)
        writer.writeheader()
        for thread_file in sorted(tmp_dir.glob("thread_*.csv")):
            with thread_file.open(newline="") as f_in:
                for row in csv.DictReader(f_in):
                    writer.writerow(row)
            thread_file.unlink()
    tmp_dir.rmdir()


def _thread_map(fn, items, max_workers: int = 2):
    """Run tasks in a thread pool and return all results."""
    from concurrent.futures import ThreadPoolExecutor

    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(fn, item) for item in items]
        for fut in futures:
            results.append(fut.result())
    return results


def batch_extract(
    rows: list[dict],
    max_workers: int = 2,
    *,
    tmp_dir: Path | None = None,
    fieldnames: list[str] | None = None,
) -> list[dict]:
    """Extract item names for multiple rows concurrently."""

    if fieldnames is not None:
        if tmp_dir is None:
            tmp_dir = Path(tempfile.mkdtemp())
    else:
        fieldnames = []

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

        row_data = {
            "month": month,
            "url": url,
            "item_count": item_count,
            "image_url": image_url,
            "item_name": item_name,
            "error": error,
            "used_fallback": used_fallback,
        }
        if fieldnames:
            write_thread_csv_row(row_data, fieldnames, tmp_dir)
        return row_data

    results = _thread_map(_worker, rows, max_workers)
    return results
