import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

os.environ.setdefault("FIRECRAWL_API_KEY", "test")

from modules.extraction import parse_image_url


def test_parse_image_url_strips_query_string():
    meta = {"og:image": "https://example.com/img.jpg?width=100&height=100"}
    assert parse_image_url(meta) == "https://example.com/img.jpg"

