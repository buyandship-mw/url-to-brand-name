import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

os.environ.setdefault("FIRECRAWL_API_KEY", "test")

import modules.extraction as extraction


def test_thread_files_removed(tmp_path):
    tmp_dir = tmp_path / "tmp"
    out_csv = tmp_path / "out.csv"

    def worker(x):
        return {"value": x}

    extraction._thread_map(
        worker,
        [1, 2, 3],
        max_workers=2,
        fieldnames=["value"],
        final_csv=str(out_csv),
        tmp_dir=str(tmp_dir),
    )

    assert out_csv.exists()
    assert not tmp_dir.exists()

