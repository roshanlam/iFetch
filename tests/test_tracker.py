import json
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from ifetch.tracker import DownloadTracker  # noqa: E402


def test_tracker_save_load_cleanup(tmp_path):
    file_path = tmp_path / "data.bin"
    file_path.touch()  # create empty file

    tracker = DownloadTracker(file_path)
    assert tracker.current_position == 0

    tracker.save_status(1234)
    assert tracker.current_position == 1234
    # Status file exists
    assert tracker.status_path.exists()

    # New instance should load position
    tracker2 = DownloadTracker(file_path)
    assert tracker2.current_position == 1234

    tracker2.cleanup()
    # Status file should be removed
    assert not tracker.status_path.exists() 