"""Fast smoke test for the offline FakeNode benchmark harness."""

from pathlib import Path

from benchmarks.offline_benchmark import (
    _build_nodes,
    run_cold,
    run_resume,
    run_warm,
)


def test_offline_harness_tiny_fixture(tmp_path):
    nodes = _build_nodes(4, 4 * 1024)
    cold_dir = tmp_path / "cold"
    resume_dir = tmp_path / "resume"

    cold = run_cold(nodes, cold_dir)
    assert cold["successful"] == 4
    assert cold["bytes_transferred"] == 4 * 4 * 1024

    opens_after_cold = sum(n.open_calls for n in nodes)
    warm = run_warm(nodes, cold_dir)
    warm_opens = sum(n.open_calls for n in nodes) - opens_after_cold
    assert warm["bytes_transferred"] == 0
    assert warm_opens == 0

    resume = run_resume(nodes, resume_dir)
    assert resume["mismatches"] == 0
