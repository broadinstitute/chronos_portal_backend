"""
PoolQ Test Harness

Runs the PoolQ test suite from a JSON configuration file and returns
structured results (pass/fail per test, error messages).

Usage:
    python -m tests.poolq_harness [--tests tests.json] [--test name]
"""

import argparse
import asyncio
import json
import shutil
import sys
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Optional

import pandas as pd

# Add server to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))
from app.services.job_manager import job_manager
from app.services.poolq import process_reads_with_poolq

POOLQ_DIR = Path(__file__).parent.parent / "poolq-3.13.2"


@dataclass
class TestResult:
    """Result of a single PoolQ test."""
    name: str
    passed: bool
    error: Optional[str] = None
    job_id: Optional[str] = None


def load_tests(tests_json: Path) -> list[dict]:
    """Load and validate test definitions from JSON."""
    with open(tests_json) as f:
        data = json.load(f)

    required_fields = [
        "name", "readcounts", "readcount_format",
        "condition_map", "condition_map_format",
        "guide_map", "guide_map_format",
        "options", "expected_counts"
    ]

    for test in data["tests"]:
        missing = [f for f in required_fields if f not in test]
        if missing:
            raise ValueError(f"Test '{test.get('name', '?')}' missing fields: {missing}")

    return data["tests"]


def setup_test_job(test: dict) -> str:
    """Create and configure a test job. Returns job_id."""
    job_id = job_manager.create_job(f"Test{test['name']}")
    uploads_dir = job_manager.get_uploads_dir(job_id)

    # Copy condition_map
    src = POOLQ_DIR / test["condition_map"]
    dst = uploads_dir / src.name
    shutil.copy(src, dst)
    job_manager.store_file_path("condition_map", dst)
    job_manager.add_file_info("condition_map", src.name, test["condition_map_format"], dst)

    # Copy guide_map
    src = POOLQ_DIR / test["guide_map"]
    dst = uploads_dir / src.name
    shutil.copy(src, dst)
    job_manager.store_file_path("guide_map", dst)
    job_manager.add_file_info("guide_map", src.name, test["guide_map_format"], dst)

    # Copy readcount files
    job_manager.clear_readcount_files()
    for rc_path in test["readcounts"]:
        src = POOLQ_DIR / rc_path
        dst = uploads_dir / src.name
        shutil.copy(src, dst)
        job_manager.add_readcount_file(dst)

    # Set format and options
    job_manager.set_readcount_format(test["readcount_format"])
    job_manager.set_readcount_options(test["options"])

    return job_id


def compare_counts(actual_path: Path, expected_path: Path) -> tuple[bool, Optional[str]]:
    """Compare actual counts to expected. Returns (passed, error_message)."""
    actual = pd.read_csv(actual_path, sep="\t", index_col=0)
    expected = pd.read_csv(expected_path, sep="\t", index_col=0)

    # Drop non-count columns that PoolQ adds
    drop_cols = ["Row Barcode IDs", "Construct IDs"]
    actual = actual.drop(columns=[c for c in drop_cols if c in actual.columns], errors="ignore")
    expected = expected.drop(columns=[c for c in drop_cols if c in expected.columns], errors="ignore")

    if actual.equals(expected):
        return True, None

    # Generate diff info
    if actual.shape != expected.shape:
        return False, f"Shape mismatch: {actual.shape} vs {expected.shape}"

    diff_mask = actual != expected
    diff_count = diff_mask.sum().sum()
    return False, f"{diff_count} cells differ"


def cleanup_job(job_id: str):
    """Delete job directory and log file."""
    job_dir = job_manager.get_job_dir(job_id)
    log_path = job_manager.get_log_path(job_id)

    if job_dir.exists():
        shutil.rmtree(job_dir)
    if log_path.exists():
        log_path.unlink()


def run_single_test(test: dict) -> TestResult:
    """Run a single test and return result."""
    job_id = None
    try:
        # Setup
        job_id = setup_test_job(test)

        # Run PoolQ (sync wrapper around async function)
        asyncio.run(process_reads_with_poolq(job_id))

        # Compare results
        uploads_dir = job_manager.get_uploads_dir(job_id)
        actual_path = uploads_dir / "poolq_work" / "counts.txt"
        expected_path = POOLQ_DIR / test["expected_counts"]

        passed, error = compare_counts(actual_path, expected_path)

        if passed:
            cleanup_job(job_id)
            return TestResult(name=test["name"], passed=True)
        else:
            return TestResult(name=test["name"], passed=False, error=error, job_id=job_id)

    except Exception as e:
        return TestResult(name=test["name"], passed=False, error=str(e), job_id=job_id)


def run_poolq_tests(tests_json: Path, test_name: Optional[str] = None) -> list[TestResult]:
    """Run all tests (or a specific test) and return results."""
    tests = load_tests(tests_json)

    if test_name:
        tests = [t for t in tests if t["name"] == test_name]
        if not tests:
            raise ValueError(f"Test '{test_name}' not found")

    results = []
    for test in tests:
        print(f"Running test: {test['name']}...", file=sys.stderr)
        result = run_single_test(test)
        results.append(result)
        status = "PASS" if result.passed else "FAIL"
        print(f"  {status}", file=sys.stderr)

    return results


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(description="Run PoolQ integration tests")
    parser.add_argument(
        "--tests", type=Path,
        default=Path(__file__).parent / "poolq_tests.json",
        help="Path to test JSON file"
    )
    parser.add_argument("--test", type=str, help="Run specific test by name")
    args = parser.parse_args()

    results = run_poolq_tests(args.tests, args.test)

    # Output structured JSON
    output = {
        "total": len(results),
        "passed": sum(1 for r in results if r.passed),
        "failed": sum(1 for r in results if not r.passed),
        "results": [asdict(r) for r in results]
    }
    print(json.dumps(output, indent=2))

    # Exit with error code if any failed
    sys.exit(0 if output["failed"] == 0 else 1)


if __name__ == "__main__":
    main()
