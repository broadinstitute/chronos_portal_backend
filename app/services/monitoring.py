"""Memory monitoring for production diagnostics."""

import json
import os
from datetime import datetime
from pathlib import Path

import psutil


def log_memory(logs_dir: Path, job_id: str, phase: str):
    """Log memory snapshot to memory.jsonl.

    Args:
        logs_dir: Directory containing log files
        job_id: Current job identifier
        phase: Operation phase (e.g., "chronos_start", "qc_complete")
    """
    process = psutil.Process(os.getpid())
    mem_info = process.memory_info()
    system_mem = psutil.virtual_memory()

    entry = {
        "ts": datetime.now().isoformat(),
        "job_id": job_id,
        "phase": phase,
        "rss_mb": round(mem_info.rss / 1024 / 1024, 1),
        "system_avail_mb": round(system_mem.available / 1024 / 1024, 1),
        "system_percent": system_mem.percent,
    }

    log_path = logs_dir / "memory.jsonl"
    with open(log_path, "a") as f:
        f.write(json.dumps(entry) + "\n")


def get_current_memory():
    """Get current memory stats for live endpoint."""
    process = psutil.Process(os.getpid())
    mem = process.memory_info()
    sys_mem = psutil.virtual_memory()
    return {
        "rss_mb": round(mem.rss / 1024 / 1024, 1),
        "system_avail_mb": round(sys_mem.available / 1024 / 1024, 1),
        "system_percent": sys_mem.percent,
    }
