import asyncio
import json
import shutil
from datetime import datetime, timedelta
from pathlib import Path


async def cleanup_memory_log(logs_dir: Path):
    """Remove memory.jsonl entries older than 60 days."""
    log_path = logs_dir / "memory.jsonl"
    if not log_path.exists():
        return

    cutoff = datetime.now() - timedelta(days=60)
    kept = []

    with open(log_path, "r") as f:
        for line in f:
            try:
                entry = json.loads(line)
                ts = datetime.fromisoformat(entry["ts"])
                if ts >= cutoff:
                    kept.append(line)
            except (json.JSONDecodeError, KeyError, ValueError):
                pass  # Skip malformed entries

    with open(log_path, "w") as f:
        f.writelines(kept)


async def cleanup_old_files(jobs_dir: Path, logs_dir: Path):
    """Delete jobs >30 days old and orphaned logs >60 days old."""
    now = datetime.now()
    job_cutoff = now - timedelta(days=30)
    log_cutoff = now - timedelta(days=60)

    # Get set of existing job IDs for orphan check
    existing_jobs = set()

    # Clean old jobs
    for job_dir in jobs_dir.iterdir():
        if not job_dir.is_dir():
            continue
        existing_jobs.add(job_dir.name)
        mtime = datetime.fromtimestamp(job_dir.stat().st_mtime)
        if mtime < job_cutoff:
            try:
                shutil.rmtree(job_dir)
                existing_jobs.discard(job_dir.name)
            except Exception as e:
                print(f"Failed to delete job {job_dir.name}: {e}")

    # Clean orphaned logs (job no longer exists and log >60 days old)
    for log_file in logs_dir.iterdir():
        if not log_file.is_file() or log_file.suffix != '.log':
            continue
        job_id = log_file.stem  # filename without .log extension
        if job_id in existing_jobs:
            continue  # Keep logs for existing jobs
        mtime = datetime.fromtimestamp(log_file.stat().st_mtime)
        if mtime < log_cutoff:
            try:
                log_file.unlink()
            except Exception as e:
                print(f"Failed to delete log {log_file.name}: {e}")

    # Clean old memory log entries
    await cleanup_memory_log(logs_dir)


async def cleanup_scheduler(jobs_dir: Path, logs_dir: Path):
    """Run cleanup immediately, then every 24 hours."""
    while True:
        try:
            await cleanup_old_files(jobs_dir, logs_dir)
        except Exception as e:
            print(f"Cleanup error: {e}")
        await asyncio.sleep(86400)  # 24 hours
