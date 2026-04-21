"""Logging utilities for job progress updates."""

import asyncio
import sys
import traceback
from .job_manager import job_manager
from .connection_manager import manager


async def send_log(job_id: str, message: str):
    """Append message to log file, print to stdout, and send to client.

    Outputs to:
    1. Server stdout (terminal)
    2. Job log file
    3. Client's Server Output (WebSocket)
    """
    # 1. Print to server stdout
    print(f"[{job_id}] {message}", flush=True)

    # 2. Write to log file
    log_path = job_manager.get_log_path(job_id)
    with open(log_path, "a") as f:
        f.write(message + "\n")
        f.flush()

    # 3. Send to client's Server Output
    full_log = log_path.read_text()
    await manager.broadcast({
        "type": "log",
        "job_id": job_id,
        "log": full_log,
    })
    await asyncio.sleep(0)


async def send_error(job_id: str, error: Exception, context: str = ""):
    """Unified error handler that sends errors to all 4 destinations.

    Outputs to:
    1. Server stderr (terminal)
    2. Job log file
    3. Client's Server Output (WebSocket log)
    4. Error popup (WebSocket error)

    Args:
        job_id: The job identifier
        error: The exception that occurred
        context: Optional context string (e.g., "QC analysis", "Chronos training")
    """
    # Format error message
    error_str = str(error)
    full_traceback = traceback.format_exc()

    if context:
        error_msg = f"{context} failed: {error_str}"
    else:
        error_msg = error_str

    # 1. Print to server stderr
    print(f"\n{'='*60}", file=sys.stderr, flush=True)
    print(f"[{job_id}] ERROR: {error_msg}", file=sys.stderr, flush=True)
    print(full_traceback, file=sys.stderr, flush=True)
    print(f"{'='*60}\n", file=sys.stderr, flush=True)

    # 2 & 3. Write to log file and send to client's Server Output
    await send_log(job_id, f"ERROR: {error_msg}")
    await send_log(job_id, full_traceback)

    # 4. Send error popup to client
    await manager.send_error(error_msg, job_id)

    # Small delay to ensure messages are sent
    await asyncio.sleep(0.1)
