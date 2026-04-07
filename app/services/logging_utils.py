"""Logging utilities for job progress updates."""

import asyncio
from .job_manager import job_manager
from .connection_manager import manager


async def send_log(job_id: str, message: str):
    """Append message to log file and send full log to client."""
    log_path = job_manager.get_log_path(job_id)

    with open(log_path, "a") as f:
        f.write(message + "\n")
        f.flush()

    full_log = log_path.read_text()
    await manager.broadcast({
        "type": "log",
        "job_id": job_id,
        "log": full_log,
    })
    await asyncio.sleep(0)
