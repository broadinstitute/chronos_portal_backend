"""QC analysis routes."""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional
import asyncio

from ..services.job_manager import job_manager
from ..services.connection_manager import manager
from ..services.data_loader import load_crispr_data, load_controls
from ..services.concurrency import matplotlib_lock
from ..services.logging_utils import send_log, send_error
from ..services.monitoring import log_memory
from ..services.validation import (
    validate_preprocessing_complete,
    validate_required_files,
)

router = APIRouter()


async def run_initial_qc(job_id: str, title: str):
    """Run initial QC analysis and generate report."""
    from chronos import reports

    try:
        log_memory(job_manager.logs_dir, job_id, "qc_start")
        await send_log(job_id, "Starting QC analysis...")

        # Load data using shared utilities
        await send_log(job_id, "Loading data files...")
        readcounts, sequence_map, guide_map = load_crispr_data(job_id)

        # Load controls (required for QC)
        positive_controls, negative_controls = load_controls(
            positive_path=job_manager.get_file_path("positive_controls"),
            negative_path=job_manager.get_file_path("negative_controls"),
            require_controls=True
        )

        await send_log(job_id, "Running Initial QC...")

        reports_dir = job_manager.get_reports_dir(job_id)

        # Convert gene lists to sgRNA lists
        negative_control_sgrnas = guide_map[guide_map.gene.isin(negative_controls)].sgrna.unique()
        positive_control_sgrnas = guide_map[guide_map.gene.isin(positive_controls)].sgrna.unique()

        # Run QC report - let library stdout go to terminal (like chronos_run.py)
        async with matplotlib_lock:
            import matplotlib.pyplot as plt
            plt.close('all')  # Clear stale figures
            await asyncio.to_thread(
                reports.qc_initial_data,
                readcounts=readcounts,
                guide_map=guide_map,
                sequence_map=sequence_map,
                positive_control_sgrnas=positive_control_sgrnas,
                negative_control_sgrnas=negative_control_sgrnas,
                directory=str(reports_dir),
                title=title,
                report_name=f"{title} initial qc.pdf"
            )

        await send_log(job_id, "Initial QC report generated.")
        log_memory(job_manager.logs_dir, job_id, "qc_complete")
        job_manager.mark_qc_completed()

        await manager.send_status(
            "complete",
            "QC analysis complete!",
            job_id,
            {"reports_dir": str(reports_dir)}
        )

    except Exception as e:
        await send_error(job_id, e, "QC analysis")


class QCRequest(BaseModel):
    job_id: Optional[str] = None
    title: Optional[str] = None
    use_pretrained: bool = True


@router.post("/run-qc")
async def start_qc(request: QCRequest = None):
    # Resume job if job_id provided
    if request and request.job_id:
        job_manager.resume_job(request.job_id)

    if not job_manager.current_job_id:
        raise HTTPException(status_code=400, detail="No active job. Upload files first.")

    # Validate required files
    required = ["readcounts", "condition_map", "guide_map"]
    missing = validate_required_files(required)

    if missing:
        raise HTTPException(
            status_code=400,
            detail=f"Missing required files: {', '.join(missing)}"
        )

    # For sequence formats, check preprocessing is complete
    # (condition_map validation happens during preprocessing)
    validate_preprocessing_complete()

    job_id = job_manager.current_job_id
    title = request.title if request and request.title else job_manager.get_title()

    # Save use_pretrained setting
    if request:
        job_manager.set_use_pretrained(request.use_pretrained)

    job_manager.mark_qc_started()

    import asyncio
    asyncio.create_task(run_initial_qc(job_id, title))

    return {"status": "started", "job_id": job_id}
