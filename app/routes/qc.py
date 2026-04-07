"""QC analysis routes."""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional
import asyncio
import traceback

from ..services.job_manager import job_manager
from ..services.connection_manager import manager
from ..services.file_utils import parse_file
from ..services.data_loader import load_crispr_data, load_controls
from ..services.concurrency import matplotlib_lock

router = APIRouter()


async def run_initial_qc(job_id: str, title: str):
    import asyncio

    log_path = job_manager.get_log_path(job_id)

    def append_log(message: str):
        with open(log_path, "a") as f:
            f.write(str(message) + "\n")

    try:
        await manager.send_status("running", "Starting QC analysis...", job_id)

        # Load data using shared utilities
        await manager.send_status("running", "Loading data files...", job_id)
        readcounts, sequence_map, guide_map = load_crispr_data(job_id)

        # Load controls (required for QC)
        positive_controls, negative_controls = load_controls(
            positive_path=job_manager.get_file_path("positive_controls"),
            negative_path=job_manager.get_file_path("negative_controls"),
            require_controls=True
        )

        await manager.send_status("running", "Running Chronos QC...", job_id)

        reports_dir = job_manager.get_reports_dir(job_id)
        log_path = job_manager.get_log_path(job_id)

        # Convert gene lists to sgRNA lists
        negative_control_sgrnas = guide_map[guide_map.gene.isin(negative_controls)].sgrna.unique()
        positive_control_sgrnas = guide_map[guide_map.gene.isin(positive_controls)].sgrna.unique()

        def run_qc_report():
            """Run QC report in thread, capturing output."""
            import contextlib
            import io
            from chronos import reports

            output_capture = io.StringIO()
            error = None

            try:
                with contextlib.redirect_stdout(output_capture), contextlib.redirect_stderr(output_capture):
                    reports.qc_initial_data(
                        readcounts=readcounts,
                        guide_map=guide_map,
                        sequence_map=sequence_map,
                        positive_control_sgrnas=positive_control_sgrnas,
                        negative_control_sgrnas=negative_control_sgrnas,
                        directory=str(reports_dir),
                        title=title,
                        report_name=f"{title} initial qc.pdf"
                    )
            except Exception as e:
                error = e

            return output_capture.getvalue(), error

        async with matplotlib_lock:
            output, error = await asyncio.to_thread(run_qc_report)

        with open(log_path, "w") as log_file:
            log_file.write(output)
            if error:
                log_file.write(f"\n\nERROR:\n{traceback.format_exc()}")
                await manager.send_error(str(error), job_id)
                return

        job_manager.mark_qc_completed()

        await manager.send_status(
            "complete",
            "QC analysis complete!",
            job_id,
            {"reports_dir": str(reports_dir)}
        )

    except Exception as e:
        error_msg = traceback.format_exc()
        await manager.send_error(str(e), job_id)
        append_log(error_msg)
        import asyncio
        await asyncio.sleep(0.5)


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

    required = ["readcounts", "condition_map", "guide_map"]
    missing = [f for f in required if not job_manager.get_file_path(f)]

    if missing:
        raise HTTPException(
            status_code=400,
            detail=f"Missing required files: {', '.join(missing)}"
        )

    job_id = job_manager.current_job_id
    title = request.title if request and request.title else job_manager.get_title()

    # Save use_pretrained setting
    if request:
        job_manager.set_use_pretrained(request.use_pretrained)

    job_manager.mark_qc_started()

    import asyncio
    asyncio.create_task(run_initial_qc(job_id, title))

    return {"status": "started", "job_id": job_id}
