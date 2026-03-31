from fastapi import APIRouter, HTTPException, BackgroundTasks
from pathlib import Path
import pandas as pd
import sys
import traceback

from ..services.job_manager import job_manager
from ..services.connection_manager import manager

router = APIRouter()


def parse_file(file_path: Path, *args, **kwargs) -> pd.DataFrame:
    if file_path.suffix == ".csv":
        return pd.read_csv(file_path, *args, **kwargs)
    elif file_path.suffix == ".tsv":
        return pd.read_csv(file_path, *args, **kwargs)
    else:
        raise ValueError(f"unexpected file suffix in {file_path}")


def parse_gene_list(file_path: Path) -> list[str]:
    with open(file_path, "r") as f:
        return [line.strip() for line in f if line.strip()]


async def run_initial_qc(job_id: str, title: str):
    import asyncio

    try:
        await manager.send_status("running", "Starting QC analysis...", job_id)

        readcounts_path = job_manager.get_file_path("readcounts")
        condition_map_path = job_manager.get_file_path("condition_map")
        guide_map_path = job_manager.get_file_path("guide_map")
        positive_controls_path = job_manager.get_file_path("positive_controls")
        negative_controls_path = job_manager.get_file_path("negative_controls")

        if not all([readcounts_path, condition_map_path, guide_map_path]):
            await manager.send_error(
                "Missing required files: readcounts, condition_map, or guide_map",
                job_id
            )
            return

        await manager.send_status("running", "Loading data files...", job_id)

        readcounts = parse_file(readcounts_path, index_col=0).astype(float)
        sequence_map = parse_file(condition_map_path)
        guide_map = parse_file(guide_map_path)

        positive_controls = None
        negative_controls = None

        if positive_controls_path:
            positive_controls = parse_gene_list(positive_controls_path)
        if negative_controls_path:
            negative_controls = parse_gene_list(negative_controls_path)

        await manager.send_status("running", "Running Chronos QC...", job_id)

        reports_dir = job_manager.get_reports_dir(job_id)
        log_path = job_manager.get_log_path(job_id)

        negative_control_sgrnas = guide_map[guide_map.gene.isin(negative_controls)].sgrna.unique()
        positive_control_sgrnas = guide_map[guide_map.gene.isin(positive_controls)].sgrna.unique()

        with open(log_path, "w") as log_file:
            import contextlib
            import io

            output_capture = io.StringIO()

            try:
                with contextlib.redirect_stdout(output_capture), contextlib.redirect_stderr(output_capture):
                    from chronos import reports

                    reports.qc_initial_data(
                        readcounts=readcounts,
                        guide_map=guide_map,
                        sequence_map=sequence_map,
                        positive_control_sgrnas=positive_control_sgrnas,
                        negative_control_sgrnas=negative_control_sgrnas,
                        directory=str(reports_dir),
                        title=title,
                    )

                log_file.write(output_capture.getvalue())

            except Exception as e:
                log_file.write(output_capture.getvalue())
                log_file.write(f"\n\nERROR:\n{traceback.format_exc()}")
                await manager.send_error(str(e), job_id)
                return

        await manager.send_status(
            "complete",
            "QC analysis complete!",
            job_id,
            {"reports_dir": str(reports_dir)}
        )

    except Exception as e:
        await manager.send_error(f"Unexpected error: {str(e)}", job_id)


from pydantic import BaseModel
from typing import Optional

class QCRequest(BaseModel):
    job_id: Optional[str] = None
    title: Optional[str] = None

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

    import asyncio
    asyncio.create_task(run_initial_qc(job_id, title))

    return {"status": "started", "job_id": job_id}
