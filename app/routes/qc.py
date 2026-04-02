from fastapi import APIRouter, HTTPException, BackgroundTasks
from pathlib import Path
import sys
import traceback

from ..services.job_manager import job_manager
from ..services.connection_manager import manager
from ..services.file_utils import parse_file, parse_gene_list

router = APIRouter()

# Default control files
DEFAULT_CONTROLS_DIR = Path(__file__).parent.parent / "data" / "controls"
DEFAULT_POSITIVE_CONTROLS = DEFAULT_CONTROLS_DIR / "positive controls.txt"
DEFAULT_NEGATIVE_CONTROLS = DEFAULT_CONTROLS_DIR / "negative controls.txt"


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

        readcounts = parse_file(
            readcounts_path,
            job_manager.get_file_format("readcounts"),
            index_col=0
        ).astype(float)
        sequence_map = parse_file(
            condition_map_path,
            job_manager.get_file_format("condition_map")
        )
        guide_map = parse_file(
            guide_map_path,
            job_manager.get_file_format("guide_map")
        )

        if not "sgrna" in guide_map:
            raise KeyError("guide_map missing required column 'sgrna'")
        if not "sequence_ID" in sequence_map:
            raise KeyError("condition_map missing required column 'sequence_ID'")

        if not len(
            set(readcounts.columns) & set(guide_map.sgrna)
        ) and not len(set(readcounts.index) & set(sequence_map.sequence_ID)
        ):
            if len(
                set(readcounts.index) & set(guide_map.sgrna)
                ) and len(set(readcounts.columns) & set(sequence_map.sequence_ID)
            ):
                #readcounts passed with guides as rows, sequences/replicates as columns
                readcounts = readcounts.T
            else:
                raise ValueError("Readcounts columns and indices do not match the \
condition mpa sequence IDs and guide map sgRNAs provided")

        guide_map = guide_map[guide_map.sgrna.isin(readcounts.columns)]

        # Load controls (use defaults if not provided)
        if positive_controls_path:
            positive_controls = parse_gene_list(positive_controls_path)
        elif DEFAULT_POSITIVE_CONTROLS.exists():
            positive_controls = parse_gene_list(DEFAULT_POSITIVE_CONTROLS)
        else:
            positive_controls = []

        if negative_controls_path:
            negative_controls = parse_gene_list(negative_controls_path)
        elif DEFAULT_NEGATIVE_CONTROLS.exists():
            negative_controls = parse_gene_list(DEFAULT_NEGATIVE_CONTROLS)
        else:
            negative_controls = []


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

        job_manager.mark_qc_completed()

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
    condition1: Optional[str] = None
    condition2: Optional[str] = None

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

    # Save compare conditions if provided
    if request and (request.condition1 or request.condition2):
        job_manager.set_compare_conditions(request.condition1, request.condition2)

    job_manager.mark_qc_started()

    import asyncio
    asyncio.create_task(run_initial_qc(job_id, title))

    return {"status": "started", "job_id": job_id}
