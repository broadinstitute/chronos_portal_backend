"""QC analysis routes."""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional
import traceback

from ..services.job_manager import job_manager
from ..services.connection_manager import manager
from ..services.file_utils import parse_file
from ..services.data_loader import load_crispr_data, load_controls

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
        error_msg = traceback.format_exc()
        await manager.send_error(str(e), job_id)
        append_log(error_msg)
        import asyncio
        await asyncio.sleep(0.5)


class QCRequest(BaseModel):
    job_id: Optional[str] = None
    title: Optional[str] = None
    condition1: Optional[str] = None
    condition2: Optional[str] = None
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

    # Validate compare conditions if provided
    if request and (request.condition1 or request.condition2):
        # Both must be provided
        if not request.condition1 or not request.condition2:
            raise HTTPException(
                status_code=400,
                detail="Both conditions must be specified for comparison"
            )

        # Validate conditions exist in condition map
        try:
            condition_map_path = job_manager.get_file_path("condition_map")
            condition_map = parse_file(
                condition_map_path,
                job_manager.get_file_format("condition_map")
            )

            # Check required columns for comparison
            if not hasattr(condition_map, 'columns'):
                raise HTTPException(
                    status_code=400,
                    detail="Condition map file could not be parsed as a table"
                )

            if "condition" not in condition_map.columns:
                raise HTTPException(
                    status_code=400,
                    detail=f"Condition map must have a 'condition' column for comparison. Found columns: {list(condition_map.columns)}"
                )
            if "replicate" not in condition_map.columns:
                raise HTTPException(
                    status_code=400,
                    detail=f"Condition map must have a 'replicate' column for comparison. Found columns: {list(condition_map.columns)}"
                )

            # Get unique conditions (excluding null/NaN values)
            available_conditions = condition_map["condition"].dropna().unique().tolist()

            # Check both conditions exist
            if request.condition1 not in available_conditions:
                raise HTTPException(
                    status_code=400,
                    detail=f"'{request.condition1}' was not found in the condition map's conditions: {available_conditions}"
                )
            if request.condition2 not in available_conditions:
                raise HTTPException(
                    status_code=400,
                    detail=f"'{request.condition2}' was not found in the condition map's conditions: {available_conditions}"
                )

            # Run check_condition_map from chronos.hit_calling if available
            try:
                from chronos.hit_calling import check_condition_map
                check_condition_map({"default": condition_map})
            except ImportError:
                pass

        except HTTPException:
            raise
        except Exception as e:
            print(f"[QC] Condition validation error: {traceback.format_exc()}", flush=True)
            raise HTTPException(status_code=400, detail=f"Error validating conditions: {str(e)}")

        job_manager.set_compare_conditions(request.condition1, request.condition2)

    # Save use_pretrained setting
    if request:
        job_manager.set_use_pretrained(request.use_pretrained)

    job_manager.mark_qc_started()

    import asyncio
    asyncio.create_task(run_initial_qc(job_id, title))

    return {"status": "started", "job_id": job_id}
