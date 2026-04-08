"""Differential dependency routes and functions."""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from pathlib import Path
from typing import Optional
import asyncio
import re
import traceback

from ..services.job_manager import job_manager
from ..services.connection_manager import manager
from ..services.concurrency import matplotlib_lock
from ..services.logging_utils import send_log
from ..services.file_utils import parse_file
from ..services.data_loader import load_crispr_data, load_controls

router = APIRouter()


# =============================================================================
# Module 6: Differential Dependency
# =============================================================================

async def run_differential_dependency(
    job_id: str,
    readcounts,
    condition_map,
    guide_map,
    negative_control_genes: list,
    library_label: str,
    condition1: str,
    condition2: str,
    csv_dir: Path,
):
    """Run condition comparison using ConditionComparison."""
    from chronos.hit_calling import ConditionComparison

    try:
        await send_log(job_id, "")
        await send_log(job_id, "=" * 60)
        await send_log(job_id, "DIFFERENTIAL DEPENDENCY")
        await send_log(job_id, "=" * 60)
        await send_log(job_id, f"Comparing {condition1} vs {condition2}...")
        await manager.send_status(
            "running",
            f"Running condition comparison: {condition1} vs {condition2}...",
            job_id
        )

        await send_log(job_id, "Initializing ConditionComparison model...")
        comparator = ConditionComparison(
            readcounts={library_label: readcounts},
            condition_map={library_label: condition_map},
            guide_gene_map={library_label: guide_map},
            negative_control_genes=negative_control_genes,
        )

        await send_log(job_id, "Training comparison models (this may take a while)...")
        comparison_stats = await asyncio.to_thread(
            comparator.compare_conditions,
            (condition1, condition2)
        )

        # Save results
        safe_c1 = re.sub(r'[^\w\-]', '_', condition1)
        safe_c2 = re.sub(r'[^\w\-]', '_', condition2)
        output_filename = f"condition_comparison_{safe_c1}_vs_{safe_c2}.csv"
        output_path = csv_dir / output_filename
        comparison_stats.to_csv(output_path)
        await send_log(job_id, f"Saved comparison results to {output_filename}")

        await send_log(job_id, "Condition comparison complete!")
        await asyncio.sleep(0.1)
        await manager.send_status(
            "comparison_complete",
            "Condition comparison complete!",
            job_id,
            {"comparison_file": output_filename}
        )

        # Generate differential dependency report
        reports_dir = job_manager.get_reports_dir(job_id)
        job_dir = job_manager.get_job_dir(job_id)
        title_file = job_dir / "title.txt"
        title = title_file.read_text().strip() if title_file.exists() else job_id

        await run_differential_dependency_report(
            job_id=job_id,
            title=title,
            condition1=condition1,
            condition2=condition2,
            stats_file=output_path,
            reports_dir=reports_dir,
        )

    except Exception as e:
        error_msg = f"Condition comparison failed: {e}"
        await send_log(job_id, f"ERROR: {error_msg}")
        await send_log(job_id, traceback.format_exc())
        await manager.send_error(error_msg, job_id)
        await asyncio.sleep(0.5)


# =============================================================================
# Module 7: Differential Dependency Report
# =============================================================================

async def run_differential_dependency_report(
    job_id: str,
    title: str,
    condition1: str,
    condition2: str,
    stats_file: Path,
    reports_dir: Path,
):
    """Generate differential dependency report after condition comparison."""
    from chronos import reports

    try:
        await send_log(job_id, "")
        await send_log(job_id, "=" * 60)
        await send_log(job_id, "DIFFERENTIAL DEPENDENCY REPORT")
        await send_log(job_id, "=" * 60)

        await send_log(job_id, "Generating differential dependency report...")
        async with matplotlib_lock:
            import matplotlib.pyplot as plt
            plt.close('all')  # Clear stale figures to prevent wrong images being saved
            await asyncio.to_thread(
                reports.differential_dependency_report,
                title=title,
                stats_file=str(stats_file),
                report_name=f"{title} {condition1} vs {condition2} differential dependency report.pdf",
                directory=str(reports_dir),
            )
        await send_log(job_id, "Differential dependency report complete!")
        await asyncio.sleep(0.1)
        await manager.send_status("dd_report_ready", "Differential dependency report ready", job_id)

    except Exception as e:
        error_msg = f"Differential dependency report failed: {e}"
        await send_log(job_id, f"ERROR: {error_msg}")
        await send_log(job_id, traceback.format_exc())
        await manager.send_error(error_msg, job_id)
        await asyncio.sleep(0.5)


# =============================================================================
# API Routes
# =============================================================================

class DifferentialDependencyRequest(BaseModel):
    job_id: str
    condition1: str
    condition2: str


@router.post("/run-differential-dependency")
async def start_differential_dependency(request: DifferentialDependencyRequest):
    """Start differential dependency analysis for two conditions."""
    job_id = request.job_id
    job_manager.resume_job(job_id)

    if not job_manager.current_job_id:
        raise HTTPException(status_code=400, detail="Job not found")

    # Validate conditions exist in available_conditions
    available_conditions = job_manager.get_available_conditions()
    if not available_conditions:
        raise HTTPException(
            status_code=400,
            detail="No conditions available. Condition map must have a 'condition' column."
        )

    if request.condition1 not in available_conditions:
        raise HTTPException(
            status_code=400,
            detail=f"'{request.condition1}' not found in available conditions: {available_conditions}"
        )
    if request.condition2 not in available_conditions:
        raise HTTPException(
            status_code=400,
            detail=f"'{request.condition2}' not found in available conditions: {available_conditions}"
        )
    if request.condition1 == request.condition2:
        raise HTTPException(
            status_code=400,
            detail="Conditions must be different"
        )

    # Load data needed for comparison
    readcounts, sequence_map, guide_map = load_crispr_data(job_id)
    _, negative_controls = load_controls(
        positive_path=job_manager.get_file_path("positive_controls"),
        negative_path=job_manager.get_file_path("negative_controls"),
        require_controls=False
    )

    job_dir = job_manager.get_job_dir(job_id)
    csv_dir = job_dir / "CSVOutputs"
    csv_dir.mkdir(exist_ok=True)

    library_label = job_manager.get_library_label()

    # Start comparison in background
    asyncio.create_task(run_differential_dependency(
        job_id=job_id,
        readcounts=readcounts,
        condition_map=sequence_map,
        guide_map=guide_map,
        negative_control_genes=negative_controls,
        library_label=library_label,
        condition1=request.condition1,
        condition2=request.condition2,
        csv_dir=csv_dir,
    ))

    return {"status": "started", "job_id": job_id}


@router.get("/jobs/{job_id}/conditions")
async def get_available_conditions(job_id: str):
    """Get available conditions for a job."""
    job_manager.resume_job(job_id)

    if not job_manager.current_job_id:
        raise HTTPException(status_code=404, detail="Job not found")

    conditions = job_manager.get_available_conditions()
    return {"job_id": job_id, "conditions": conditions or []}
