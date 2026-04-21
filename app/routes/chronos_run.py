"""Chronos analysis routes and modular analysis functions."""

from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel
from pathlib import Path
from typing import Optional
import asyncio
import io
import zipfile
from functools import partial

# Set matplotlib to non-GUI backend before any plotting imports
import matplotlib
matplotlib.use('Agg')

from ..services.job_manager import job_manager
from ..services.connection_manager import manager
from ..services.file_utils import parse_file
from ..services.data_loader import load_crispr_data, load_controls
from ..services.concurrency import matplotlib_lock
from ..services.logging_utils import send_log, send_error

router = APIRouter()


# =============================================================================
# Module 1: Load Data
# =============================================================================

async def load_data(job_id: str):
    """Load all data needed for Chronos analysis.

    Returns:
        dict with keys: readcounts, sequence_map, guide_map,
                       positive_controls, negative_controls, copy_number_path
    """
    await send_log(job_id, "Loading data files...")

    readcounts, sequence_map, guide_map = load_crispr_data(job_id)

    positive_controls, negative_controls = load_controls(
        positive_path=job_manager.get_file_path("positive_controls"),
        negative_path=job_manager.get_file_path("negative_controls"),
        require_controls=False
    )

    negative_control_sgrnas = guide_map[
        guide_map.gene.isin(negative_controls)
    ].sgrna.unique().tolist()

    return {
        "readcounts": readcounts,
        "sequence_map": sequence_map,
        "guide_map": guide_map,
        "positive_controls": positive_controls,
        "negative_controls": negative_controls,
        "negative_control_sgrnas": negative_control_sgrnas,
        "copy_number_path": job_manager.get_file_path("copy_number"),
    }


# =============================================================================
# Module 2: Train Chronos
# =============================================================================

async def train_chronos(job_id: str, data: dict, output_dir: Path, csv_dir: Path):
    """Preprocess data, build model, train, save, and convert outputs.

    Returns:
        chronos.Chronos model instance
    """
    import chronos

    readcounts = data["readcounts"]
    sequence_map = data["sequence_map"]
    guide_map = data["guide_map"]
    negative_control_sgrnas = data["negative_control_sgrnas"]

    # Preprocess
    await manager.send_status("running", "Preprocessing data...", job_id)
    await send_log(job_id, "Running nan_outgrowths preprocessing...")
    chronos.nan_outgrowths(
        readcounts=readcounts,
        guide_gene_map=guide_map,
        sequence_map=sequence_map
    )

    # Initialize model
    await manager.send_status("running", "Initializing Chronos model...", job_id)
    await send_log(job_id, "Initializing Chronos model...")

    library_label = job_manager.get_library_label()
    use_pretrained = job_manager.get_use_pretrained()
    await send_log(job_id, f"Using library label: {library_label}")
    await send_log(job_id, f"Use pretrained parameters: {use_pretrained}")

    model = chronos.Chronos(
        readcounts={library_label: readcounts},
        sequence_map={library_label: sequence_map},
        guide_gene_map={library_label: guide_map},
        negative_control_sgrnas={library_label: negative_control_sgrnas},
        pretrained=use_pretrained,
    )

    # Load pretrained parameters if requested
    if use_pretrained:
        await manager.send_status("running", "Loading pretrained DepMap parameters...", job_id)
        await send_log(job_id, "Fetching/loading DepMap pretrained parameters...")
        try:
            await asyncio.to_thread(
                chronos.fetch_parameters,
                "app/data/DepMapDataURLs.json",
                output_dir="app/data/DepMapData",
                relative_to_chronos=False
            )
            await asyncio.to_thread(model.import_model, "app/data/DepMapData")
            await asyncio.sleep(0.5)
            await send_log(job_id, "Pretrained parameters loaded successfully.")
        except Exception as e:
            await send_error(job_id, e, "Loading pretrained parameters")
            raise

    # Train
    await manager.send_status("running", "Training Chronos model (this may take a while)...", job_id)
    await send_log(job_id, "Training Chronos model...")
    await asyncio.to_thread(model.train)
    await send_log(job_id, "Training complete.")

    # Save model outputs
    await manager.send_status("running", "Saving model outputs...", job_id)
    await send_log(job_id, "Saving model outputs...")
    await asyncio.to_thread(partial(model.save, str(output_dir), overwrite=True))
    await send_log(job_id, "Model saved.")

    # Convert HDF5 files to CSV
    await convert_hdf5_to_csv(output_dir, csv_dir, job_id)

    return model


# =============================================================================
# Module 3: Copy Number Correction
# =============================================================================

async def apply_copy_number_correction(
    job_id: str,
    model,
    copy_number_path: Path,
    output_dir: Path,
    csv_dir: Path
):
    """Apply copy number correction if data is available.

    Returns:
        Corrected gene effect DataFrame, or None if correction not applied
    """
    import chronos

    if not copy_number_path:
        return None

    await manager.send_status("running", "Applying copy number correction...", job_id)
    await send_log(job_id, "Applying copy number correction...")

    await send_log(job_id, "Loading copy number data...")
    cn = parse_file(
        copy_number_path,
        job_manager.get_file_format("copy_number"),
        index_col=0
    )
    await send_log(job_id, f"Copy number data: {cn.shape[0]} genes x {cn.shape[1]} samples")

    try:
        await send_log(job_id, "Computing corrected gene effects...")
        gene_effects = model.gene_effect
        corrected, shifts = chronos.alternate_CN(gene_effects, cn)
        await send_log(job_id, "Correction complete.")

        # Save corrected gene effect
        chronos.write_hdf5(corrected, output_dir / "gene_effect_corrected.hdf5")
        corrected.to_csv(csv_dir / "gene_effect_corrected.csv")
        await send_log(job_id, "Saved gene_effect_corrected.hdf5 and .csv")

        # Save shifts
        shifts.to_csv(csv_dir / "copy_number_shifts.csv")
        await send_log(job_id, "Saved copy_number_shifts.csv")

        return corrected

    except Exception as e:
        await send_error(job_id, e, "Copy number correction")
        await send_log(job_id, "Analysis will proceed with uncorrected data.")
        return None


# =============================================================================
# Module 4: Post-Chronos QC Report
# =============================================================================

async def run_post_chronos_qc(
    job_id: str,
    output_dir: Path,
    reports_dir: Path,
    title: str,
    positive_controls: list,
    negative_controls: list,
    copy_number_path: Path,
    gene_effect_file: str,
):
    """Generate post-Chronos QC report in background."""
    import chronos.reports

    try:
        await send_log(job_id, "Generating dataset QC report...")
        await send_log(job_id, f"Using {gene_effect_file} for QC report")

        # Load copy number for report if available
        cn_for_report = None
        if copy_number_path:
            try:
                cn_for_report = parse_file(
                    copy_number_path,
                    job_manager.get_file_format("copy_number"),
                    index_col=0
                )
            except Exception:
                pass

        async with matplotlib_lock:
            import matplotlib.pyplot as plt
            plt.close('all')  # Clear stale figures
            await asyncio.to_thread(
                chronos.reports.dataset_qc_report,
                title=title + " Chronos QC",
                report_name=f"{title} chronos qc.pdf",
                data=str(output_dir),
                positive_control_genes=positive_controls,
                negative_control_genes=negative_controls,
                copy_number=cn_for_report,
                directory=str(reports_dir),
                gene_effect_file=gene_effect_file,
            )
        await send_log(job_id, "Dataset QC report generated.")
        await asyncio.sleep(0.1)
        await manager.send_status("qc_report_ready", "QC report ready", job_id)

    except Exception as e:
        await send_error(job_id, e, "Dataset QC report")


# =============================================================================
# Module 5: Hit Calling
# =============================================================================

async def run_hit_calling(
    job_id: str,
    gene_effect,
    negative_controls: list,
    positive_controls: list,
    output_dir: Path,
    csv_dir: Path,
    reports_dir: Path,
    title: str,
    gene_effect_file: str,
    full_gene_effect_file: str
):
    """Compute dependency statistics, save results, and generate report."""
    import chronos
    import chronos.reports
    from chronos.hit_calling import (
        get_probability_dependent, get_fdr_from_probabilities,
        get_pvalue_dependent, get_fdr_from_pvalues
    )

    try:
        await send_log(job_id, "")
        await send_log(job_id, "=" * 60)
        await send_log(job_id, "HIT CALLING")
        await send_log(job_id, "=" * 60)

        # P-value based (needs negative controls only)
        await send_log(job_id, "Computing p-values...")
        pvalues = await asyncio.to_thread(get_pvalue_dependent, gene_effect, negative_controls)
        chronos.write_hdf5(pvalues, output_dir / "pvalues.hdf5")
        pvalues.to_csv(csv_dir / "pvalues.csv")
        await send_log(job_id, "Saved pvalues.hdf5 and .csv")

        await send_log(job_id, "Computing FDR from p-values...")
        fdr_pval = await asyncio.to_thread(get_fdr_from_pvalues, pvalues)
        chronos.write_hdf5(fdr_pval, output_dir / "fdr_from_pvalues.hdf5")
        fdr_pval.to_csv(csv_dir / "fdr_from_pvalues.csv")
        await send_log(job_id, "Saved fdr_from_pvalues.hdf5 and .csv")

        # Probability based (needs both controls)
        await send_log(job_id, "Computing dependency probabilities...")
        probs = await asyncio.to_thread(
            get_probability_dependent, gene_effect, negative_controls, positive_controls
        )
        chronos.write_hdf5(probs, output_dir / "probability_dependent.hdf5")
        probs.to_csv(csv_dir / "probability_dependent.csv")
        await send_log(job_id, "Saved probability_dependent.hdf5 and .csv")

        await send_log(job_id, "Computing FDR from probabilities...")
        fdr_prob = await asyncio.to_thread(get_fdr_from_probabilities, probs)
        chronos.write_hdf5(fdr_prob, output_dir / "fdr_from_probabilities.hdf5")
        fdr_prob.to_csv(csv_dir / "fdr_from_probabilities.csv")
        await send_log(job_id, "Saved fdr_from_probabilities.hdf5 and .csv")

        await send_log(job_id, "Hit calling complete. Generating hit calling report...")

        # Generate hit calling report
        async with matplotlib_lock:
            import matplotlib.pyplot as plt
            plt.close('all')  # Clear stale figures
            await asyncio.to_thread(
                chronos.reports.hit_calling_report,
                title=title,
                report_name=f"{title} hits.pdf",
                directory=str(reports_dir),
                gene_effect_file=str(output_dir / gene_effect_file),
                p_value_file=str(output_dir / "pvalues.hdf5"),
                frequentist_fdr_file=str(output_dir / "fdr_from_pvalues.hdf5"),
                probability_file=str(output_dir / "probability_dependent.hdf5"),
                bayesian_fdr_file=str(output_dir / "fdr_from_probabilities.hdf5"),
                full_gene_effect_file=full_gene_effect_file
            )
        await send_log(job_id, "Hit calling report generated.")
        await asyncio.sleep(0.1)
        await manager.send_status("hits_report_ready", "Hits report ready", job_id)

    except Exception as e:
        await send_error(job_id, e, "Hit calling")


# =============================================================================
# Utility Functions
# =============================================================================

async def convert_hdf5_to_csv(hdf5_dir: Path, csv_dir: Path, job_id: str):
    """Convert all HDF5 files in a directory to CSV."""
    import chronos

    hdf5_files = list(hdf5_dir.glob("*.hdf5"))
    await send_log(job_id, f"Converting {len(hdf5_files)} HDF5 files to CSV...")

    for hdf5_file in hdf5_files:
        try:
            df = chronos.read_hdf5(str(hdf5_file))
            csv_path = csv_dir / f"{hdf5_file.stem}.csv"
            df.to_csv(csv_path)
            await send_log(job_id, f"Converted {hdf5_file.name} -> {csv_path.name}")
        except Exception as e:
            await send_error(job_id, e, f"Converting {hdf5_file.name}")


# =============================================================================
# Main Orchestrator
# =============================================================================

async def run_chronos_analysis(job_id: str):
    """Main orchestrator for Chronos analysis pipeline."""
    try:
        await send_log(job_id, "\n" + "=" * 60)
        await send_log(job_id, "CHRONOS ANALYSIS")
        await send_log(job_id, "=" * 60 + "\n")

        job_manager.mark_chronos_started()
        await manager.send_status("running", "Starting Chronos analysis...", job_id)

        # Setup directories
        job_dir = job_manager.get_job_dir(job_id)
        chronos_output_dir = job_dir / "ChronosOutput"
        chronos_output_dir.mkdir(exist_ok=True)
        csv_dir = job_dir / "CSVOutputs"
        csv_dir.mkdir(exist_ok=True)

        # Module 1: Load data
        await manager.send_status("running", "Loading data files...", job_id)
        data = await load_data(job_id)

        # Module 2: Train Chronos
        model = await train_chronos(job_id, data, chronos_output_dir, csv_dir)

        # Module 3: Copy number correction
        corrected_gene_effect = await apply_copy_number_correction(
            job_id, model, data["copy_number_path"], chronos_output_dir, csv_dir
        )

        # Determine which gene effect to use for downstream analysis
        gene_effect = corrected_gene_effect if corrected_gene_effect is not None else model.gene_effect

        # Mark Chronos as complete and send user to results
        job_manager.mark_chronos_completed()
        await send_log(job_id, "Chronos analysis complete! Running post-processing...")
        await asyncio.sleep(0.1)
        await manager.send_status(
            "chronos_complete",
            "Chronos analysis complete!",
            job_id,
            data={"output_dir": str(csv_dir)}
        )

        # Setup for report generation (run sequentially - matplotlib is not thread-safe)
        reports_dir = job_manager.get_reports_dir(job_id)
        title_file = job_dir / "title.txt"
        title = title_file.read_text().strip() if title_file.exists() else job_id
        gene_effect_file = "gene_effect_corrected.hdf5" if corrected_gene_effect is not None else "gene_effect.hdf5"
        full_gene_effect_file = job_manager.get_full_gene_effect_file()

        # Module 4: Post-Chronos QC
        await run_post_chronos_qc(
            job_id,
            chronos_output_dir,
            reports_dir,
            title,
            data["positive_controls"],
            data["negative_controls"],
            data["copy_number_path"],
            gene_effect_file,
        )

        # Module 5: Hit calling
        await run_hit_calling(
            job_id,
            gene_effect,
            data["negative_controls"],
            data["positive_controls"],
            chronos_output_dir,
            csv_dir,
            reports_dir,
            title,
            gene_effect_file,
            str(full_gene_effect_file),
        )

        # Differential dependency is now triggered from the Results page via /api/run-differential-dependency

    except Exception as e:
        await send_error(job_id, e, "Chronos analysis")


# =============================================================================
# API Routes
# =============================================================================

class ChronosRequest(BaseModel):
    job_id: str


@router.post("/run-chronos")
async def start_chronos(request: ChronosRequest):
    job_id = request.job_id
    job_manager.resume_job(job_id)

    if not job_manager.current_job_id:
        raise HTTPException(status_code=400, detail="Job not found")

    asyncio.create_task(run_chronos_analysis(job_id))
    return {"status": "started", "job_id": job_id}


@router.get("/outputs/{job_id}")
async def list_outputs(job_id: str):
    """List all output files for a job from Reports, ChronosOutput, and CSVOutputs."""
    job_dir = job_manager.get_job_dir(job_id)
    reports_dir = job_dir / "Reports"
    csv_dir = job_dir / "CSVOutputs"
    chronos_dir = job_dir / "ChronosOutput"

    files = []

    # Include PDF reports
    if reports_dir.exists():
        for pdf_file in reports_dir.glob("*.pdf"):
            files.append({
                "name": pdf_file.name,
                "size": pdf_file.stat().st_size,
                "source": "Reports",
            })

    if chronos_dir.exists():
        for csv_file in chronos_dir.glob("*.csv"):
            files.append({
                "name": csv_file.name,
                "size": csv_file.stat().st_size,
                "source": "ChronosOutput",
            })

    if csv_dir.exists():
        for csv_file in csv_dir.glob("*.csv"):
            files.append({
                "name": csv_file.name,
                "size": csv_file.stat().st_size,
                "source": "CSVOutputs",
            })

    # Include PoolQ outputs (for sequence format jobs)
    poolq_dir = job_dir / "uploads" / "poolq_work"
    if poolq_dir.exists():
        for poolq_file in poolq_dir.glob("*"):
            if poolq_file.is_file():
                files.append({
                    "name": poolq_file.name,
                    "size": poolq_file.stat().st_size,
                    "source": "PoolQ",
                })

    # Include readcounts.csv from uploads (PoolQ output)
    readcounts_file = job_dir / "uploads" / "readcounts.csv"
    if readcounts_file.exists():
        files.append({
            "name": "readcounts.csv",
            "size": readcounts_file.stat().st_size,
            "source": "PoolQ",
        })

    if not files:
        raise HTTPException(status_code=404, detail="No outputs found")

    files.sort(key=lambda f: f["name"].lower())
    return {"job_id": job_id, "files": files}


@router.get("/outputs/{job_id}/download/{filename}")
async def download_output(job_id: str, filename: str, source: str = "CSVOutputs"):
    """Download a single output file."""
    job_dir = job_manager.get_job_dir(job_id)

    if source == "Reports":
        file_path = job_dir / "Reports" / filename
    elif source == "ChronosOutput":
        file_path = job_dir / "ChronosOutput" / filename
    elif source == "PoolQ":
        # Check poolq_work first, then uploads for readcounts.csv
        file_path = job_dir / "uploads" / "poolq_work" / filename
        if not file_path.exists():
            file_path = job_dir / "uploads" / filename
    else:
        file_path = job_dir / "CSVOutputs" / filename

    if not file_path.exists():
        raise HTTPException(status_code=404, detail="File not found")

    return FileResponse(file_path, filename=filename)


class FileInfo(BaseModel):
    name: str
    source: str = "CSVOutputs"


class DownloadRequest(BaseModel):
    files: list[FileInfo]


@router.post("/outputs/{job_id}/download-zip")
async def download_zip(job_id: str, request: DownloadRequest):
    """Download multiple files as a zip archive."""
    job_dir = job_manager.get_job_dir(job_id)

    title_file = job_dir / "title.txt"
    title = title_file.read_text().strip() if title_file.exists() else job_id
    zip_filename = f"{title}_outputs.zip"

    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        for file_info in request.files:
            if file_info.source == "Reports":
                file_path = job_dir / "Reports" / file_info.name
            elif file_info.source == "ChronosOutput":
                file_path = job_dir / "ChronosOutput" / file_info.name
            elif file_info.source == "PoolQ":
                file_path = job_dir / "uploads" / "poolq_work" / file_info.name
                if not file_path.exists():
                    file_path = job_dir / "uploads" / file_info.name
            else:
                file_path = job_dir / "CSVOutputs" / file_info.name

            if file_path.exists():
                zf.write(file_path, file_info.name)

    zip_buffer.seek(0)

    return StreamingResponse(
        zip_buffer,
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="{zip_filename}"'}
    )
