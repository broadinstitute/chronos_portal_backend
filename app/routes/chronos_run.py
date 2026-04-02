from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel
from pathlib import Path
from typing import Optional
import asyncio
import sys
import io
import zipfile
from functools import partial

# Set matplotlib to non-GUI backend before any plotting imports
import matplotlib
matplotlib.use('Agg')

from ..services.job_manager import job_manager
from ..services.connection_manager import manager
from ..services.file_utils import parse_file, parse_gene_list

router = APIRouter()

# Default control files
DEFAULT_CONTROLS_DIR = Path(__file__).parent.parent / "data" / "controls"
DEFAULT_POSITIVE_CONTROLS = DEFAULT_CONTROLS_DIR / "positive controls.txt"
DEFAULT_NEGATIVE_CONTROLS = DEFAULT_CONTROLS_DIR / "negative controls.txt"


class ChronosRequest(BaseModel):
    job_id: str


async def run_chronos_analysis(job_id: str):
    """Run Chronos analysis and stream stdout to client."""
    import chronos
    import chronos.reports
    import pandas as pd
    import traceback

    log_path = job_manager.get_log_path(job_id)

    def append_log(message: str):
        with open(log_path, "a") as f:
            f.write(message + "\n")

    try:
        # Add header to log file
        append_log("\n" + "="*60)
        append_log("CHRONOS ANALYSIS")
        append_log("="*60 + "\n")

        job_manager.mark_chronos_started()
        await manager.send_status("running", "Starting Chronos analysis...", job_id)

        # Resume job to get file paths
        job_manager.resume_job(job_id)

        readcounts_path = job_manager.get_file_path("readcounts")
        condition_map_path = job_manager.get_file_path("condition_map")
        guide_map_path = job_manager.get_file_path("guide_map")
        copy_number_path = job_manager.get_file_path("copy_number")
        positive_controls_path = job_manager.get_file_path("positive_controls")
        negative_controls_path = job_manager.get_file_path("negative_controls")

        if not all([readcounts_path, condition_map_path, guide_map_path]):
            await manager.send_error("Missing required files", job_id)
            return

        await manager.send_status("running", "Loading data files...", job_id)
        await send_log(job_id, "Loading data files...")

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

        guide_map = guide_map[guide_map.sgrna.isin(readcounts.columns)]

        # Load controls (use defaults if not provided)
        if negative_controls_path:
            negative_controls = parse_gene_list(negative_controls_path)
        elif DEFAULT_NEGATIVE_CONTROLS.exists():
            negative_controls = parse_gene_list(DEFAULT_NEGATIVE_CONTROLS)
        else:
            negative_controls = []

        if positive_controls_path:
            positive_controls = parse_gene_list(positive_controls_path)
        elif DEFAULT_POSITIVE_CONTROLS.exists():
            positive_controls = parse_gene_list(DEFAULT_POSITIVE_CONTROLS)
        else:
            positive_controls = []

        negative_control_sgrnas = guide_map[guide_map.gene.isin(negative_controls)].sgrna.unique().tolist()

        # Create output directory
        job_dir = job_manager.get_job_dir(job_id)
        chronos_output_dir = job_dir / "ChronosOutput"
        chronos_output_dir.mkdir(exist_ok=True)

        await manager.send_status("running", "Preprocessing data...", job_id)
        await send_log(job_id, "Running nan_outgrowths preprocessing...")

        # Preprocess
        chronos.nan_outgrowths(
            readcounts=readcounts,
            guide_gene_map=guide_map,
            sequence_map=sequence_map
        )

        await manager.send_status("running", "Initializing Chronos model...", job_id)
        await send_log(job_id, "Initializing Chronos model...")

        # Initialize model (custom library, no pretrained)
        model = chronos.Chronos(
            readcounts={"library": readcounts},
            sequence_map={"library": sequence_map},
            guide_gene_map={"library": guide_map},
            negative_control_sgrnas={"library": negative_control_sgrnas},
        )

        await manager.send_status("running", "Training Chronos model (this may take a while)...", job_id)
        await send_log(job_id, "Training Chronos model...")

        # Run training in thread pool to keep event loop responsive
        await asyncio.to_thread(model.train)

        await send_log(job_id, "Training complete.")

        await manager.send_status("running", "Saving model outputs...", job_id)
        await send_log(job_id, "Saving model outputs...")
        await asyncio.to_thread(partial(model.save, str(chronos_output_dir), overwrite=True))
        await send_log(job_id, "Model saved.")

        await manager.send_status("running", "Converting outputs to CSV...", job_id)
        await send_log(job_id, "Converting HDF5 files to CSV...")

        csv_dir = job_dir / "CSVOutputs"
        csv_dir.mkdir(exist_ok=True)

        hdf5_files = list(chronos_output_dir.glob("*.hdf5"))
        await send_log(job_id, f"Found {len(hdf5_files)} HDF5 files to convert.")

        for hdf5_file in hdf5_files:
            try:
                df = chronos.read_hdf5(str(hdf5_file))
                csv_path = csv_dir / f"{hdf5_file.stem}.csv"
                df.to_csv(csv_path)
                await send_log(job_id, f"Converted {hdf5_file.name} -> {csv_path.name}")
            except Exception as e:
                error_msg = f"Failed to convert {hdf5_file.name}: {e}"
                await send_log(job_id, error_msg)
                await manager.send_error(error_msg, job_id)

        # Copy number correction if available
        if copy_number_path:
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

                corrected_path = csv_dir / "gene_effect_corrected.csv"
                corrected.to_csv(corrected_path)
                chronos.write_hdf5(corrected, chronos_output_dir / "gene_effect_corrected.hdf5")
                await send_log(job_id, f"Saved {corrected_path.name}")

                shifts_path = csv_dir / "copy_number_shifts.csv"
                shifts.to_csv(shifts_path)
                await send_log(job_id, f"Saved {shifts_path.name}")
            except Exception as e:
                error_msg = f"Copy number correction failed: {e}.\n Analysis will proceed with \
uncorrected data."
                await send_log(job_id, error_msg)
                print(f"[SERVER] Sending error: {error_msg}", flush=True)
                await manager.send_error(error_msg, job_id)
                # Longer delay to let client process error before next messages
                await asyncio.sleep(0.5)

        # Generate dataset QC report
        await manager.send_status("running", "Generating dataset QC report...", job_id)
        await send_log(job_id, "Generating dataset QC report...")

        reports_dir = job_manager.get_reports_dir(job_id)
        title_file = job_dir / "title.txt"
        title = title_file.read_text().strip() if title_file.exists() else job_id

        # Use corrected gene effect if available, otherwise base
        corrected_hdf5 = chronos_output_dir / "gene_effect_corrected.hdf5"
        gene_effect_file = "gene_effect_corrected.hdf5" if corrected_hdf5.exists() else "gene_effect.hdf5"
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
                pass  # Skip copy number in report if loading fails

        try:
            await asyncio.to_thread(
                chronos.reports.dataset_qc_report,
                title=title + "post chronos",
                data=str(chronos_output_dir),
                positive_control_genes=positive_controls,
                negative_control_genes=negative_controls,
                copy_number=cn_for_report,
                directory=str(reports_dir),
                gene_effect_file=gene_effect_file,
            )
            await send_log(job_id, "Dataset QC report generated.")
        except Exception as e:
            error_msg = f"Dataset QC report failed: {e}"
            await send_log(job_id, error_msg)
            await manager.send_error(error_msg, job_id)
            await asyncio.sleep(0.5)

        job_manager.mark_chronos_completed()
        await send_log(job_id, "Chronos analysis complete!")
        print(f"[SERVER] Sending chronos_complete status for job {job_id}", flush=True)
        await manager.send_status(
            "chronos_complete",
            "Chronos analysis complete!",
            job_id,
            {"output_dir": str(csv_dir)}
        )
        print(f"[SERVER] chronos_complete status sent", flush=True)

    except Exception as e:
        error_msg = traceback.format_exc()
        await send_log(job_id, f"ERROR: {error_msg}")
        await manager.send_error(str(e), job_id)
        # Delay to let client process error popup
        await asyncio.sleep(0.5)


async def send_log(job_id: str, message: str):
    """Send a log message to the client and append to log file."""
    print(f"[SEND_LOG] {message}", flush=True)

    # Append to log file
    log_path = job_manager.get_log_path(job_id)
    with open(log_path, "a") as f:
        f.write(message + "\n")
        f.flush()

    # Send to client
    await manager.broadcast({
        "type": "log",
        "job_id": job_id,
        "message": message,
    })
    # Yield to event loop to ensure message is sent
    await asyncio.sleep(0)


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
    """List all CSV output files for a job from both ChronosOutput and CSVOutputs."""
    job_dir = job_manager.get_job_dir(job_id)
    csv_dir = job_dir / "CSVOutputs"
    chronos_dir = job_dir / "ChronosOutput"

    files = []

    # Collect CSVs from ChronosOutput directory
    if chronos_dir.exists():
        for csv_file in chronos_dir.glob("*.csv"):
            files.append({
                "name": csv_file.name,
                "size": csv_file.stat().st_size,
                "source": "ChronosOutput",
            })

    # Collect CSVs from CSVOutputs directory (converted HDF5 files)
    if csv_dir.exists():
        for csv_file in csv_dir.glob("*.csv"):
            files.append({
                "name": csv_file.name,
                "size": csv_file.stat().st_size,
                "source": "CSVOutputs",
            })

    if not files:
        raise HTTPException(status_code=404, detail="No outputs found")

    # Sort alphabetically by filename
    files.sort(key=lambda f: f["name"].lower())

    return {"job_id": job_id, "files": files}


@router.get("/outputs/{job_id}/download/{filename}")
async def download_output(job_id: str, filename: str, source: str = "CSVOutputs"):
    """Download a single output file."""
    job_dir = job_manager.get_job_dir(job_id)

    # Check in the specified source directory
    if source == "ChronosOutput":
        file_path = job_dir / "ChronosOutput" / filename
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

    # Get job title for zip filename
    title_file = job_dir / "title.txt"
    title = title_file.read_text().strip() if title_file.exists() else job_id
    zip_filename = f"{title}_outputs.zip"

    # Create zip in memory
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        for file_info in request.files:
            if file_info.source == "ChronosOutput":
                file_path = job_dir / "ChronosOutput" / file_info.name
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
