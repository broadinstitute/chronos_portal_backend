from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel
from pathlib import Path
from typing import Optional
import asyncio
import sys
import io
import zipfile

from ..services.job_manager import job_manager
from ..services.connection_manager import manager

router = APIRouter()


def parse_file(file_path: Path, file_format: str, *args, **kwargs):
    import pandas as pd
    sep = "\t" if file_format == "tsv" else ","
    return pd.read_csv(file_path, sep=sep, *args, **kwargs)


def parse_gene_list(file_path: Path) -> list[str]:
    with open(file_path, "r") as f:
        return [line.strip() for line in f if line.strip()]


class ChronosRequest(BaseModel):
    job_id: str


async def run_chronos_analysis(job_id: str):
    """Run Chronos analysis and stream stdout to client."""
    import chronos
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

        negative_controls = []
        if negative_controls_path:
            negative_controls = parse_gene_list(negative_controls_path)

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

        class OutputCapture:
            def __init__(self, original, job_id):
                self.original = original
                self.job_id = job_id
                self.buffer = ""

            def write(self, text):
                self.original.write(text)
                self.buffer += text
                if "\n" in self.buffer:
                    lines = self.buffer.split("\n")
                    for line in lines[:-1]:
                        if line.strip():
                            asyncio.create_task(send_log(self.job_id, line))
                    self.buffer = lines[-1]

            def flush(self):
                self.original.flush()

        old_stdout = sys.stdout
        old_stderr = sys.stderr
        sys.stdout = OutputCapture(old_stdout, job_id)
        sys.stderr = OutputCapture(old_stderr, job_id)

        # Initialize model (custom library, no pretrained)
        model = chronos.Chronos(
            readcounts={"library": readcounts},
            sequence_map={"library": sequence_map},
            guide_gene_map={"library": guide_map},
            negative_control_sgrnas={"library": negative_control_sgrnas},
        )

        await manager.send_status("running", "Training Chronos model (this may take a while)...", job_id)
        await send_log(job_id, "Training Chronos model...")

        # Capture stdout during training
        

        try:
            model.train()
        finally:
            sys.stdout = old_stdout
            sys.stderr = old_stderr

        await manager.send_status("running", "Saving model outputs...", job_id)
        await send_log(job_id, "Saving model outputs...")

        # Save model
        model.save(str(chronos_output_dir), overwrite=True)

        # Get gene effects for normalization
        gene_effects = model.gene_effect

        # Copy number correction if available
        if copy_number_path:
            await send_log(job_id, "Applying copy number correction...")
            cn = parse_file(
                copy_number_path,
                job_manager.get_file_format("copy_number"),
                index_col=0
            )
            try:
                corrected, shifts = chronos.alternate_CN(gene_effects, cn)
                chronos.write_hdf5(corrected, str(chronos_output_dir / "gene_effect_corrected.hdf5"))
            except Exception as e:
                await send_log(job_id, f"Copy number correction failed: {e}")

        await manager.send_status("running", "Converting outputs to CSV...", job_id)
        await send_log(job_id, "Converting HDF5 files to CSV...")

        # Convert all HDF5 files to CSV
        csv_dir = job_dir / "CSVOutputs"
        csv_dir.mkdir(exist_ok=True)

        for hdf5_file in chronos_output_dir.glob("*.hdf5"):
            try:
                df = chronos.read_hdf5(str(hdf5_file))
                csv_path = csv_dir / f"{hdf5_file.stem}.csv"
                df.to_csv(csv_path)
                await send_log(job_id, f"Converted {hdf5_file.name} -> {csv_path.name}")
            except Exception as e:
                await send_log(job_id, f"Failed to convert {hdf5_file.name}: {e}")

        job_manager.mark_chronos_completed()
        await send_log(job_id, "Chronos analysis complete!")
        await manager.send_status(
            "chronos_complete",
            "Chronos analysis complete!",
            job_id,
            {"output_dir": str(csv_dir)}
        )

    except Exception as e:
        error_msg = traceback.format_exc()
        await send_log(job_id, f"ERROR: {error_msg}")
        await manager.send_error(str(e), job_id)


async def send_log(job_id: str, message: str):
    """Send a log message to the client and append to log file."""
    # Append to log file
    log_path = job_manager.get_log_path(job_id)
    with open(log_path, "a") as f:
        f.write(message + "\n")

    # Send to client
    await manager.broadcast({
        "type": "log",
        "job_id": job_id,
        "message": message,
    })


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

    # Add CSVs from ChronosOutput directory
    if chronos_dir.exists():
        for csv_file in sorted(chronos_dir.glob("*.csv")):
            files.append({
                "name": csv_file.name,
                "size": csv_file.stat().st_size,
                "source": "ChronosOutput",
            })

    # Add CSVs from CSVOutputs directory (converted HDF5 files)
    if csv_dir.exists():
        for csv_file in sorted(csv_dir.glob("*.csv")):
            files.append({
                "name": csv_file.name,
                "size": csv_file.stat().st_size,
                "source": "CSVOutputs",
            })

    if not files:
        raise HTTPException(status_code=404, detail="No outputs found")

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
