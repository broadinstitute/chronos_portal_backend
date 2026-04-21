"""Preprocessing routes for sequence read processing."""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional
import asyncio

from ..services.job_manager import job_manager
from ..services.connection_manager import manager
from ..services.validation import validate_condition_map_for_sequence_reads
from ..services.logging_utils import send_log, send_error

router = APIRouter()


async def preprocess_sequence_reads(job_id: str):
    """Convert raw fastq/bam/sam files to a readcount matrix.

    This orchestrator handles the conversion of raw sequencing reads into
    a readcount matrix suitable for Chronos analysis.

    TODO: Actual implementation of sequence parsing
    Currently a placeholder that validates inputs and marks preprocessing complete.

    Steps (when fully implemented):
    1. Load readcount_options from job_manager
    2. Load all sequence readcount files
    3. Parse reads using find methods (fixed/prefix/template)
    4. Extract sgRNA barcodes and sample barcodes
    5. Build readcount matrix (samples × guides)
    6. Save as CSV to uploads/readcounts.csv
    7. Mark preprocessing complete
    """
    try:
        await send_log(job_id, "Starting sequence read preprocessing...")

        # Validate condition_map structure
        await send_log(job_id, "Validating condition map...")
        validate_condition_map_for_sequence_reads()

        # Get readcount files and options
        readcount_files = job_manager.get_readcount_files()
        readcount_options = job_manager.get_readcount_options()

        await send_log(job_id, f"Found {len(readcount_files)} readcount file(s) to process...")

        # Log the options being used
        read_type = readcount_options.get("readType", "single")
        sgrna_method = readcount_options.get("sgrnaFindMethod", "fixed")
        sample_method = readcount_options.get("sampleFindMethod", "fixed")

        await send_log(
            job_id,
            f"Processing with: read_type={read_type}, sgRNA_method={sgrna_method}, sample_method={sample_method}"
        )

        # Run PoolQ pipeline to process raw reads
        from ..services.poolq import process_reads_with_poolq
        await send_log(job_id, "Processing reads with PoolQ...")
        readcounts = await process_reads_with_poolq(job_id)
        await send_log(job_id, f"Generated readcount matrix: {readcounts.shape}")

        job_manager.mark_preprocessing_complete()

        await send_log(job_id, "Sequence read preprocessing complete!")
        await manager.send_status(
            "preprocessing_complete",
            "Sequence read preprocessing complete",
            job_id,
            {
                "readcount_files": len(readcount_files),
            }
        )

    except HTTPException as e:
        # Re-raise HTTP exceptions (validation errors)
        await send_error(job_id, e, "Preprocessing")
        raise
    except Exception as e:
        await send_error(job_id, e, "Preprocessing")
        raise HTTPException(status_code=500, detail=str(e))


class PreprocessingRequest(BaseModel):
    job_id: str


@router.post("/run-preprocessing")
async def start_preprocessing(request: PreprocessingRequest):
    """Start sequence read preprocessing for a job.

    This endpoint triggers the conversion of raw sequencing reads (fastq/bam/sam)
    into a readcount matrix. Must be called before running QC for sequence formats.
    """
    job_manager.resume_job(request.job_id)

    if not job_manager.current_job_id:
        raise HTTPException(status_code=400, detail="Job not found")

    # Check this is actually a sequence format job
    if not job_manager.is_sequence_format():
        raise HTTPException(
            status_code=400,
            detail="Preprocessing is only required for sequence formats (fastq/bam/sam)"
        )

    # Check we have readcount files
    if job_manager.get_readcount_count() == 0:
        raise HTTPException(
            status_code=400,
            detail="No readcount files found. Please upload readcount files first."
        )

    # Check condition_map is uploaded
    if not job_manager.get_file_path("condition_map"):
        raise HTTPException(
            status_code=400,
            detail="condition_map is required for preprocessing"
        )

    job_manager.mark_preprocessing_started()

    # Run preprocessing as async task
    asyncio.create_task(preprocess_sequence_reads(request.job_id))

    return {
        "status": "started",
        "job_id": request.job_id,
        "message": "Sequence read preprocessing started"
    }


@router.get("/preprocessing-status/{job_id}")
async def get_preprocessing_status(job_id: str):
    """Get the preprocessing status for a job."""
    job_manager.resume_job(job_id)

    if not job_manager.current_job_id:
        raise HTTPException(status_code=404, detail="Job not found")

    is_sequence = job_manager.is_sequence_format()
    is_complete = job_manager.is_preprocessing_complete()

    return {
        "job_id": job_id,
        "is_sequence_format": is_sequence,
        "preprocessing_required": is_sequence,
        "preprocessing_complete": is_complete,
        "readcount_files": job_manager.get_readcount_count(),
    }
