from fastapi import APIRouter, UploadFile, File, Form, HTTPException
from pathlib import Path
from typing import Optional
import json

from ..services.job_manager import job_manager
from ..services.file_utils import (
    parse_file,
    parse_gene_list,
    is_sequence_format,
    is_archive,
    extract_archive,
)

router = APIRouter()

# Path to built-in library data
DATA_DIR = Path(__file__).parent.parent / "data"
LIBRARIES_DIR = DATA_DIR / "guide_maps"
LIBRARY_MAP_PATH = DATA_DIR / "library_map.json"


def load_library_map():
    """Load the library map from JSON."""
    if LIBRARY_MAP_PATH.exists():
        with open(LIBRARY_MAP_PATH) as f:
            return json.load(f)
    return {}


@router.get("/libraries")
async def list_libraries():
    """List available built-in sgRNA libraries."""
    library_map = load_library_map()
    return {"libraries": list(library_map.keys())}


@router.post("/set-library/{library_name}")
async def set_library(
    library_name: str,
    job_id: Optional[str] = Form(None),
    job_name: Optional[str] = Form(None),
):
    """Set a built-in library as the guide map for a job."""
    library_map = load_library_map()

    if library_name not in library_map:
        raise HTTPException(status_code=404, detail=f"Library not found: {library_name}")

    library_info = library_map[library_name]
    guide_map_file = library_info["guide_map"]
    library_label = library_info["library_label"]
    library_path = LIBRARIES_DIR / guide_map_file

    if not library_path.exists():
        raise HTTPException(status_code=404, detail=f"Guide map file not found: {guide_map_file}")

    # Resume existing job or create new one
    if job_id:
        job_manager.resume_job(job_id, job_name)
    else:
        # No job_id provided means client is starting fresh - create new job
        job_manager.create_job(job_name)

    # Store the library path and label
    job_manager.store_file_path("guide_map", library_path)
    job_manager.add_file_info("guide_map", guide_map_file, "csv", library_path)
    job_manager.set_library_label(library_label)

    return {
        "status": "success",
        "job_id": job_manager.current_job_id,
        "library": library_name,
        "library_label": library_label,
    }


@router.post("/upload/{file_type}")
async def upload_file(
    file_type: str,
    file: UploadFile = File(...),
    file_format: str = Form("csv"),
    job_id: Optional[str] = Form(None),
    job_name: Optional[str] = Form(None),
    file_index: Optional[str] = Form(None),  # For multi-file uploads
    total_files: Optional[str] = Form(None),  # For multi-file uploads
):
    valid_types = [
        "readcounts",
        "condition_map",
        "guide_map",
        "copy_number",
        "positive_controls",
        "negative_controls",
    ]

    if file_type not in valid_types:
        raise HTTPException(status_code=400, detail=f"Invalid file type: {file_type}")

    # Resume existing job or create new one
    if job_id:
        job_manager.resume_job(job_id, job_name)
    else:
        # No job_id provided means client is starting fresh - create new job
        job_manager.create_job(job_name)

    uploads_dir = job_manager.get_uploads_dir(job_manager.current_job_id)

    # Handle sequence format readcounts specially
    if file_type == "readcounts" and is_sequence_format(file_format):
        return await _handle_sequence_readcount_upload(
            file, file_format, uploads_dir, file_index, total_files
        )

    # Standard upload path for non-sequence formats
    file_path = uploads_dir / f"{file_type}.{file_format}"

    content = await file.read()
    with open(file_path, "wb") as f:
        f.write(content)

    job_manager.store_file_path(file_type, file_path)
    job_manager.add_file_info(file_type, file.filename, file_format, file_path)

    # If uploading a custom guide map, set library label to "custom"
    if file_type == "guide_map":
        job_manager.set_library_label("custom")

    try:
        if file_type in ["positive_controls", "negative_controls"]:
            items = parse_gene_list(file_path)
            return {
                "status": "success",
                "job_id": job_manager.current_job_id,
                "file_type": file_type,
                "filename": file.filename,
                "item_count": len(items),
            }
        else:
            df = parse_file(file_path, file_format)

            # Extract available conditions from condition_map
            if file_type == "condition_map" and "condition" in df.columns:
                conditions = (
                    df["condition"]
                    .dropna()
                    .loc[lambda x: x != "pDNA"]
                    .unique()
                    .tolist()
                )
                job_manager.set_available_conditions(conditions)

            return {
                "status": "success",
                "job_id": job_manager.current_job_id,
                "file_type": file_type,
                "filename": file.filename,
                "rows": len(df),
                "columns": len(df.columns),
            }
    except UnicodeDecodeError:
        raise HTTPException(
            status_code=400,
            detail="Error parsing file: Unable to read file as text. Did you select the wrong file format?"
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error parsing file: {str(e)}")


async def _handle_sequence_readcount_upload(
    file: UploadFile,
    file_format: str,
    uploads_dir: Path,
    file_index: Optional[str],
    total_files: Optional[str],
):
    """Handle upload of sequence format readcount files (fastq/bam/sam).

    Supports:
    - Single file upload
    - Multi-file upload (with file_index and total_files)
    - Archive upload (zip/gz) with automatic extraction
    """
    # Create readcounts subdirectory for sequence files
    readcounts_dir = uploads_dir / "readcounts"
    readcounts_dir.mkdir(exist_ok=True)

    # Clear previous readcount files on first upload of a batch
    if file_index is None or file_index == "0":
        job_manager.clear_readcount_files()
        job_manager.set_readcount_format(file_format)

    # Save the uploaded file
    saved_path = readcounts_dir / file.filename
    content = await file.read()
    with open(saved_path, "wb") as f:
        f.write(content)

    # Check if file is an archive and extract
    if is_archive(file.filename):
        try:
            extracted_files = extract_archive(saved_path, readcounts_dir)
            # Remove the archive after extraction
            saved_path.unlink()

            # Track all extracted files
            for extracted_path in extracted_files:
                job_manager.add_readcount_file(extracted_path)

            return {
                "status": "success",
                "job_id": job_manager.current_job_id,
                "file_type": "readcounts",
                "filename": file.filename,
                "extracted_files": len(extracted_files),
                "total_readcount_files": job_manager.get_readcount_count(),
            }
        except Exception as e:
            raise HTTPException(
                status_code=400,
                detail=f"Error extracting archive: {str(e)}"
            )
    else:
        # Single file or one of multiple files
        job_manager.add_readcount_file(saved_path)

        return {
            "status": "success",
            "job_id": job_manager.current_job_id,
            "file_type": "readcounts",
            "filename": file.filename,
            "file_index": file_index,
            "total_files": total_files,
            "total_readcount_files": job_manager.get_readcount_count(),
        }


@router.post("/readcount-options")
async def set_readcount_options(request: dict):
    """Store sequence format options for raw reads processing.

    Expected fields:
    - job_id: str (required)
    - readType: "single" | "paired"
    - sgrnaFindMethod: "fixed" | "prefix" | "template"
    - sgrnaStart, sgrnaEnd, sgrnaPrefix, sgrnaTemplate, etc.
    - sampleFindMethod, sampleStart, etc.
    - reverseSampleFindMethod (if paired)
    - countAmbiguous: bool
    - errorIfTooShort: bool
    """
    job_id = request.get("job_id")
    if not job_id:
        raise HTTPException(status_code=400, detail="job_id is required")

    job_manager.resume_job(job_id)

    # Store all options except job_id
    options = {k: v for k, v in request.items() if k != "job_id"}
    job_manager.set_readcount_options(options)

    return {
        "status": "success",
        "job_id": job_id,
        "options_saved": True,
    }


@router.post("/new-job")
async def create_new_job():
    job_manager.clear_uploads()
    job_id = job_manager.create_job()
    return {"job_id": job_id}


@router.get("/job-status")
async def get_job_status():
    return {
        "job_id": job_manager.current_job_id,
        "uploaded_files": list(job_manager.uploaded_files.keys()),
    }
