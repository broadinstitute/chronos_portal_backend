from fastapi import APIRouter, UploadFile, File, Form, HTTPException
from pathlib import Path
from typing import Optional

from ..services.job_manager import job_manager
from ..services.file_utils import parse_file, parse_gene_list

router = APIRouter()

# Path to built-in library guide maps
LIBRARIES_DIR = Path(__file__).parent.parent / "data" / "guide_maps"


@router.get("/libraries")
async def list_libraries():
    """List available built-in sgRNA libraries."""
    libraries = []
    if LIBRARIES_DIR.exists():
        for f in sorted(LIBRARIES_DIR.glob("*.csv")):
            libraries.append(f.stem)
    return {"libraries": libraries}


@router.post("/set-library/{library_name}")
async def set_library(
    library_name: str,
    job_id: Optional[str] = Form(None),
    job_name: Optional[str] = Form(None),
):
    """Set a built-in library as the guide map for a job."""
    library_path = LIBRARIES_DIR / f"{library_name}.csv"
    if not library_path.exists():
        raise HTTPException(status_code=404, detail=f"Library not found: {library_name}")

    # Resume existing job or create new one
    if job_id:
        job_manager.resume_job(job_id)
    elif not job_manager.current_job_id:
        job_manager.create_job(job_name)

    # Store the library path directly (no need to copy)
    job_manager.store_file_path("guide_map", library_path)
    job_manager.add_file_info("guide_map", f"{library_name}.csv", "csv", library_path)

    return {
        "status": "success",
        "job_id": job_manager.current_job_id,
        "library": library_name,
    }


@router.post("/upload/{file_type}")
async def upload_file(
    file_type: str,
    file: UploadFile = File(...),
    file_format: str = Form("csv"),
    job_id: Optional[str] = Form(None),
    job_name: Optional[str] = Form(None),
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
        job_manager.resume_job(job_id)
    elif not job_manager.current_job_id:
        job_manager.create_job(job_name)

    uploads_dir = job_manager.get_uploads_dir(job_manager.current_job_id)
    file_path = uploads_dir / f"{file_type}.{file_format}"

    content = await file.read()
    with open(file_path, "wb") as f:
        f.write(content)

    job_manager.store_file_path(file_type, file_path)
    job_manager.add_file_info(file_type, file.filename, file_format, file_path)

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
            return {
                "status": "success",
                "job_id": job_manager.current_job_id,
                "file_type": file_type,
                "filename": file.filename,
                "rows": len(df),
                "columns": len(df.columns),
            }
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error parsing file: {str(e)}")


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
