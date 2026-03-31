from fastapi import APIRouter, UploadFile, File, Form, HTTPException
from pathlib import Path
from typing import Optional
import pandas as pd

from ..services.job_manager import job_manager

router = APIRouter()


def parse_file(file_path: Path, file_format: str) -> pd.DataFrame:
    sep = "\t" if file_format == "tsv" else ","
    return pd.read_csv(file_path, sep=sep, index_col=0)


def parse_gene_list(file_path: Path) -> list[str]:
    with open(file_path, "r") as f:
        return [line.strip() for line in f if line.strip()]


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
    job_manager.add_file_info(file_type, file.filename, file_format)

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
