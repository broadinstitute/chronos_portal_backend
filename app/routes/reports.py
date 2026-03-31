from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse
from pathlib import Path
import json

from ..services.job_manager import job_manager

router = APIRouter()


@router.get("/jobs")
async def list_jobs():
    """List all jobs that have completed QC (have a Reports directory with content)."""
    jobs = []
    jobs_dir = job_manager.jobs_dir

    if not jobs_dir.exists():
        return {"jobs": []}

    for job_dir in sorted(jobs_dir.iterdir(), reverse=True):
        if not job_dir.is_dir():
            continue

        reports_dir = job_dir / "Reports"
        # Check if QC has completed (has PNG files)
        if reports_dir.exists() and list(reports_dir.glob("*.png")):
            title_file = job_dir / "title.txt"
            title = title_file.read_text().strip() if title_file.exists() else job_dir.name

            jobs.append({
                "job_id": job_dir.name,
                "title": title,
            })

    return {"jobs": jobs}

SECTIONS = [
    {
        "id": "lfc_distribution",
        "title": "sgRNA Log Fold-Change Distribution",
        "text": "For a traditional genome-wide loss of viability experiment we expect the bulk of log fold change scores near 0, with a long left tail of true viability depletion.",
        "image": "lfc_distribution.png",
    },
    {
        "id": "control_qc",
        "title": "Control QC Metrics",
        "text": """Depletion of positive controls is a positive signal for screen quality, while high standard deviation in negative controls is a negative signal for screen quality. However, these measures tend to be negatively correlated in CRISPR screens: screens that show the greatest dropout of essential genes also have the greatest noise in nonessential genes.

The null-normalized median difference (NNMD) is ((median(positive controls) - median(negative controls)) / mad(negative controls). In Project Achilles, we look for NNMD scores below -1.25 to consider a replicate passing but this threshold depends strongly on the controls you have chosen. We also provide the area under the ROC curve for separating the positive and negative control log fold changes. These measures should have a strong negative correlation.""",
        "image": "control_sep.png",
    },
    {
        "id": "replicate_correlation",
        "title": "Replicate Correlation",
        "text": "Below is the Pearson correlation of replicate Log Fold-Change with the mean LFC over all replicates (x axis) vs the mean correlation with other replicates of the same cell line (y axis). Generally these are closely related and correlate with other measures of screen quality.",
        "image": "replicate_correlations.png",
    },
]


@router.get("/reports/{job_id}")
async def get_report(job_id: str):
    """Get report metadata for a job."""
    job_dir = job_manager.get_job_dir(job_id)
    if not job_dir.exists():
        raise HTTPException(status_code=404, detail="Job not found")

    reports_dir = job_manager.get_reports_dir(job_id)
    if not reports_dir.exists():
        raise HTTPException(status_code=404, detail="Reports not found")

    # Get job title
    title_file = job_dir / "title.txt"
    title = title_file.read_text().strip() if title_file.exists() else "Untitled"

    # Load job config to get status
    config_path = job_dir / "config.json"
    config = {}
    if config_path.exists():
        with open(config_path, "r") as f:
            config = json.load(f)

    # Check which images exist
    sections = []
    for section in SECTIONS:
        image_path = reports_dir / section["image"]
        if image_path.exists():
            sections.append({
                **section,
                "image_url": f"/api/reports/{job_id}/image/{section['image']}",
            })

    return {
        "job_id": job_id,
        "title": title,
        "sections": sections,
        "chronos_completed": config.get("chronos_completed_at") is not None,
    }


@router.get("/reports/{job_id}/image/{filename}")
async def get_report_image(job_id: str, filename: str):
    """Serve a report image."""
    reports_dir = job_manager.get_reports_dir(job_id)
    image_path = reports_dir / filename

    if not image_path.exists():
        raise HTTPException(status_code=404, detail="Image not found")

    return FileResponse(image_path, media_type="image/png")


@router.get("/reports/{job_id}/pdf")
async def get_report_pdf(job_id: str):
    """Serve the PDF report."""
    reports_dir = job_manager.get_reports_dir(job_id)

    # Find the PDF file (named after the job title)
    pdf_files = list(reports_dir.glob("*.pdf"))
    if not pdf_files:
        raise HTTPException(status_code=404, detail="PDF report not found")

    pdf_path = pdf_files[0]
    return FileResponse(
        pdf_path,
        media_type="application/pdf",
        filename=pdf_path.name,
    )
