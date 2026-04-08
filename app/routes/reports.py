from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse
from pathlib import Path
import json
import re
from pypdf import PdfReader
from typing import List

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


@router.get("/jobs/{job_id}/log")
async def get_job_log(job_id: str):
    """Get the log file content for a job."""
    log_path = job_manager.get_log_path(job_id)

    if not log_path.exists():
        return {"job_id": job_id, "log": ""}

    return {"job_id": job_id, "log": log_path.read_text()}


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
    """Serve the initial QC PDF report."""
    job_dir = job_manager.get_job_dir(job_id)
    reports_dir = job_manager.get_reports_dir(job_id)

    title_file = job_dir / "title.txt"
    title = title_file.read_text().strip() if title_file.exists() else "Untitled"

    # Look for initial QC PDF
    pdf_path = reports_dir / f"{title} initial qc.pdf"
    if not pdf_path.exists():
        for f in reports_dir.glob("*initial qc*.pdf"):
            pdf_path = f
            break

    if not pdf_path.exists():
        raise HTTPException(status_code=404, detail="Initial QC PDF report not found")

    return FileResponse(
        pdf_path,
        media_type="application/pdf",
        filename=pdf_path.name,
    )


# Post-Chronos QC report sections
# Mapping based on chronos/reports.py::dataset_qc_report
POST_CHRONOS_SECTIONS = [
    {
        "id": "global_control_separation",
        "title": "Global Control Separation",
        "images": ["global_controls.png"],
    },
    {
        "id": "copy_number_effect",
        "title": "Copy Number Effect",
        "images": ["copy_number_effect.png"],
        "optional": True,  # Only shown if copy number was provided
    },
    {
        "id": "screen_efficacy",
        "title": "Efficacies",
        "pdf_heading": "Screen Efficacy, Growth Rate, and Guide Efficacy",
        "images": ["parameter_distributions.png"],
    },
    {
        "id": "readcount_predictions",
        "title": "Readcount Predictions",
        "images": ["readcount_predictions.png"],
    },
    {
        "id": "lfc_predictions",
        "title": "LFC Predictions",
        "pdf_heading": "Log Fold-Change Predictions",
        "images": ["lfc_corr_vs_excess_variance.png"],
    },
    {
        "id": "gene_score_difference",
        "title": "Gene Scores",
        "pdf_heading": "Gene Score Difference from Naive",
        "images": ["gene_corrs.png", "gene_means.png", "gene_zscore_extremes.png"],
    },
]


def extract_pdf_sections(pdf_path: Path) -> dict[str, str]:
    """Extract text sections from post-Chronos QC PDF."""
    reader = PdfReader(pdf_path)
    full_text = ""
    for page in reader.pages:
        full_text += page.extract_text() + "\n"

    # Section headings we want to extract
    target_headings = [
        "Global Control Separation",
        "Copy Number Effect",
        "Screen Efficacy, Growth Rate, and Guide Efficacy",
        "Readcount Predictions",
        "Log Fold-Change Predictions",
        "Gene Score Difference from Naive",
    ]

    # Pattern to detect any heading-like line:
    # - Starts at beginning of line
    # - Contains mostly title-cased words
    # - Is relatively short (under 80 chars)
    # - Doesn't end with common sentence punctuation
    heading_pattern = re.compile(
        r'^([A-Z][A-Za-z0-9,\-\' ]{2,80})$',
        re.MULTILINE
    )

    # Find all potential headings in the document
    all_headings = [(m.start(), m.group(1).strip()) for m in heading_pattern.finditer(full_text)]

    sections = {}
    for target in target_headings:
        # Find where this target heading starts
        target_match = re.search(re.escape(target), full_text, re.IGNORECASE)
        if not target_match:
            continue

        start = target_match.end()

        # Find the next heading after this section's content
        end = len(full_text)
        for pos, heading_text in all_headings:
            if pos > start and heading_text != target:
                end = pos
                break

        # Extract and clean the text
        text = full_text[start:end].strip()
        # Remove excessive whitespace
        text = re.sub(r'\n{3,}', '\n\n', text)
        sections[target] = text

    return sections


@router.get("/reports/{job_id}/chronos-qc")
async def get_chronos_qc_report(job_id: str):
    """Get post-Chronos QC report sections with text and images."""
    job_dir = job_manager.get_job_dir(job_id)
    if not job_dir.exists():
        raise HTTPException(status_code=404, detail="Job not found")

    reports_dir = job_manager.get_reports_dir(job_id)
    if not reports_dir.exists():
        raise HTTPException(status_code=404, detail="Reports not found")

    # Get job title
    title_file = job_dir / "title.txt"
    title = title_file.read_text().strip() if title_file.exists() else "Untitled"

    # Find the post-chronos PDF
    pdf_name = f"{title} chronos qc.pdf"
    pdf_path = reports_dir / pdf_name

    if not pdf_path.exists():
        # Try to find any chronos qc pdf
        for f in reports_dir.glob("*chronos qc*.pdf"):
            pdf_path = f
            break

    if not pdf_path.exists():
        raise HTTPException(status_code=404, detail="Post-Chronos QC report not found")

    # Extract text from PDF
    pdf_sections = extract_pdf_sections(pdf_path)

    # Build response sections
    sections = []
    for section_def in POST_CHRONOS_SECTIONS:
        # Check if images exist
        image_urls = []
        for img in section_def["images"]:
            img_path = reports_dir / img
            if img_path.exists():
                image_urls.append(f"/api/reports/{job_id}/image/{img}")

        # Skip optional sections with no images
        if section_def.get("optional") and not image_urls:
            continue

        # Skip sections with no images at all
        if not image_urls:
            continue

        # Use pdf_heading for text lookup if present, otherwise use title
        pdf_heading = section_def.get("pdf_heading", section_def["title"])
        sections.append({
            "id": section_def["id"],
            "title": section_def["title"],
            "text": pdf_sections.get(pdf_heading, ""),
            "image_urls": image_urls,
        })

    return {
        "job_id": job_id,
        "title": title,
        "sections": sections,
    }


# Hit Calling Report sections
HITS_SECTIONS = [
    {
        "id": "fdr",
        "title": "False Discovery Rates",
        "pdf_heading": "False Discovery Rates",
        "image_pattern": "fdr_volcano_*.png",
    },
    {
        "id": "specific_biology",
        "title": "Specific Biology",
        "pdf_heading": "Specific Biology",
        "image_pattern": "select_dependencies_*.png",
    },
]


def extract_hits_pdf_sections(pdf_path: Path) -> dict[str, str]:
    """Extract text sections from hit calling report PDF."""
    reader = PdfReader(pdf_path)
    full_text = ""
    for page in reader.pages:
        full_text += page.extract_text() + "\n"

    target_headings = ["False Discovery Rates", "Specific Biology"]

    heading_pattern = re.compile(
        r'^([A-Z][A-Za-z0-9,\-\' ]{2,80})$',
        re.MULTILINE
    )

    all_headings = [(m.start(), m.group(1).strip()) for m in heading_pattern.finditer(full_text)]

    sections = {}
    for target in target_headings:
        target_match = re.search(re.escape(target), full_text, re.IGNORECASE)
        if not target_match:
            continue

        start = target_match.end()
        end = len(full_text)
        for pos, heading_text in all_headings:
            if pos > start and heading_text != target:
                end = pos
                break

        text = full_text[start:end].strip()
        text = re.sub(r'\n{3,}', '\n\n', text)
        sections[target] = text

    return sections


@router.get("/reports/{job_id}/hits")
async def get_hits_report(job_id: str):
    """Get hit calling report sections with text and images."""
    job_dir = job_manager.get_job_dir(job_id)
    if not job_dir.exists():
        raise HTTPException(status_code=404, detail="Job not found")

    reports_dir = job_manager.get_reports_dir(job_id)
    if not reports_dir.exists():
        raise HTTPException(status_code=404, detail="Reports not found")

    # Get job title
    title_file = job_dir / "title.txt"
    title = title_file.read_text().strip() if title_file.exists() else "Untitled"

    # Find the hit calling PDF
    pdf_path = reports_dir / f"{title} hits.pdf"
    if not pdf_path.exists():
        # Try to find any hits pdf
        for f in reports_dir.glob("*hits*.pdf"):
            pdf_path = f
            break
    if not pdf_path.exists():
        raise HTTPException(status_code=404, detail="Hit calling report not found")

    # Extract text from PDF
    pdf_sections = extract_hits_pdf_sections(pdf_path)

    # Build response sections
    sections = []
    for section_def in HITS_SECTIONS:
        # Find images matching the pattern
        image_urls = []
        for img_path in sorted(reports_dir.glob(section_def["image_pattern"])):
            image_urls.append(f"/api/reports/{job_id}/image/{img_path.name}")

        # Skip sections with no images
        if not image_urls:
            continue

        pdf_heading = section_def.get("pdf_heading", section_def["title"])
        sections.append({
            "id": section_def["id"],
            "title": section_def["title"],
            "text": pdf_sections.get(pdf_heading, ""),
            "image_urls": image_urls,
        })

    return {
        "job_id": job_id,
        "title": title,
        "sections": sections,
    }


@router.get("/reports/{job_id}/hits/pdf")
async def get_hits_pdf(job_id: str):
    """Serve the hit calling PDF report."""
    job_dir = job_manager.get_job_dir(job_id)
    reports_dir = job_manager.get_reports_dir(job_id)

    title_file = job_dir / "title.txt"
    title = title_file.read_text().strip() if title_file.exists() else "Untitled"

    pdf_path = reports_dir / f"{title} hits.pdf"
    if not pdf_path.exists():
        for f in reports_dir.glob("*hits*.pdf"):
            pdf_path = f
            break

    if not pdf_path.exists():
        raise HTTPException(status_code=404, detail="Hit calling report PDF not found")

    return FileResponse(
        pdf_path,
        media_type="application/pdf",
        filename=pdf_path.name,
    )


# Differential Dependency Report sections
DD_SECTIONS = [
    {
        "id": "differential_dependency",
        "title": "Differential Dependency",
        "pdf_heading": "Differential Dependency",
        "image_patterns": [
            "differential_dependency_*.png",
            "diffdep_up_genetea_enrichment_*.png",
            "diffdep_down_genetea_enrichment_*.png",
        ],
    },
]


def extract_dd_pdf_sections(pdf_path: Path) -> dict[str, str]:
    """Extract text sections from differential dependency report PDF."""
    reader = PdfReader(pdf_path)
    full_text = ""
    for page in reader.pages:
        full_text += page.extract_text() + "\n"

    target_headings = ["Differential Dependency"]

    heading_pattern = re.compile(
        r'^([A-Z][A-Za-z0-9,\-\' ]{2,80})$',
        re.MULTILINE
    )

    all_headings = [(m.start(), m.group(1).strip()) for m in heading_pattern.finditer(full_text)]

    sections = {}
    for target in target_headings:
        target_match = re.search(re.escape(target), full_text, re.IGNORECASE)
        if not target_match:
            continue

        start = target_match.end()
        end = len(full_text)
        for pos, heading_text in all_headings:
            if pos > start and heading_text != target:
                end = pos
                break

        text = full_text[start:end].strip()
        text = re.sub(r'\n{3,}', '\n\n', text)
        sections[target] = text

    return sections


@router.get("/reports/{job_id}/differential-dependency")
async def get_dd_report(job_id: str):
    """Get differential dependency report sections with text and images."""
    job_dir = job_manager.get_job_dir(job_id)
    if not job_dir.exists():
        raise HTTPException(status_code=404, detail="Job not found")

    reports_dir = job_manager.get_reports_dir(job_id)
    if not reports_dir.exists():
        raise HTTPException(status_code=404, detail="Reports not found")

    # Get job title
    title_file = job_dir / "title.txt"
    title = title_file.read_text().strip() if title_file.exists() else "Untitled"

    # Find the differential dependency PDF
    pdf_path = reports_dir / f"{title} differential dependency report.pdf"
    if not pdf_path.exists():
        for f in reports_dir.glob("*differential dependency*.pdf"):
            pdf_path = f
            break
    if not pdf_path.exists():
        raise HTTPException(status_code=404, detail="Differential dependency report not found")

    # Extract text from PDF
    pdf_sections = extract_dd_pdf_sections(pdf_path)

    # Build response sections
    sections = []
    for section_def in DD_SECTIONS:
        # Collect all images matching any of the patterns
        image_urls = []
        for pattern in section_def["image_patterns"]:
            for img_path in sorted(reports_dir.glob(pattern)):
                image_urls.append(f"/api/reports/{job_id}/image/{img_path.name}")

        # Skip sections with no images
        if not image_urls:
            continue

        pdf_heading = section_def.get("pdf_heading", section_def["title"])
        sections.append({
            "id": section_def["id"],
            "title": section_def["title"],
            "text": pdf_sections.get(pdf_heading, ""),
            "image_urls": image_urls,
        })

    return {
        "job_id": job_id,
        "title": title,
        "sections": sections,
    }


@router.get("/reports/{job_id}/differential-dependency/pdf")
async def get_dd_pdf(job_id: str):
    """Serve the differential dependency PDF report."""
    job_dir = job_manager.get_job_dir(job_id)
    reports_dir = job_manager.get_reports_dir(job_id)

    title_file = job_dir / "title.txt"
    title = title_file.read_text().strip() if title_file.exists() else "Untitled"

    pdf_path = reports_dir / f"{title} differential dependency report.pdf"
    if not pdf_path.exists():
        for f in reports_dir.glob("*differential dependency*.pdf"):
            pdf_path = f
            break

    if not pdf_path.exists():
        raise HTTPException(status_code=404, detail="Differential dependency report PDF not found")

    return FileResponse(
        pdf_path,
        media_type="application/pdf",
        filename=pdf_path.name,
    )
