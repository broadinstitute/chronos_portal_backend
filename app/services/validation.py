"""Validation utilities for uploaded files and job state."""

from fastapi import HTTPException

from .job_manager import job_manager
from .file_utils import parse_file


def validate_condition_map_for_sequence_reads():
    """Validate condition_map has required columns for sequence format readcounts.

    Rules depend on alreadyDeconvoluted option:
    - If alreadyDeconvoluted=True: 'file' column required, 'barcode' optional
    - If alreadyDeconvoluted=False: 'barcode' column required

    When 'file' column is present, validates all entries match uploaded filenames.

    Raises:
        HTTPException: If validation fails
    """
    if not job_manager.is_sequence_format():
        return  # Only validate for sequence formats

    readcount_files = job_manager.get_readcount_files()
    file_count = len(readcount_files)

    if file_count == 0:
        raise HTTPException(
            status_code=400,
            detail="No readcount files found. Please upload readcount files."
        )

    # Load condition_map
    condition_map_path = job_manager.get_file_path("condition_map")
    condition_map_format = job_manager.get_file_format("condition_map")
    condition_map = parse_file(condition_map_path, condition_map_format)

    has_barcode = "barcode" in condition_map.columns
    has_file = "file" in condition_map.columns

    # Check alreadyDeconvoluted option
    readcount_options = job_manager.get_readcount_options()
    already_deconvoluted = readcount_options.get("alreadyDeconvoluted", False)

    if already_deconvoluted:
        # Already deconvoluted: 'file' column required, 'barcode' optional
        if not has_file:
            raise HTTPException(
                status_code=400,
                detail="condition_map must have 'file' column when 'Already deconvoluted' is checked"
            )
        # Validate all file entries match uploaded filenames
        uploaded_names = {f.name for f in readcount_files}
        file_entries = set(condition_map["file"])

        missing = file_entries - uploaded_names
        if missing:
            raise HTTPException(
                status_code=400,
                detail=f"condition_map 'file' column contains unknown files: {', '.join(sorted(missing))}"
            )
    else:
        # Not deconvoluted: 'barcode' column required
        if not has_barcode:
            raise HTTPException(
                status_code=400,
                detail="condition_map must have 'barcode' column when using a raw sequencing format for reads"
            )


def validate_preprocessing_complete():
    """Check that preprocessing has been completed for sequence format reads.

    For sequence formats (fastq/bam/sam), preprocessing must convert raw reads
    to a readcount matrix before QC can run.

    Raises:
        HTTPException: If preprocessing has not been completed
    """
    if not job_manager.is_sequence_format():
        return  # Only check for sequence formats

    # Check if preprocessing has been marked complete
    if not job_manager.is_preprocessing_complete():
        raise HTTPException(
            status_code=400,
            detail="Sequence read preprocessing must be completed before running QC. "
                   "Please run preprocessing first to convert raw reads to a readcount matrix."
        )


def validate_required_files(required_types: list[str]) -> list[str]:
    """Check that all required file types have been uploaded.

    Args:
        required_types: List of required file type names

    Returns:
        List of missing file types (empty if all present)
    """
    missing = []
    for file_type in required_types:
        if file_type == "readcounts" and job_manager.is_sequence_format():
            # For sequence formats, check readcount files instead
            if job_manager.get_readcount_count() == 0:
                missing.append(file_type)
        elif not job_manager.get_file_path(file_type):
            missing.append(file_type)
    return missing
