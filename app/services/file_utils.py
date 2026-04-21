"""Shared file parsing utilities."""
from pathlib import Path
import gzip
import zipfile
import shutil
from typing import List
import pandas as pd


# Sequence file formats that require special handling
SEQUENCE_FORMATS = {"fastq", "bam", "sam"}


def is_sequence_format(file_format: str) -> bool:
    """Check if format is a raw sequencing format."""
    return file_format.lower() in SEQUENCE_FORMATS


def is_archive(filename: str) -> bool:
    """Check if file is a zip or gz archive."""
    lower = filename.lower()
    return lower.endswith(".zip") or lower.endswith(".gz")


def is_gzip(filename: str) -> bool:
    """Check if file is gzip compressed."""
    return filename.lower().endswith(".gz")


def is_zip(filename: str) -> bool:
    """Check if file is a zip archive."""
    return filename.lower().endswith(".zip")


def extract_gzip(file_path: Path, dest_dir: Path) -> List[Path]:
    """Extract a gzip file (single file compression).

    Args:
        file_path: Path to the .gz file
        dest_dir: Directory to extract to

    Returns:
        List containing the single extracted file path
    """
    # Remove .gz extension to get output filename
    output_name = file_path.stem  # e.g., "reads.fastq.gz" -> "reads.fastq"
    output_path = dest_dir / output_name

    with gzip.open(file_path, "rb") as f_in:
        with open(output_path, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)

    return [output_path]


def extract_zip(file_path: Path, dest_dir: Path) -> List[Path]:
    """Extract a zip archive.

    Args:
        file_path: Path to the .zip file
        dest_dir: Directory to extract to

    Returns:
        List of extracted file paths (excludes directories and hidden files)
    """
    extracted_files = []

    with zipfile.ZipFile(file_path, "r") as zf:
        for member in zf.namelist():
            # Skip directories and hidden files (starting with . or in __MACOSX)
            if member.endswith("/") or member.startswith("__MACOSX") or "/." in member:
                continue

            # Extract to dest_dir, flattening any directory structure
            filename = Path(member).name
            if not filename:
                continue

            output_path = dest_dir / filename

            with zf.open(member) as source, open(output_path, "wb") as target:
                shutil.copyfileobj(source, target)

            extracted_files.append(output_path)

    return extracted_files


def extract_archive(file_path: Path, dest_dir: Path) -> List[Path]:
    """Extract an archive (zip or gz) to the destination directory.

    Args:
        file_path: Path to the archive file
        dest_dir: Directory to extract to

    Returns:
        List of extracted file paths

    Raises:
        ValueError: If file is not a recognized archive format
    """
    if is_gzip(file_path.name):
        return extract_gzip(file_path, dest_dir)
    elif is_zip(file_path.name):
        return extract_zip(file_path, dest_dir)
    else:
        raise ValueError(f"Unknown archive format: {file_path.name}")


def parse_file(file_path: Path, file_format: str, **kwargs) -> pd.DataFrame:
    """Parse a data file (CSV, TSV, or HDF5) into a DataFrame.

    Args:
        file_path: Path to the file
        file_format: One of 'csv', 'tsv', or 'hdf5'
        **kwargs: Additional arguments passed to pd.read_csv (ignored for hdf5)

    Returns:
        pandas DataFrame
    """
    if file_format == "hdf5":
        import chronos
        return chronos.read_hdf5(str(file_path))

    sep = "\t" if file_format == "tsv" else ","
    return pd.read_csv(file_path, sep=sep, **kwargs)


def parse_gene_list(file_path: Path) -> list[str]:
    """Parse a text file containing one gene per line.

    Args:
        file_path: Path to the text file

    Returns:
        List of gene names
    """
    with open(file_path, "r") as f:
        return [line.strip() for line in f if line.strip()]
