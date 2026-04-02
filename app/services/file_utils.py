"""Shared file parsing utilities."""
from pathlib import Path
import pandas as pd


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
