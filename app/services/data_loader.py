"""Shared utilities for loading and validating CRISPR screen data."""

from pathlib import Path
from .job_manager import job_manager
from .file_utils import parse_file, parse_gene_list

DEFAULT_CONTROLS_DIR = Path(__file__).parent.parent / "data" / "controls"
DEFAULT_POSITIVE_CONTROLS = DEFAULT_CONTROLS_DIR / "positive_controls.txt"
DEFAULT_NEGATIVE_CONTROLS = DEFAULT_CONTROLS_DIR / "negative_controls.txt"


def load_crispr_data(job_id: str):
    """Load and validate readcounts, condition_map, guide_map.

    Returns:
        tuple: (readcounts, sequence_map, guide_map)

    Raises:
        ValueError: If required files are missing or data doesn't match
        KeyError: If required columns are missing
    """
    job_manager.resume_job(job_id)

    readcounts_path = job_manager.get_file_path("readcounts")
    condition_map_path = job_manager.get_file_path("condition_map")
    guide_map_path = job_manager.get_file_path("guide_map")

    if not all([readcounts_path, condition_map_path, guide_map_path]):
        raise ValueError("Missing required files: readcounts, condition_map, or guide_map")

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

    # Validate required columns
    if "sgrna" not in guide_map:
        raise KeyError("guide_map missing required column 'sgrna'")
    if "sequence_ID" not in sequence_map:
        raise KeyError("condition_map missing required column 'sequence_ID'")

    # Auto-transpose if needed
    readcounts = auto_orient_readcounts(readcounts, guide_map, sequence_map)

    # Filter guide_map to only include guides present in readcounts
    guide_map = guide_map[guide_map.sgrna.isin(readcounts.columns)]

    return readcounts, sequence_map, guide_map


def auto_orient_readcounts(readcounts, guide_map, sequence_map):
    """Transpose readcounts if guides are on rows instead of columns.

    Expected orientation: sequences/replicates as rows, guides as columns.
    """
    guides_in_cols = len(set(readcounts.columns) & set(guide_map.sgrna))
    seqs_in_rows = len(set(readcounts.index) & set(sequence_map.sequence_ID))

    if not guides_in_cols and not seqs_in_rows:
        # Check if transposed
        guides_in_rows = len(set(readcounts.index) & set(guide_map.sgrna))
        seqs_in_cols = len(set(readcounts.columns) & set(sequence_map.sequence_ID))
        if guides_in_rows and seqs_in_cols:
            readcounts = readcounts.T
        else:
            raise ValueError(
                "Readcounts columns and indices do not match the "
                "condition map sequence IDs and guide map sgRNAs provided"
            )

    missing_sgrnas = sorted(set(readcounts.columns) - set(guide_map.sgrna))
    if missing_sgrnas:
        raise ValueError(f"{len(missing_sgrnas)} sgrnas in readcounts missing from the guide map")

    missing_sequences = sorted(set(readcounts.index) - set(sequence_map.sequence_ID))
    if missing_sequences:
        raise ValueError(f"{len(missing_sequences)} sequences in readcounts missing from the sequence map")

    return readcounts


def load_controls(positive_path=None, negative_path=None, require_controls=False):
    """Load control gene lists with fallback to defaults.

    Args:
        positive_path: Path to positive controls file (optional)
        negative_path: Path to negative controls file (optional)
        require_controls: If True, raise error when controls not found

    Returns:
        tuple: (positive_controls, negative_controls) as lists of gene names

    Raises:
        FileNotFoundError: If require_controls=True and controls not found
    """
    # Load positive controls
    if positive_path:
        positive = parse_gene_list(positive_path)
    elif DEFAULT_POSITIVE_CONTROLS.exists():
        positive = parse_gene_list(DEFAULT_POSITIVE_CONTROLS)
    elif require_controls:
        raise FileNotFoundError(f"Missing positive controls at {DEFAULT_POSITIVE_CONTROLS}")
    else:
        positive = []

    # Load negative controls
    if negative_path:
        negative = parse_gene_list(negative_path)
    elif DEFAULT_NEGATIVE_CONTROLS.exists():
        negative = parse_gene_list(DEFAULT_NEGATIVE_CONTROLS)
    elif require_controls:
        raise FileNotFoundError(f"Missing negative controls at {DEFAULT_NEGATIVE_CONTROLS}")
    else:
        negative = []

    return positive, negative
