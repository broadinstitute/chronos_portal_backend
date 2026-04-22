"""PoolQ integration for processing raw sequencing reads.

This module handles the conversion of raw sequencing reads (fastq/bam/sam)
to readcount matrices using PoolQ as the underlying tool.

Pipeline:
1. write_poolq_inputs() - Convert condition_map and guide_map to PoolQ format
2. run_poolq() - Execute PoolQ via subprocess
3. postprocess_poolq_output() - Convert PoolQ output to readcount matrix
"""

from pathlib import Path
from typing import Optional
import pandas as pd
from chronos import write_hdf5
import os
import subprocess

from .job_manager import job_manager


def write_poolq_inputs(
    job_id: str,
    condition_map: pd.DataFrame,
    guide_map: pd.DataFrame,
    poolq_dir: Path
) -> dict:
    """Write condition_map and guide_map in PoolQ-expected format.

    Args:
        job_id: The job identifier
        condition_map: DataFrame with sample/barcode information
        guide_map: DataFrame with sgRNA/gene mappings
        output_dir: Directory to write PoolQ input files

    Returns:
        dict with paths to generated files:
        {
            "row_barcodes": Path to row (sgrna) barcodes file,
            "column_barcdes": Path to column (sequenced sample) file,
        }
    """

    guide_map[["sgrna", "sgrna"]].to_csv(poolq_dir / "row_barcodes.csv", index=None, header=None)

    condition_map[["barcode", "sequence_ID"]].drop_duplicates().to_csv(poolq_dir / "column_barcodes.csv", index=None, header=None)

    return {"row_barcodes": poolq_dir / "row_barcodes.csv", "column_barcodes": poolq_dir / "column_barcodes.csv"}


def run_poolq(
    job_id: str,
    readcount_files: list[Path],
    input_files: dict,
    output_dir: Path,
    options: dict,
    condition_map: pd.DataFrame
) -> Path:
    """Execute PoolQ via subprocess to count barcodes/sgRNAs.

    Args:
        job_id: The job identifier
        readcount_files: List of input fastq/bam/sam files
        input_files: Dict of PoolQ input file paths (from write_poolq_inputs)
        output_dir: Directory for PoolQ output
        options: Processing options from readcount_options:
            - readType: "single" or "paired"
            - sgrnaFindMethod: "fixed", "prefix", or "template"
            - sgrnaStart, sgrnaEnd, sgrnaPrefix, sgrnaTemplate, etc.
            - sampleFindMethod, sampleStart, etc.
            - countAmbiguous: bool
            - errorIfTooShort: bool

    Returns:
        Path to PoolQ output file(s)

    Raises:
        subprocess.CalledProcessError: If PoolQ fails
        FileNotFoundError: If PoolQ executable not found
    """
    command = f'java -Xmx4G -XX:+UseG1GC -jar "../../../../poolq-3.13.2/poolq3.jar" '
    row_op = f'--row-reference {input_files["row_barcodes"]} '
    col_op = f'--col-reference {input_files["column_barcodes"]} '

    file_assignments = options.get("fileAssignments")

    if options["alreadyDeconvoluted"]:
        try:
            mapper = condition_map[["barcode", "file"]].copy()
        except KeyError:
            raise KeyError(
                "If your reads files are already deconvoluted/demultiplexed by"
                "condition, the condition map must have both a 'barcode'"
                "and a 'file' column"
            )

        mapper["file"] = mapper["file"].apply(lambda s: s.split(' ')[-1])
        path_mapper = {path.name: path for path in readcount_files}
        missing = sorted(set(mapper.file) - set(path_mapper.keys()))
        if missing:
            raise ValueError(f"files listed in the condition map not found: {', '.join(missing)}")
        
        mapper["file"].replace(path_mapper, inplace=True)
        reads = "--compat --demultiplexed --row-reads " + ','.join([f'{row["barcode"]}:{row["file"]}'
            for ind, row in mapper.iterrows()
            if file_assignments[row["file"].name] in ["sgrna", "all", "both"]
            ])

    elif not file_assignments:
        reads = f'--reads {",".join([str(s) for s in readcount_files])}'

    elif all([v == "both" for v in file_assignments.values()]):
        reads = f'--reads {",".join([str(s) for s in readcount_files])}'

    else:
        row_reads = f'--row-reads {",".join([
            str(s) for s in readcount_files if file_assignments[s.name] in ["sgrna", "all", "both"]
        ])}'
        col_reads = f'--col-reads {",".join([
            str(s) for s in readcount_files if file_assignments[s.name] in ["sample", "all", "both"]
        ])}'
        reads = ' '.join([row_reads, col_reads])

    if options["readType"] == 'paired':
        if options["alreadyDeconvoluted"]:
            row_reads_reversed = "--rev-row-reads " + ','.join([f'{row["barcode"]}:{row["file"]}'
            for ind, row in mapper.iterrows()
            if file_assignments[row["file"].name] in ["sgrna_reversed", "all", "both"]
            ])
        else:
            row_reads_reversed  = f'--rev-row-reads {",".join([
                str(s) for s in readcount_files if file_assignments[s.name] in ["sgrna_reversed", "all"]
            ])}'
        reads = ' '.join([reads, row_reads_reversed])

    def option_exists(option):
        return not (options.get(option) is None)

    search_policies = {}
    axes = ["sgrna", "sample", "sgrnaReversed"]

    for axis in axes:
        if axis == "sample" and options["alreadyDeconvoluted"]:
            search_policies[axis] = None
            continue

        if axis == "sgrnaReversed" and options["readType"] == 'single':
            search_policies[axis] = None
            continue

        if options[f"{axis}FindMethod"] == "fixed":
            search_policies[axis] = f'FIXED:{int(options[f"{axis}Start"])}'
            if options.get(f"{axis}End"):
                search_policies[axis] = search_policies[axis] + f':{int(options[f"{axis}End"])}'

            continue

        elif options[f"{axis}FindMethod"] == "template":
            search_policies[axis] = f'TEMPLATE:{options[f"{axis}Template"]}'

        elif options[f"{axis}FindMethod"] == "prefix":
            search_policies[axis] = f'PREFIX:{options[f"{axis}Prefix"]}'

        if option_exists(f"{axis}SearchAfter") or option_exists(f"{axis}StartsBefore"):
                search_policies[axis] = search_policies[axis] + "@"
        if option_exists(f"{axis}SearchAfter"):
            search_policies[axis] = search_policies[axis] + str(int(options[f"{axis}SearchAfter"]))
        if option_exists(f"{axis}StartsBefore"):
            search_policies[axis] = search_policies[axis] + f'-{options[f"{axis}StartsBefore"]}'

    command = ' '.join([
        f'{command} {reads} {row_op} {col_op}',
        f'--row-barcode-policy {search_policies["sgrna"]}'
    ])
    if search_policies['sample']:
        command = command + f' --col-barcode-policy {search_policies["sample"]}'

    if search_policies['sgrnaReversed']:
        command = command + f' --rev-row-barcode-policy {search_policies["sgrnaReversed"]}'

    if options["countAmbiguous"]:
        command = command + " --count-ambiguous"

    job_manager.job_config["lastPoolQCommand"] = command
    job_manager._save_config()
    out = subprocess.run(command, shell=True, capture_output=True, cwd=output_dir)

    if out.returncode != 0:
        raise RuntimeError(
            f'Error code {out.returncode} in executing \n\n  {command}  \n\n. Error:\n\n{out.stderr.decode()}'
        )

    return output_dir / "counts.txt"



def postprocess_poolq_output(
    job_id: str,
    poolq_output: Path,
    output_path: Path,
) -> pd.DataFrame:
    """Convert PoolQ output to a readcount matrix for Chronos.

    Args:
        job_id: The job identifier
        poolq_output: Path to PoolQ output file(s)
        output_path: Path to save the final readcount matrix CSV

    Returns:
        DataFrame with readcount matrix:
        - Rows: samples/replicates (sequence_ID)
        - Columns: sgRNA identifiers
        - Values: read counts
    """
    reads = pd.read_csv(poolq_output, sep="\t", index_col=0)\
        .drop(["Row Barcode IDs", "Construct IDs"], axis=1, errors="ignore")\
        .T

    if (reads.sum(axis=1) == 0).any():
        raise ValueError(
            f"no reads found for sequence IDs:\n{'\n'.join(reads.sum(axis=1).loc[lambda x: x==0].index)}\nCheck your PoolQ parameters"
        )
    reads.to_csv(output_path)
    return reads



async def process_reads_with_poolq(job_id: str) -> pd.DataFrame:
    """Full pipeline to process raw reads using PoolQ.

    This is the main entry point called by the preprocessing orchestrator.

    Args:
        job_id: The job identifier

    Returns:
        DataFrame with the readcount matrix

    Raises:
        Various exceptions if any step fails
    """
    from .file_utils import parse_file

    # Get job directories
    uploads_dir = job_manager.get_uploads_dir(job_id)
    poolq_dir = uploads_dir / "poolq_work"
    poolq_dir.mkdir(exist_ok=True)

    # Load input data
    condition_map_path = job_manager.get_file_path("condition_map")
    condition_map_format = job_manager.get_file_format("condition_map")
    condition_map = parse_file(condition_map_path, condition_map_format)

    guide_map_path = job_manager.get_file_path("guide_map")
    guide_map_format = job_manager.get_file_format("guide_map")
    guide_map = parse_file(guide_map_path, guide_map_format)

    readcount_files = job_manager.get_readcount_files()
    options = job_manager.get_readcount_options()

    # Step 1: Write PoolQ inputs
    input_files = write_poolq_inputs(
        job_id=job_id,
        condition_map=condition_map,
        guide_map=guide_map,
        poolq_dir=poolq_dir,
    )

    # Step 2: Run PoolQ
    poolq_output = run_poolq(
        job_id=job_id,
        readcount_files=readcount_files,
        input_files=input_files,
        output_dir=poolq_dir,
        options=options,
        condition_map=condition_map
    )

    # Step 3: Postprocess output
    output_path = uploads_dir / "readcounts.csv"
    readcounts = postprocess_poolq_output(
        job_id=job_id,
        poolq_output=poolq_output,
        output_path=output_path,
    )

    # Update job manager with the new readcounts file
    job_manager.store_file_path("readcounts", output_path)
    job_manager.add_file_info("readcounts", "readcounts.csv", "csv", output_path)

    return readcounts
