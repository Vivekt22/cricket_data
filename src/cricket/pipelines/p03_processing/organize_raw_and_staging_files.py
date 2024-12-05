import os
from pathlib import Path
import polars as pl

from pathlib import Path

def organize_raw_and_staging_files(params: dict, cricket_dataset: pl.LazyFrame) -> None:
    staged_directory = Path(params["staged_directory"])
    raw_directory = Path(params["raw_directory"])

    # Get list of files (by stem) in the raw and staged directories
    raw_files = {file.stem for file in raw_directory.glob("*.yaml")}
    staged_files = {file.stem for file in staged_directory.glob("*.yaml")}

    # Files to move from raw to staged (those that are not in staged)
    files_to_shift = raw_files - staged_files

    # Move files to the staging directory
    for file in files_to_shift:
        try:
            source = raw_directory / f"{file}.yaml"
            destination = staged_directory / f"{file}.yaml"
            source.rename(destination)
        except Exception as e:
            raise ValueError(f"Error moving files: {e}")
    
    # Delete all .txt and .yaml files from the raw directory
    for file in raw_directory.glob("*.yaml"):
        try:
            file.unlink()  # Safely remove the file
        except Exception as e:
            raise ValueError(f"Error removing files: {e}")

    for file in raw_directory.glob("*.txt"):
        try:
            file.unlink()  # Safely remove the file
        except Exception as e:
            raise ValueError(f"Error removing files: {e}")
        