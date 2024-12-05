import os
import zipfile
from pathlib import Path

import requests
from tqdm import tqdm
import polars as pl


def download_raw_files(params: dict) -> pl.LazyFrame:
    url = params["cricsheet_url"]
    raw_directory = params["raw_directory"]

    # Delete all .txt and .yaml files from the raw directory
    for file in Path(raw_directory).glob("*.yaml"):
        try:
            file.unlink()  # Safely remove the file
            
        except Exception as e:
            raise ValueError(f"Error removing file {file}: {e}")
    print(f"Deleted yaml files")

    for file in Path(raw_directory).glob("*.txt"):
        try:
            file.unlink()  # Safely remove the file
        except Exception as e:
            raise ValueError(f"Error removing file {file}: {e}")
    print(f"Deleted file txt files")
    
    zip_file_path = "all.zip"
    new_files_count = 0

    # Download the zip file
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(zip_file_path, 'wb') as f:
            for chunk in tqdm(r.iter_content(chunk_size=8192), unit="KB"):
                f.write(chunk)

    # Extract the zip file
    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        for member in tqdm(zip_ref.infolist(), desc="Extracting"):
            member_path = os.path.join(raw_directory, member.filename)
            if os.path.isfile(member_path) and os.path.getsize(member_path) == member.file_size:
                continue
            zip_ref.extract(member, raw_directory)
            new_files_count += 1

    os.remove(zip_file_path)

    df_raw_match_ids = (
        pl.LazyFrame(
            [file.stem for file in Path(raw_directory).glob("*.yaml")],
            schema={"match_id": pl.String}
        )
    )

    return df_raw_match_ids


def remove_raw_files_previously_staged(params: dict, raw_match_ids: pl.LazyFrame) -> pl.LazyFrame:
    raw_directory = Path(params["raw_directory"])
    staged_directory = Path(params["staged_directory"])

    staged_file_names = {file.stem for file in staged_directory.glob("*.yaml")}

    raw_files_names = {file.stem for file in raw_directory.glob("*.yaml")}

    files_to_remove = raw_files_names.intersection(staged_file_names)

    raw_files_to_remove = [file for file in raw_directory.glob("*.yaml") if file.stem in files_to_remove]

    for file in raw_files_to_remove:
        file.unlink()

    raw_files_cleaned = list(raw_files_names.difference(files_to_remove))
    df_raw_match_ids_cleaned = pl.LazyFrame(raw_files_cleaned, schema={"match_id": pl.String})

    return df_raw_match_ids_cleaned