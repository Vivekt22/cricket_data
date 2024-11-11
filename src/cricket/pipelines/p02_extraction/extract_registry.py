from multiprocessing import Pool, cpu_count
from pathlib import Path

import polars as pl
import yaml


def extract_registry(file: Path):
    try:
        registry_schema = {
            'match_id': pl.Utf8,
            'person_name': pl.Utf8,
            'person_id': pl.Utf8
        }

        match_id = file.stem

        with open(file) as f:
            match_data = yaml.safe_load(f)
        match_info = match_data['info']

        registry_dict = match_info.get('registry', {}).get('people', {})
        registry_rows = [(match_id, name, person_id) for name, person_id in registry_dict.items()]

        registry_df = pl.DataFrame(registry_rows, schema=registry_schema, orient="row")

        return registry_df

    except Exception:
        return None

def extract_all_registry(raw_data_files):
    with Pool(cpu_count()) as pool:
        all_registry = pool.map(extract_registry, raw_data_files)
    non_empty_dfs = [df for df in all_registry if df is not None]
    if non_empty_dfs:
        return pl.concat(non_empty_dfs)
    else:
        return pl.DataFrame()

def main_extract_registry_raw_data(params: dict, raw_match_ids: pl.LazyFrame) -> pl.LazyFrame:
    raw_data_directory = Path(params["raw_data_directory"])
    raw_data_files = [
        raw_data_directory.joinpath(f"{file}.yaml")
        for file in
        raw_match_ids.select(pl.col("match_id")).collect().to_series().to_list()
    ]
    final_registry_df = extract_all_registry(raw_data_files)
    return final_registry_df

