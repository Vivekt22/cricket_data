from multiprocessing import Pool, cpu_count
from pathlib import Path

import polars as pl
import yaml

from cricket.utility_functions import convert_schema_dict


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

def update_registry(params: dict, raw_match_ids_cleaned: pl.LazyFrame) -> pl.LazyFrame:
    # Preprocessed registry from previous runs

    # Define the path to the preprocessed registry
    preprocessed_directory = Path(params["preprocessed_directory"])
    registry_path = preprocessed_directory.joinpath("registry.parquet")
    registry_data_exists = registry_path.exists()

    # Check if the preprocessed registry exist
    # If they exist, load the preprocessed registry
    # If they do not exist, create an empty LazyFrame
    if registry_data_exists:
        df_preprocessed_registry = pl.scan_parquet(registry_path)
    else:
        schema = convert_schema_dict(params["registry_schema"])
        df_preprocessed_registry = pl.LazyFrame(schema=schema)

    # If there are no new match IDs, return the preprocessed registry
    if raw_match_ids_cleaned.drop_nulls().collect().height == 0:
        return df_preprocessed_registry
    
    # Get the paths to the raw files
    raw_directory = Path(params["raw_directory"])
    raw_files = [
        raw_directory.joinpath(file + ".yaml") 
        for file 
        in raw_match_ids_cleaned.select("match_id").collect().to_series().to_list()
    ]

    # Extract the new registry
    with Pool(cpu_count()) as pool:
        all_registry = pool.map(extract_registry, raw_files)
    non_empty_dfs = [df for df in all_registry if df is not None]
    # If there are new registry, concatenate the preprocessed registry with the new registry
    if non_empty_dfs:
        df_new_registry = pl.concat(non_empty_dfs).lazy()

        is_new_match_id_in_preprocessed_match_id = (
            df_new_registry.filter(pl.col("match_id").is_in(df_preprocessed_registry.select("match_id").unique().collect()))
            .collect().height > 0
        )
    
        if is_new_match_id_in_preprocessed_match_id:
            raise ValueError("New match IDs already exist in the preprocessed registry.")
        
        df_registry = pl.concat([df_preprocessed_registry, df_new_registry])

    # If there are no new registry, return the preprocessed registry
    else:
        df_registry = df_preprocessed_registry

    return df_registry


