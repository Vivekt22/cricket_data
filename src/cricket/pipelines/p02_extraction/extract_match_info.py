from datetime import date, datetime
from multiprocessing import Pool, cpu_count
from pathlib import Path

import polars as pl
import yaml

from cricket.utility_functions import convert_schema_dict

def extract_match_info(file: Path):
    try:
        match_info_schema = {
            'match_id': pl.Utf8,
            'city': pl.Utf8,
            'match_start_date': pl.Datetime("ns"),
            'match_end_date': pl.Datetime("ns"),
            'match_type': pl.Utf8,
            'gender': pl.Utf8,
            'umpire_1': pl.Utf8,
            'umpire_2': pl.Utf8,
            'win_by': pl.Utf8,
            'win_margin': pl.Float64,
            'winner': pl.Utf8,
            'player_of_match': pl.Utf8,
            'team1': pl.Utf8,
            'team2': pl.Utf8,
            'toss_decision': pl.Utf8,
            'toss_winner': pl.Utf8,
            'venue': pl.Utf8
        }

        match_id = file.stem

        with open(file) as f:
            match_data = yaml.safe_load(f)
        match_info = match_data['info']

        def safe_get_dict(ref_dict, ref_key, ref_index=None):
            try:
                result = ref_dict[ref_key]
                if ref_index is not None:
                    result = result[ref_index]
                return result
            except (KeyError, IndexError, TypeError):
                return None

        def parse_date(value):
            if isinstance(value, str):
                return datetime.strptime(value, '%Y-%m-%d')
            elif isinstance(value, (datetime, date)):
                return datetime.combine(value, datetime.min.time())
            return None

        outcome_by = match_info.get('outcome', {}).get('by', {})
        win_by_key = next(iter(outcome_by), None)
        win_margin_value = float(outcome_by[win_by_key]) if win_by_key else 0

        match_info_row = [
            match_id,
            safe_get_dict(match_info, 'city'),
            parse_date(safe_get_dict(match_info, 'dates', 0)),
            parse_date(safe_get_dict(match_info, 'dates', -1)),
            safe_get_dict(match_info, 'match_type'),
            safe_get_dict(match_info, 'gender'),
            safe_get_dict(match_info, 'umpires', 0),
            safe_get_dict(match_info, 'umpires', -1),
            win_by_key,
            win_margin_value,
            safe_get_dict(safe_get_dict(match_info, 'outcome'), 'winner'),
            safe_get_dict(match_info, 'player_of_match', 0),
            safe_get_dict(match_info, 'teams', 0),
            safe_get_dict(match_info, 'teams', -1),
            safe_get_dict(safe_get_dict(match_info, 'toss'), 'decision'),
            safe_get_dict(safe_get_dict(match_info, 'toss'), 'winner'),
            safe_get_dict(match_info, 'venue')
        ]

        match_info_df = pl.DataFrame([match_info_row], schema=match_info_schema, orient="row")
        return match_info_df

    except Exception:
        return None


def update_match_info(params: dict, raw_match_ids_cleaned: pl.LazyFrame) -> pl.LazyFrame:
    # Preprocessed match_info from previous runs

    # Define the path to the preprocessed match_info
    preprocessed_directory = Path(params["preprocessed_directory"])
    match_info_path = preprocessed_directory.joinpath("match_info.parquet")
    match_info_data_exists = match_info_path.exists()

    # Check if the preprocessed match_info exist
    # If they exist, load the preprocessed match_info
    # If they do not exist, create an empty LazyFrame
    if match_info_data_exists:
        df_preprocessed_match_info = pl.scan_parquet(match_info_path)
    else:
        schema = convert_schema_dict(params["match_info_schema"])
        df_preprocessed_match_info = pl.LazyFrame(schema=schema)

    # If there are no new match IDs, return the preprocessed match_info
    if raw_match_ids_cleaned.drop_nulls().collect().height == 0:
        return df_preprocessed_match_info
    
    # Get the paths to the raw files
    raw_directory = Path(params["raw_directory"])
    raw_files = [
        raw_directory.joinpath(file + ".yaml") 
        for file 
        in raw_match_ids_cleaned.select("match_id").collect().to_series().to_list()
    ]

    # Extract the new match_info
    with Pool(cpu_count()) as pool:
        all_match_info = pool.map(extract_match_info, raw_files)
    non_empty_dfs = [df for df in all_match_info if df is not None]
    # If there are new match_info, concatenate the preprocessed match_info with the new match_info
    if non_empty_dfs:
        df_new_match_info = pl.concat(non_empty_dfs).lazy()

        is_new_match_id_in_preprocessed_match_id = (
            df_new_match_info.filter(pl.col("match_id").is_in(df_preprocessed_match_info.select("match_id").unique().collect()))
            .collect().height > 0
        )

        if is_new_match_id_in_preprocessed_match_id:
            raise ValueError("New match IDs already exist in the preprocessed match_info.")
        
        df_match_info = pl.concat([df_preprocessed_match_info.cast(schema), df_new_match_info.cast(schema)])

    # If there are no new match_info, return the preprocessed match_info
    else:
        df_match_info = df_preprocessed_match_info

    return df_match_info

