from datetime import date, datetime
from multiprocessing import Pool, cpu_count
from pathlib import Path

import polars as pl
import yaml


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

def extract_all_match_info(raw_data_files):
    with Pool(cpu_count()) as pool:
        all_match_info = pool.map(extract_match_info, raw_data_files)
    non_empty_dfs = [df for df in all_match_info if df is not None]
    if non_empty_dfs:
        return pl.concat(non_empty_dfs)
    else:
        return pl.DataFrame(schema={
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
        })

def main_extract_match_info_raw_data(params: dict, raw_match_ids: pl.LazyFrame) -> pl.LazyFrame:
    raw_data_directory = Path(params["raw_data_directory"])
    raw_data_files = [
        raw_data_directory.joinpath(f"{file}.yaml")
        for file in
        raw_match_ids.select(pl.col("match_id")).collect().to_series().to_list()
    ]
    final_match_info_df = extract_all_match_info(raw_data_files)
    return final_match_info_df
