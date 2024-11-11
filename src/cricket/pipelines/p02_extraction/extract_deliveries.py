from multiprocessing import Pool, cpu_count
from pathlib import Path

import polars as pl
import yaml


def extract_deliveries(file: Path):
    try:
        innings_schema = {
            'match_id': pl.Utf8,
            'innings': pl.Utf8,
            'batting_team': pl.Utf8,
            'bowling_team': pl.Utf8,
            'declared': pl.Int8,
            'delivery': pl.Float64,
            'batter': pl.Utf8,
            'bowler': pl.Utf8,
            'non_striker': pl.Utf8,
            'batter_runs': pl.Int64,
            'extra_runs': pl.Int64,
            'total_runs': pl.Int64,
            'wicket_type': pl.Utf8,
            'player_out': pl.Utf8
        }

        match_id = file.stem

        with open(file) as f:
            match_data = yaml.safe_load(f)
        match_info = match_data.get('info', {})

        all_innings = []
        for innings in match_data.get('innings', []):
            for inning, inning_details in innings.items():
                batting_team = inning_details.get('team')
                bowling_team = [team for team in match_info.get('teams', []) if team != batting_team]
                bowling_team = bowling_team[0]

                for delivery_details in inning_details.get('deliveries', []):
                    for delivery, delivery_info in delivery_details.items():
                        innings_row = [
                            match_id,
                            inning,
                            batting_team,
                            bowling_team,
                            1 if delivery_info.get('declared', '').lower() == 'yes' else 0,
                            float(delivery),
                            delivery_info.get('batsman'),
                            delivery_info.get('bowler'),
                            delivery_info.get('non_striker'),
                            delivery_info['runs'].get('batsman'),
                            delivery_info['runs'].get('extras'),
                            delivery_info['runs'].get('total'),
                            delivery_info.get('wicket', {}).get('kind'),
                            delivery_info.get('wicket', {}).get('player_out')
                        ]
                        all_innings.append(innings_row)

        if all_innings:
            innings_df = pl.DataFrame(all_innings, schema=innings_schema, orient="row")
            return innings_df
        else:
            return None

    except Exception:
        return None

def extract_all_deliveries(raw_data_files):
    with Pool(cpu_count()) as pool:
        all_innings = pool.map(extract_deliveries, raw_data_files)
    non_empty_dfs = [df for df in all_innings if df is not None]
    if non_empty_dfs:
        return pl.concat(non_empty_dfs)
    else:
        return pl.DataFrame()

def main_extract_deliveries_raw_data(params: dict, raw_match_ids: pl.LazyFrame) -> pl.LazyFrame:
    raw_data_directory = Path(params["raw_data_directory"])
    raw_data_files = [
        raw_data_directory.joinpath(f"{file}.yaml")
        for file in
        raw_match_ids.select(pl.col("match_id")).collect().to_series().to_list()
    ]
    final_innings_df = extract_all_deliveries(raw_data_files)
    return final_innings_df
