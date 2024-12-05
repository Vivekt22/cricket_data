from multiprocessing import Pool, cpu_count
from pathlib import Path

import polars as pl
import yaml

from cricket.utility_functions import convert_schema_dict

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


def updated_deliveries(params: dict, raw_match_ids_cleaned: pl.LazyFrame) -> pl.LazyFrame:
    # Preprocessed deliveries from previous runs

    # Define the path to the preprocessed deliveries
    preprocessed_directory = Path(params["preprocessed_directory"])
    delivery_path = preprocessed_directory.joinpath("deliveries.parquet")
    delivery_data_exists = delivery_path.exists()

    # Check if the preprocessed deliveries exist
    # If they exist, load the preprocessed deliveries
    # If they do not exist, create an empty LazyFrame
    if delivery_data_exists:
        df_preprocessed_delivery = pl.scan_parquet(delivery_path)
    else:
        schema = convert_schema_dict(params["delivery_schema"])
        df_preprocessed_delivery = pl.LazyFrame(schema=schema)

    # If there are no new match IDs, return the preprocessed deliveries
    if raw_match_ids_cleaned.drop_nulls().collect().height == 0:
        return df_preprocessed_delivery
    
    # Get the paths to the raw files
    raw_directory = Path(params["raw_directory"])
    raw_files = [
        raw_directory.joinpath(file + ".yaml") 
        for file 
        in raw_match_ids_cleaned.select("match_id").collect().to_series().to_list()
    ]

    # Extract the new deliveries
    with Pool(cpu_count()) as pool:
        all_innings = pool.map(extract_deliveries, raw_files)
    non_empty_dfs = [df for df in all_innings if df is not None]
    # If there are new deliveries, concatenate the preprocessed deliveries with the new deliveries
    if non_empty_dfs:
        df_new_deliveries = pl.concat(non_empty_dfs).lazy()

        is_new_match_id_in_preprocessed_match_id = (
            df_new_deliveries.filter(pl.col("match_id").is_in(df_preprocessed_delivery.select("match_id").unique().collect()))
            .collect().height > 0
        )

        if is_new_match_id_in_preprocessed_match_id:
            raise ValueError("New match IDs already exist in the preprocessed deliveries.")
        
        df_deliveries = pl.concat([df_preprocessed_delivery, df_new_deliveries])

    # If there are no new deliveries, return the preprocessed deliveries
    else:
        df_deliveries = df_preprocessed_delivery

    return df_deliveries

    
