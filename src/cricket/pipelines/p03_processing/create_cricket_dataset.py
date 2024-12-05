from pathlib import Path
import polars as pl
import polars.selectors as cs
from polars import col


def create_cricket_dataset(
        df_raw_match_ids_cleaned: pl.LazyFrame,
        params: dict,
        df_deliveries: pl.LazyFrame,
        df_match_info: pl.LazyFrame,
        df_registry: pl.LazyFrame,
        df_cricinfo_people: pl.LazyFrame,
        df_people_raw: pl.LazyFrame,
        df_team_league_mapping: pl.LazyFrame
        ) -> pl.LazyFrame:
    if df_raw_match_ids_cleaned.select("match_id").collect().height > 0:
        df_deliveries_import = (
            df_deliveries
            .with_row_index('delivery_id', offset=1)
            .with_columns(
                innings_id = pl.concat_str("match_id", pl.lit("_"), col("innings").str.slice(0,1))
            )
        )

        df_registry_import = df_registry

        df_cricinfo_people_import = (
            df_cricinfo_people
            .rename(lambda c: c.lower().replace(' ', '_'))
            .select(col('id').alias('key_cricinfo'), col('name').alias('person_name_espn'), col('batting_style'), col('bowling_style'))
        )

        df_people_raw_data_import = (
            df_people_raw
            .select(col('identifier').alias('person_id'), 'key_cricinfo', col('name').alias('person_name'), col('unique_name').alias('person_unique_name'))
        )

        df_match_info_import = df_match_info

        df_people = (
            df_people_raw_data_import
            .join(df_cricinfo_people_import, how='left', on='key_cricinfo')
            .with_columns(person_name = pl.coalesce(col('person_name_espn'), col('person_name')))
            .drop('person_name_espn', 'batting_style', 'bowling_style')
        )

        df_person_names = (
            df_registry_import
            .join(df_people.rename({'person_name': 'person_name_full'}), how='left', on='person_id')
            .with_columns(person_name = pl.coalesce(col('person_name_full'), col('person_name')))
            .drop('person_name_full', 'key_cricinfo')
            .unique()
        )

        SIX = 6

        df_deliveries = (
            df_deliveries_import
            .join(
                df_person_names.rename({'person_unique_name': 'batter', 'person_name': 'batter_name', 'person_id': 'batter_id'}),
                how='left',
                on=['batter', 'match_id']
            )
            .join(
                df_person_names.rename({'person_unique_name': 'bowler', 'person_name': 'bowler_name', 'person_id': 'bowler_id'}),
                how='left',
                on=['bowler', 'match_id']
            )
            .join(
                df_person_names.rename({'person_unique_name': 'non_striker', 'person_name': 'non_striker_name', 'person_id': 'non_striker_id'}),
                how='left',
                on=['non_striker', 'match_id']
            )
            .join(
                df_person_names.rename({'person_unique_name': 'player_out', 'person_name': 'player_out_name', 'person_id': 'player_out_id'}),
                how='left',
                on=['player_out', 'match_id']
            )
            .with_columns(
                missing_batter_name = pl.when(col('batter_name').is_null()).then(True).otherwise(False),
                missing_bowler_name = pl.when(col('bowler_name').is_null()).then(True).otherwise(False),
                missing_non_striker_name = pl.when(col('non_striker_name').is_null()).then(True).otherwise(False),
                missing_player_out_name = pl.when(col('player_out_name').is_null()).then(True).otherwise(False)
            )
            .with_columns(
                batter = pl.coalesce('batter_name', 'batter'),
                bowler = pl.coalesce('bowler_name', 'bowler'),
                non_striker = pl.coalesce('non_striker_name', 'non_striker'),
                player_out = pl.coalesce('player_out_name', 'player_out')
            )
            .with_columns(
                over = col('delivery').cast(pl.Int64) + 1,
                ball = ((col('delivery') - col('delivery').cast(pl.Int64)) * 10).round().cast(pl.Int64),
                wickets = pl.when(col('wicket_type').is_not_null()).then(1).otherwise(0),
                six = pl.when(col('batter_runs') >= SIX).then(1).otherwise(0),
                four = pl.when(col('batter_runs').is_between(4, 5)).then(1).otherwise(0)
            )
            .select(
                [
                    'delivery_id',
                    'match_id',
                    'innings_id',
                    'innings',
                    'batting_team',
                    'bowling_team',
                    'declared',
                    'bowler',
                    'batter',
                    'non_striker',
                    'player_out',
                    'delivery',
                    'over',
                    'ball',
                    'wickets',
                    'batter_runs',
                    'extra_runs',
                    'total_runs',
                    'six',
                    'four',
                    'wicket_type',
                    'missing_batter_name',
                    'missing_bowler_name',
                    'missing_non_striker_name',
                    'missing_player_out_name',
                    "batter_id",
                    "bowler_id",
                    "non_striker_id",
                    "player_out_id"
                ]
            )
        )

        df_match_info = (
            df_match_info_import
            .join(
                df_person_names.select(col('person_unique_name').alias('player_of_match'), col('person_name').alias('player_of_match_name'), 'match_id'),
                how='left',
                on=['player_of_match', 'match_id']
            )
            .with_columns('player_of_match', pl.coalesce('player_of_match_name', 'player_of_match'))
            .drop('player_of_match_name')
        )

        df_cricket_dataset = (
            df_deliveries
            .drop(*[c for c in df_deliveries.collect_schema().names() if c.startswith('missing_')])
            .drop(cs.starts_with('missing_'))
            .join(df_match_info, how='inner', on='match_id')
        )

        df_cricket_dataset = (
            df_cricket_dataset
            .cast({cs.datetime(): pl.Date})
            .with_columns(cs.string().replace("Royal Challengers Bengaluru", "Royal Challengers Bangalore"))
            .join(
                df_team_league_mapping,
                how="left",
                left_on="batting_team",
                right_on="team_name",
                coalesce=True
            )
            .with_columns(
                match_type = pl.when(
                    (col("match_type") == "ODM")
                    & (col("league").str.to_lowercase().str.contains("international"))
                )
                .then(pl.lit("ODI"))
                .otherwise(col("match_type"))
            )
        )
    elif Path(params["processed_directory"]).joinpath("cricket_dataset.parquet").exists():
        df_cricket_dataset = pl.read_parquet(Path(params["processed_directory"]).joinpath("cricket_dataset.parquet"))
    else:
        df_cricket_dataset = pl.DataFrame()

    return df_cricket_dataset

