import polars as pl
from polars import lit


def get_processed_match_ids(
        df_deliveries: pl.LazyFrame,
        df_match_info: pl.LazyFrame,
        df_registry: pl.LazyFrame
    ) -> pl.LazyFrame:
    deliveries_match_ids = df_deliveries.select("match_id").unique().with_columns(data_name = lit("deliveries"))
    match_info_match_ids = df_match_info.select("match_id").unique().with_columns(data_name = lit("match_info"))
    registry_match_ids = df_registry.select("match_id").unique().with_columns(data_name = lit("registry"))

    df_processed_match_ids = pl.concat(
        [
            deliveries_match_ids,
            match_info_match_ids,
            registry_match_ids
        ]
    )

    return df_processed_match_ids
