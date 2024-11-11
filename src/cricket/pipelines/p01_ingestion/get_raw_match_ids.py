from pathlib import Path

import polars as pl


def get_raw_match_ids(params: dict):
    raw_data_directory = Path(params["raw_data_directory"])
    df_raw_match_ids = (
        pl.DataFrame(
            [file.stem for file in raw_data_directory.glob("*.yaml")],
            schema={"match_id": pl.String}
        )
    )

    return df_raw_match_ids
