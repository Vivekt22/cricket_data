from kedro.pipeline import Pipeline, node, pipeline

from .create_cricket_dataset import create_cricket_dataset
from .organize_raw_and_staging_files import organize_raw_and_staging_files


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=create_cricket_dataset,
                inputs=["raw_match_ids_cleaned", "parameters", "deliveries", "match_info", "registry", "cricinfo_people", "people_raw", "team_league_mapping"],
                outputs="cricket_dataset",
                name="create_cricket_dataset"
            ),
            node(
                func=organize_raw_and_staging_files,
                inputs=["parameters", "cricket_dataset"],
                outputs=None,
                name="organize_raw_and_staging_files"
            )
        ]
    )
