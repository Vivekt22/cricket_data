from kedro.pipeline import Pipeline, node, pipeline

from .create_cricket_dataset import create_cricket_dataset


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=create_cricket_dataset,
                inputs=["deliveries", "match_info", "registry", "cricinfo_people", "people_raw"],
                outputs="cricket_dataset",
                name="create_cricket_dataset"
            )
        ]
    )
