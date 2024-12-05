from kedro.pipeline import Pipeline, node, pipeline

from .extract_deliveries import (
    updated_deliveries,
)
from .extract_match_info import update_match_info
from .extract_registry import update_registry


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=updated_deliveries,
                inputs=["parameters", "raw_match_ids_cleaned"],
                outputs="deliveries",
                name="updated_deliveries"
            ),
            node(
                func=update_match_info,
                inputs=["parameters", "raw_match_ids_cleaned"],
                outputs="match_info",
                name="update_match_info"
            ),
            node(
                func=update_registry,
                inputs=["parameters", "raw_match_ids_cleaned"],
                outputs="registry",
                name="update_registry"
            )
        ]
    )
