from kedro.pipeline import Pipeline, node, pipeline

from .extract_deliveries import (
    main_extract_deliveries_raw_data,
)
from .extract_match_info import main_extract_match_info_raw_data
from .extract_registry import main_extract_registry_raw_data
from .get_processed_match_ids import get_processed_match_ids


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=main_extract_deliveries_raw_data,
                inputs=["parameters", "raw_match_ids"],
                outputs="deliveries",
                name="extract_deliveries_raw_data"
            ),
            node(
                func=main_extract_match_info_raw_data,
                inputs=["parameters", "raw_match_ids"],
                outputs="match_info",
                name="extract_match_info_raw_data"
            ),
            node(
                func=main_extract_registry_raw_data,
                inputs=["parameters", "raw_match_ids"],
                outputs="registry",
                name="extract_registry_raw_data"
            ),
            node(
                func=get_processed_match_ids,
                inputs=["deliveries", "match_info", "registry"],
                outputs="processed_match_ids",
                name="get_processed_match_ids"
            ),
        ]
    )
