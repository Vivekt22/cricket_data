from kedro.pipeline import Pipeline, node, pipeline

from .fetch_new_data import fetch_new_data
from .get_raw_match_ids import get_raw_match_ids


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=fetch_new_data,
                inputs="parameters",
                outputs=None,
                name="fetch_new_data"
            ),
            node(
                func=get_raw_match_ids,
                inputs="parameters",
                outputs="raw_match_ids",
                name="get_raw_match_ids"
            )
        ]
    )
