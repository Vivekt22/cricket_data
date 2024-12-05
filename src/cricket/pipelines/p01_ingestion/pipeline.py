from kedro.pipeline import Pipeline, node, pipeline

from .ingestion import download_raw_files, remove_raw_files_previously_staged


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=download_raw_files,
                inputs="parameters",
                outputs="raw_match_ids",
                name="download_yaml_files",
            ),
            node(
                func=remove_raw_files_previously_staged,
                inputs=["parameters", "raw_match_ids"],
                outputs="raw_match_ids_cleaned",
                name="remove_raw_files_previously_staged"
            )
        ]
    )
