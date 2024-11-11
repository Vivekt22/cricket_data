# cricket

[![Powered by Kedro](https://img.shields.io/badge/powered_by-kedro-ffc900?logo=kedro)](https://kedro.org)

# Cricket Data Processing Project

This project processes cricket match data, starting from ingestion, through data extraction, and finally producing a cleaned, integrated dataset. It uses the Kedro framework for pipeline organization, with Polars for efficient data handling.

## Pipelines Overview

### 1. **p01_ingestion**: Ingests raw match data
   - **fetch_new_data.py**: Fetches new match data from source.
   - **get_raw_match_ids.py**: Identifies unique match IDs to process.

### 2. **p02_extraction**: Extracts and organizes raw data into structured format
   - **extract_deliveries.py**: Extracts delivery data from match files.
   - **extract_match_info.py**: Extracts match-level information.
   - **extract_registry.py**: Extracts player registry data.
   - **get_processed_match_ids.py**: Identifies already processed match IDs to avoid duplication.

### 3. **p03_processing**: Processes and integrates data to create the final dataset
   - **create_cricket_dataset.py**: Combines and refines delivery, match, and player data into a comprehensive dataset with features such as over, ball, wickets, and boundary indicators.

## Parameters and Configuration

Global parameters for the pipeline are defined in `parameters.yml`, which configures settings like data paths and any constants required for processing.

## Requirements

- **Python**: Ensure you have Python 3.8 or higher.
- **Kedro**: Manages pipeline orchestration.
- **Polars**: Efficiently handles large datasets with high performance.

Install the dependencies with:

```bash
pip install -r requirements.txt
```

## Running the Pipelines

1. **Activate Kedro**: The main entry point of the project is in `src/cricket/__main__.py`.

2. **Run Pipelines**:

   - **Ingestion Pipeline**:
     ```bash
     kedro run --pipeline=p01_ingestion
     ```

   - **Extraction Pipeline**:
     ```bash
     kedro run --pipeline=p02_extraction
     ```

   - **Processing Pipeline**:
     ```bash
     kedro run --pipeline=p03_processing
     ```

3. **Pipeline Outputs**: The final processed dataset will be output to the configured data directory.


## Notes

- **Session Storage**: `session_store.db` is used for pipeline session management.
- **Pipeline Registry**: Pipelines are registered in `pipeline_registry.py`, making them accessible in Kedro.
- **Custom Settings**: Project settings, including paths and pipeline configurations, are in `settings.py`.
