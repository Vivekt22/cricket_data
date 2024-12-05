import polars as pl

def convert_schema_dict(schema_dict: dict) -> dict:
    return {colname: getattr(pl, dtype) for colname, dtype in schema_dict.items()}