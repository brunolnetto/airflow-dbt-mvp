import json
from io import BytesIO
from pathlib import Path
import pandas as pd

from .config import logging, RAW_DIR  

def serialize_to_buffer(df: pd.DataFrame) -> BytesIO:
    """Serialize DataFrame to a BytesIO buffer in Parquet format."""
    if df.empty:
        logging.warning("⚠️ DataFrame is empty, nothing to serialize.")
        return BytesIO()  # Return empty buffer if DataFrame is empty
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    return buffer

def check_raw_file_exists(entity: str) -> Path:
    """Check if the raw JSON file for the given entity exists."""
    json_path = RAW_DIR / f"{entity}.json"
    if not json_path.exists():
        logging.error(f"❌ Raw JSON file not found for entity: {entity}")
        raise FileNotFoundError(f"File not found: {json_path}")
    return json_path

def quote_column_name(col: str) -> str:
    """Helper function to safely quote column names to handle reserved keywords and special characters."""
    return f'"{col}"'

def convert_dataframe_to_tuples(df: pd.DataFrame) -> list:
    """Convert DataFrame rows to a list of tuples."""
    # Use itertuples for better performance than to_numpy() for larger DataFrames
    return [tuple(row) for row in df.itertuples(index=False, name=None)]

def stringify_value(value):
    """Helper function to stringify values."""
    if isinstance(value, (dict, list)):
        return json.dumps(value)  # Convert nested data to JSON
    return str(value)  # Convert other values to string
