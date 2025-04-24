from typing import List, Dict, Any
import pandas as pd

from .config import logging
from .utils import stringify_value

RawDataType=List[Dict[str, Any]]

def transform_generic_data(raw_data: RawDataType) -> pd.DataFrame:
    """
    Transforms a list of raw JSON-like dictionaries into a clean, stringified DataFrame.
    Non-dict items are wrapped under a 'json_data' key.

    Args:
        raw_data (List[Dict[str, Any]]): The list of raw items to transform.

    Returns:
        pd.DataFrame: A DataFrame with stringified content.
    """
    logging.info("🔧 Starting transformation of raw data...")

    processed = []

    for idx, item in enumerate(raw_data):
        try:
            if isinstance(item, dict):
                processed_item = {
                    key: stringify_value(value) 
                    for key, value in item.items()
                }
            else:
                processed_item = {"json_data": stringify_value(item)}

            processed.append(processed_item)
        except Exception as e:
            logging.warning(f"⚠️ Skipping item at index {idx} due to error: {e}")

    df = pd.DataFrame(processed)

    logging.info(f"📊 Transformation complete: {len(df)} rows, {len(df.columns)} columns.")

    return df
