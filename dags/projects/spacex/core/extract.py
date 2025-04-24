import requests
from typing import List, Dict
from time import sleep

from spacex.config import logging

def request_data(url: str) -> List[Dict]:
    # Retry logic for transient failures
    retries = 3
    for attempt in range(retries):
        try:
            response = requests.get(url, timeout=10)  # Set a timeout of 10 seconds
            response.raise_for_status()  # Raise an exception for HTTP error codes
            data = response.json()
            
            if not isinstance(data, list):
                logging.error(f"❌ Unexpected data format for url '{url}': {data}")
                return []

            logging.info(f"✅ Data fetched for url: {url}")
            return data
        
        except requests.exceptions.RequestException as e:
            logging.error(f"❌ Failed to fetch data from {url}: {e}")
            if attempt < retries - 1:
                logging.info(f"🔁 Retrying... Attempt {attempt + 2}/{retries}")
                sleep(2)  # Wait for 2 seconds before retrying
            else:
                logging.error(f"❌ All {retries} attempts failed. Giving up.")
                return []
    
    return []  # Return empty list if all retries fail
