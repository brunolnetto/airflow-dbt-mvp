import requests
from typing import List, Dict
from time import sleep

from spacex.config import logging

def request_data(entity_url: str) -> List[Dict]:
    """
    Fetches data from a SpaceX API v4 entity URL with pagination.
    The entity_url should be the base URL for the entity (e.g., https://api.spacexdata.com/v4/launches).
    """
    query_url = f"{entity_url}/query"
    all_data: List[Dict] = []
    page = 1
    limit = 100  # SpaceX API v4 default limit for paginated queries is often 50, but can be set
    retries = 3

    logging.info(f"🚀 Starting paginated data fetch for entity URL: {entity_url}")

    while True:
        options_payload = {
            "options": {
                "page": page,
                "limit": limit,
                "pagination": True,
                # "sort": {"flight_number": "asc"} # Optional: Add sorting if needed
            }
        }
        
        logging.info(f"📄 Fetching page {page} with limit {limit} from {query_url}")

        for attempt in range(retries):
            try:
                response = requests.post(query_url, json=options_payload, timeout=20) # Increased timeout for potentially larger POST requests
                response.raise_for_status()  # Raise an exception for HTTP error codes

                try:
                    data = response.json()
                except requests.exceptions.JSONDecodeError:
                    logging.warning(f"⚠️ Failed to decode JSON response from {query_url} on page {page}, attempt {attempt + 1}/{retries}.")
                    if attempt == retries - 1:
                        logging.error(f"❌ All {retries} attempts failed to decode JSON for page {page}. Skipping this page or stopping.")
                        # Depending on requirements, we might return all_data or an empty list
                        # For now, let's assume we should stop and return what we have if a page fails decoding completely.
                        return all_data # Or raise an error, or return []
                    sleep(2**attempt) # Exponential backoff before retrying decode for the same page (though unlikely to help if content is bad)
                    continue # Retry fetching this page

                # Validate structure of paginated response
                if not isinstance(data, dict) or "docs" not in data or "hasNextPage" not in data:
                    logging.error(f"❌ Unexpected data structure in paginated response from {query_url} on page {page}: {data}")
                    return all_data # Return what has been accumulated so far

                docs = data.get("docs")
                if not isinstance(docs, list):
                    logging.error(f"❌ 'docs' field is not a list in response from {query_url} on page {page}: {docs}")
                    return all_data # Return what has been accumulated

                all_data.extend(docs)
                logging.info(f"✅ Added {len(docs)} items from page {page}. Total items: {len(all_data)}.")

                if not data.get("hasNextPage"):
                    logging.info(f"🏁 No more pages to fetch from {query_url}. Total items fetched: {len(all_data)}.")
                    return all_data

                page = data.get("nextPage", page + 1) # Use nextPage if available, otherwise increment
                if not page: # nextPage might be null if hasNextPage is false
                    logging.info(f"🏁 'nextPage' is null/false and 'hasNextPage' was true. Assuming end of data for {query_url}.")
                    return all_data

                sleep(0.5)  # Be polite to the API
                break  # Successful fetch of this page, break from retry loop and go to next page

            except requests.exceptions.RequestException as e:
                logging.error(f"❌ Failed to fetch page {page} from {query_url}: {e}")
                if attempt < retries - 1:
                    wait_time = 2**attempt
                    logging.info(f"🔁 Retrying page {page}... Attempt {attempt + 2}/{retries}, waiting {wait_time} seconds.")
                    sleep(wait_time)
                else:
                    logging.error(f"❌ All {retries} attempts failed for page {page} from {query_url}. Returning accumulated data.")
                    return all_data # Return what has been accumulated so far
        else:
            # This 'else' corresponds to the 'for attempt in range(retries)' loop.
            # It executes if the loop completed normally (i.e., no 'break' was hit, meaning all retries failed for a page)
            logging.error(f"❌ All retries failed for page {page} at {query_url}. Returning accumulated data: {len(all_data)} items.")
            return all_data

    return all_data # Should be unreachable if logic is correct, but as a fallback.
