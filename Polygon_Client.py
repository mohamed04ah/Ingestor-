# polygon_client.py
import time
import pandas as pd
from base_API_client import BaseAPIClient
from polygon import RESTClient  # Ensure the official client is installed and imported

class PolygonClient(BaseAPIClient):
    def __init__(self, api_key):
        # Initialize the official Polygon REST client with your API key.
        self.client = RESTClient(api_key)

    def fetch_data(self, features):
        """
        Fetch data using the official client's methods.
        Uses 'endpoint_type' from features:
          - 0 -> get_aggs endpoint
          - 1 -> daily_market_summary endpoint
        Returns a dictionary with the raw response data and the features used.
        """
        attempts = 0
        max_attempts = 3
        delay = 2  # seconds between retries
        
        while attempts < max_attempts:
            try:
                endpoint_type = features.get("endpoint_type", 0)
                if endpoint_type == 0:
                    # Call the aggregates endpoint
                    response = self.client.get_aggs(
                        ticker=features['ticker'],
                        multiplier=features['multiplier'],
                        timespan=features['timespan'],
                        from_=features['from'],  # 'from_' avoids conflict with reserved word 'from'
                        to=features['to']
                    )
                elif endpoint_type == 1:
                    # Call the daily market summary endpoint.
                    # (Assuming the official client exposes a method named daily_market_summary.
                    # Adjust parameters as per the official documentation.)
                    response = self.client.daily_market_summary(ticker=features['ticker'])
                else:
                    raise ValueError(f"Invalid endpoint_type: {endpoint_type}")
                
                # If call is successful, return a package containing both data and the used features.
                return {"data": response, "features": features}
            
            except Exception as e:
                attempts += 1
                if attempts == max_attempts:
                    print(f"Request failed after {max_attempts} attempts: {e}")
                    raise e
                else:
                    print(f"Attempt {attempts} failed: {e}. Retrying in {delay} seconds...")
                    time.sleep(delay)

    def parse_response(self, response_package):
        """
        Parse the response package returned by fetch_data.
        If the raw data is a dictionary with a 'results' key, extract its value.
        If it's already a list, use it directly.
        Converts the results into a pandas DataFrame.
        Returns a tuple: (dataframe, gathered_features)
        """
        response = response_package["data"]
        gathered_features = response_package["features"]
        if isinstance(response, dict):
            results = response.get('results', [])
        elif isinstance(response, list):
            results = response
        else:
            raise ValueError(f"Unexpected response type: {type(response)}")
        
        df = pd.DataFrame(results)
        return df, gathered_features
