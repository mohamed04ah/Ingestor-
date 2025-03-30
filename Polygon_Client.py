# polygon_client.py
from base_API_client import BaseAPIClient
from polygon import RESTClient  # Ensure the official client is installed and imported

class PolygonClient(BaseAPIClient):
    def __init__(self, api_key):
        # Initialize the official Polygon REST client with your API key.
        self.client = RESTClient(api_key)

    def fetch_data(self, features):
        """
        Use the official client's method to fetch aggregated data.
        Expected keys in features: 'ticker', 'multiplier', 'timespan', 'from', 'to'
        """
        response = self.client.get_aggs(
            ticker=features['ticker'],
            multiplier=features['multiplier'],
            timespan=features['timespan'],
            from_=features['from'],  # 'from_' avoids conflict with the reserved word 'from'
            to=features['to']
        )
        return response

    def parse_response(self, response):
        """
        Parse the response from the official client.
        If the response is a dictionary with a 'results' key, return its value.
        If it's already a list, return it as is.
        """
        if isinstance(response, dict):
            return response.get('results', [])
        elif isinstance(response, list):
            return response
        else:
            raise ValueError(f"Unexpected response type: {type(response)}")