# ingestor.py
import pandas as pd
from typing import Dict, Any, List, Union
from Polygon_Client import PolygonClient
from alpha_vantage_client import AlphaVantageClient

class Ingestor:
    def __init__(self, config: Dict[str, Any]):
        """
        Initializes the Ingestor with a registry of API clients.
        """
        self.clients = {
            'polygon': PolygonClient(api_key=config.get('polygon_api_key')),
            'alpha_vantage': AlphaVantageClient(api_key=config.get('alpha_vantage_api_key')),
        }

    def process_features(
        self,
        features: Union[Dict[str, Any], List[Dict[str, Any]]]
    ) -> List[Dict[str, Any]]:
        """
        Process API requests and return a list of dictionaries containing
        the API used, the request features, and the resulting DataFrame.
        """
        requests = features if isinstance(features, list) else [features]
        outputs: List[Dict[str, Any]] = []

        for feat in requests:
            # Make a copy to safely modify for the client call
            feat_copy = feat.copy()
            api_choice = feat_copy.pop('api', 'polygon')
            client = self.clients.get(api_choice)

            if not client:
                raise ValueError(f"No client found for API: {api_choice}")

            raw_pkg = client.fetch_data(feat_copy)
            df = client.parse_response(raw_pkg)

            # Append the result dictionary to the outputs list
            outputs.append({
                'api': api_choice,
                'features': feat,  # The original, unmodified feature dictionary
                'df': df
            })

        return outputs

if __name__ == "__main__":
    config = {
        'polygon_api_key': 'pEP9v2lGpSGlWpMrWdHXqprZsV5MYYbc', 
        'alpha_vantage_api_key': 'YOUR_ALPHA_VANTAGE_API_KEY'
    }
    ingestor = Ingestor(config)
    
    # Example usage with two different API requests
    feature_requests = [
        {'api': 'polygon', 'ticker': 'AAPL', 'multiplier': 1, 'timespan': 'day', 'from': '2023-01-01', 'to': '2023-02-01', 'endpoint_type': 0},
        {'api': 'alpha_vantage', 'ticker': 'MSFT', 'timespan': 'day', 'outputsize': 'compact'}
    ]
    
    results = ingestor.process_features(feature_requests)
    
    print(f"Processed {len(results)} requests.\n")
    
    for res in results:
        print(f"--- Results for API: {res['api']} ---")
        print(f"Request Features: {res['features']}")
        
        # Check if the DataFrame is not empty before printing info
        if not res['df'].empty:
            print(f"DataFrame Shape: {res['df'].shape}")
            print("DataFrame Head:")
            print(res['df'].head())
        else:
            print("DataFrame is empty.")
        print("-" * 30 + "\n")
