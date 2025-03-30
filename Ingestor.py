# ingestor.py
from Polygon_Client import PolygonClient

class Ingestor:
    def __init__(self, config):
        # Initialize a registry of API clients. More clients can be added as needed.
        self.clients = {
            'polygon': PolygonClient(api_key=config.get('polygon_api_key')),
            # Add additional API clients here, e.g., 'alpha_vantage': AlphaVantageClient(api_key=config.get('alpha_vantage_api_key'))
        }
    
    def process_features(self, features):
        """
        Process pre-parsed features.
        Expected features dictionary includes:
        {
            'api': 'polygon',  # specifies which API client to use
            'ticker': 'AAPL',
            'multiplier': 1,
            'timespan': 'day',
            'from': '2023-01-01',
            'to': '2023-06-30'
        }
        """
        # Select the API client based on the 'api' key in features (defaults to 'polygon')
        api_choice = features.pop('api', 'polygon')
        client = self.clients.get(api_choice)
        if not client:
            raise ValueError(f"No client found for API: {api_choice}")
        
        # Fetch raw data using the chosen client and then parse it
        raw_data = client.fetch_data(features)
        return client.parse_response(raw_data)

if __name__ == "__main__":
    # Example usage:
    config = {'polygon_api_key': 'amT2HDpKSqyIvpdbz5DY9qLwWwPDpaB0'}
    ingestor = Ingestor(config)
    
    # Pre-parsed features provided directly to the ingestor
    features = {
        'api': 'polygon',
        'ticker': 'AAPL',
        'multiplier': 1,
        'timespan': 'day',
        'from': '2023-01-01',
        'to': '2023-06-30'
    }
    
    dataset = ingestor.process_features(features)
    print(dataset)
