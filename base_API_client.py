# base_api_client.py
from abc import ABC, abstractmethod

class BaseAPIClient(ABC):
    @abstractmethod
    def fetch_data(self, features):
        """Fetch data based on pre-parsed features."""
        pass

    @abstractmethod
    def parse_response(self, response_package):
        """Parse and standardize the API response.
           Returns a tuple: (results, gathered_features)
        """
        pass
    
