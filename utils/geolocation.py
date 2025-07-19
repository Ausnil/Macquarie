import pandas as pd
import requests
from time import sleep
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# LocationIQ API setup
LOCATIONIQ_URL = "https://us1.locationiq.com/v1/search"
API_KEY = "pk.172cbd7cb4ab6fd935a6fc3fa0f06e44"  # Replace with your actual LocationIQ API key
HEADERS = {'User-Agent': 'address/1.0'}  # Polite usage header

def get_lat_long(address):
    """Get latitude/longitude using LocationIQ API."""
    try:
        params = {
            'key': API_KEY,
            'q': address,
            'format': 'json',
            'limit': 1  # Get only the top result
        }
        
        response = requests.get(LOCATIONIQ_URL, params=params, headers=HEADERS)
        response.raise_for_status()  # Raise HTTP errors
        
        data = response.json()
        if data:
            return float(data[0]['lat']), float(data[0]['lon'])
        else:
            logger.warning(f"No results found for address: {address}")
            return None, None
            
    except Exception as e:
        logger.error(f"Error geocoding address {address}: {str(e)}")
        return None, None

def add_geolocation_data(customers_df):
    """Add latitude and longitude columns using LocationIQ."""
    unique_addresses = customers_df['address'].unique()
    geo_data = {}
    
    for address in unique_addresses:
        lat, long = get_lat_long(address)
        geo_data[address] = {'latitude': lat, 'longitude': long}
        sleep(0.2)  # Light rate-limiting (LocationIQ allows 2 req/sec in free tier)
    
    customers_df['latitude'] = customers_df['address'].map(lambda x: geo_data[x]['latitude'])
    customers_df['longitude'] = customers_df['address'].map(lambda x: geo_data[x]['longitude'])
    
    return customers_df

# Example usage
if __name__ == "__main__":
    # Sample DataFrame
    customers_df = pd.DataFrame({
        'name': ['Alice', 'Bob'],
        'address': ['Eiffel Tower, Paris', 'Statue of Liberty, New York']
    })
    
    # Add geolocation data
    customers_df = add_geolocation_data(customers_df)
    print(customers_df)
