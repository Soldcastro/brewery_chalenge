""""
Brewery Challenge - Process Bronze Layer
"""
import json
import requests

# Define the functions to fetch breweries data from the API and write it to a file
def fetch_breweries(url):
    """
    Fetches breweries data from the Open Brewery DB API.
    :param url: The API endpoint to fetch data from
    :return: JSON response containing breweries data
    """
    try:
        response = requests.get(url, timeout=30)
    except requests.RequestException as e:
        print(f"Error fetching data from {url}: {e}")
        raise
    # Check if the response is successful
    if response.status_code == 200:
        return response.json()

def write_breweries_to_file(breweries, path):
    """
    Writes the breweries data to a JSON file in the specified path.
    :param breweries: List of breweries data to write
    :param path: The file path where the data should be written
    """
    if not breweries:
        raise ValueError("No breweries data to write.")
    try:
        with open(path, 'w', encoding='utf8') as f:
            json.dump(breweries, f, indent=4)
    except IOError as e:
        print(f"Failed to write data to file: {e}")
        raise

def main():
    '''
    Function to fetch breweries data from the Open Brewery DB API 
    and write it to a JSON file in the bronze layer.
    '''
    url = 'https://api.openbrewerydb.org/v1/breweries'
    path = '/datalake/bronze/bronze_breweries.json'
    breweries = fetch_breweries(url)
    write_breweries_to_file(breweries, path)
    print("Data fetched and written to bronze layer successfully.")

if __name__ == "__main__":
    main()
