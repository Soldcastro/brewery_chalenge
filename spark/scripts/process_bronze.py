# import libs
import requests
import json
import sys

# URL for the Open Brewery DB API to fetch breweries data
url = 'https://api.openbrewerydb.org/v1/breweries'

# Fetching the breweries data from the API and writes it to a JSON file
response = requests.get(url, timeout=30)
if response.status_code == 200:
    breweries = response.json()
    try:
        with open('/datalake/bronze/bronze_breweries.json', 'w', encoding='utf8') as f:
            json.dump(breweries, f, indent=4)
        print("Data fetched and written to bronze layer successfully.")
    except IOError as e:
        print(f"Error writing to file: {e}")
        raise
else:
    print(f"Failed to retrieve data: {response.status_code}")
    print(response.text)
    sys.exit(1)
