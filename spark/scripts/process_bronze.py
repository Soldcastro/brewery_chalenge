""""
Brewery Challenge - Process Bronze Layer
"""
import json
import requests
from minio import Minio
from minio.error import S3Error
from io import BytesIO

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

def create_minio_client():
    """
    Creates a MinIO client to interact with the MinIO server.
    :return: MinIO client instance
    """
    try:
        client = Minio(
            "minio:9000",
            access_key="testuser",
            secret_key="password",
            secure=False
        )
        return client
    except S3Error as e:
        print(f"Failed to create MinIO client: {e}")
        raise

def write_breweries_to_file(client, bucket, path, breweries):
    """
    Writes the breweries data to a JSON file in the specified path.
    :param breweries: List of breweries data to write
    :param path: The file path where the data should be written
    """
    if not breweries:
        raise ValueError("No breweries data to write.")
    try:
        # Ensure the bucket exists
        if not client.bucket_exists(bucket):
            client.make_bucket(bucket)
        
        # Convert breweries data to JSON and write to file
        data = BytesIO(json.dumps(breweries).encode('utf-8'))
        client.put_object(
            bucket,
            path, 
            data = data,
            length = data.getbuffer().nbytes,
            content_type='application/json'
            )
    except S3Error as e:
        print(f"Failed to write data to file: {e}")
        raise

def main():
    '''
    Function to fetch breweries data from the Open Brewery DB API 
    and write it to a JSON file in the bronze layer.
    '''
    url = 'https://api.openbrewerydb.org/v1/breweries'
    bucket = 'datalake'
    path = 'bronze/bronze_breweries.json'
    breweries = fetch_breweries(url)
    minio_client = create_minio_client()
    write_breweries_to_file(minio_client, bucket, path, breweries)
    print("Data fetched and written to bronze layer successfully.")

if __name__ == "__main__":
    main()
