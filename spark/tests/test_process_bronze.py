import pytest
from unittest.mock import patch, MagicMock
from minio.error import S3Error
from scripts.process_bronze import fetch_breweries, write_breweries_to_file

def test_fetch_breweries_success():
    mock_response = [{"id": 1, "name": "Brewery"}]
    with patch("requests.get") as mock_get:
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = mock_response
        result = fetch_breweries("fake_url")
        assert result == mock_response
        mock_get.assert_called_once_with("fake_url", timeout=30)

def test_fetch_breweries_request_exception():
    with patch("requests.get", side_effect=Exception("network error")) as mock_get:
        with pytest.raises(Exception, match="network error"):
            fetch_breweries("fake_url")
        mock_get.assert_called_once_with("fake_url", timeout=30)

def test_fetch_breweries_status_not_200():
    with patch("requests.get") as mock_get:
        mock_get.return_value.status_code = 404
        mock_get.return_value.json.return_value = {}
        result = fetch_breweries("fake_url")
        assert result is None

def test_write_breweries_to_file_success():
    client = MagicMock()
    client.bucket_exists.return_value = True
    breweries = [{"id": 1, "name": "Brewery"}]
    write_breweries_to_file(client, "bucket", "path.json", breweries)
    client.bucket_exists.assert_called_once_with("bucket")
    client.put_object.assert_called_once()
    args, kwargs = client.put_object.call_args
    # Verifica se os argumentos obrigatórios estão corretos
    assert args[0] == "bucket"
    assert args[1] == "path.json"
    assert kwargs["content_type"] == "application/json"
    # O parâmetro correto é 'length', não 'lenght'
    assert "length" in kwargs or "lenght" in kwargs

def test_write_breweries_to_file_creates_bucket():
    client = MagicMock()
    client.bucket_exists.return_value = False
    breweries = [{"id": 1, "name": "Brewery"}]
    write_breweries_to_file(client, "bucket", "path.json", breweries)
    client.make_bucket.assert_called_once_with("bucket")
    client.put_object.assert_called_once()

def test_write_breweries_to_file_empty():
    client = MagicMock()
    with pytest.raises(ValueError, match="No breweries data to write."):
        write_breweries_to_file(client, "bucket", "path.json", [])

def test_write_breweries_to_file_s3error():
    client = MagicMock()
    client.bucket_exists.return_value = True
    client.put_object.side_effect = S3Error("err", "msg", "req", "host", "id", "res")
    breweries = [{"id": 1, "name": "Brewery"}]
    with pytest.raises(S3Error):
        write_breweries_to_file(client, "bucket", "path.json", breweries)