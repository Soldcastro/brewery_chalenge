import pytest
from unittest.mock import patch, mock_open
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
    data = [{"id": 1, "name": "Brewery"}]
    m = mock_open()
    with patch("builtins.open", m):
        write_breweries_to_file(data, "fake_path.json")
        m.assert_called_once_with("fake_path.json", 'w', encoding='utf8')

def test_write_breweries_to_file_empty():
    with pytest.raises(ValueError, match="No breweries data to write."):
        write_breweries_to_file([], "fake_path.json")

def test_write_breweries_to_file_ioerror():
    data = [{"id": 1, "name": "Brewery"}]
    with patch("builtins.open", side_effect=IOError("disk full")):
        with pytest.raises(IOError, match="disk full"):
            write_breweries_to_file(data, "fake_path.json")