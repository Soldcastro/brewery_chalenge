import pytest
from unittest.mock import MagicMock, patch
from scripts.process_silver import get_bronze_data, transform_bronze_to_silver, write_silver_data

def test_get_bronze_data_success():
    mock_spark = MagicMock()
    mock_df = MagicMock()
    mock_df.isEmpty.return_value = False
    mock_spark.read.json.return_value = mock_df

    result = get_bronze_data(mock_spark, "fake_path.json")
    assert result == mock_df
    mock_spark.read.json.assert_called_once_with("fake_path.json", multiLine=True)

def test_get_bronze_data_empty():
    mock_spark = MagicMock()
    mock_df = MagicMock()
    mock_df.isEmpty.return_value = True
    mock_spark.read.json.return_value = mock_df

    with pytest.raises(ValueError, match="Empty DataFrame: No data to process."):
        get_bronze_data(mock_spark, "fake_path.json")
    mock_spark.stop.assert_called_once()

def test_get_bronze_data_read_error():
    mock_spark = MagicMock()
    mock_spark.read.json.side_effect = IOError("read error")

    with pytest.raises(IOError, match="read error"):
        get_bronze_data(mock_spark, "fake_path.json")

def test_transform_bronze_to_silver():
    mock_df = MagicMock()
    with patch("scripts.process_silver.col") as mock_col:
        mock_df.select.return_value = "silver_df"
        result = transform_bronze_to_silver(mock_df)
        assert result == "silver_df"
        mock_df.select.assert_called_once()

def test_write_silver_data_success():
    mock_df = MagicMock()
    mock_write = mock_df.write.mode.return_value.partitionBy.return_value
    mock_write.parquet.return_value = None

    write_silver_data(mock_df, "fake_path")
    mock_df.write.mode.assert_called_once_with("overwrite")
    mock_df.write.mode.return_value.partitionBy.assert_called_once_with('country', 'brewery_type')
    mock_write.parquet.assert_called_once_with("fake_path")

def test_write_silver_data_ioerror():
    mock_df = MagicMock()
    mock_write = mock_df.write.mode.return_value.partitionBy.return_value
    mock_write.parquet.side_effect = IOError("disk full")

    write_silver_data(mock_df, "fake_path")
    mock_df.write.mode.assert_called_once_with("overwrite")
    mock_df.write.mode.return_value.partitionBy.assert_called_once_with('country', 'brewery_type')
    mock_write.parquet.assert_called_once_with("fake_path")