import pytest
from unittest.mock import patch, MagicMock
from scripts.process_gold import get_silver_data, process_gold, write_gold_data

# Teste de sucesso para get_silver_data
def test_get_silver_data_success():
    mock_spark = MagicMock()
    mock_df = MagicMock()
    mock_df.isEmpty.return_value = False
    mock_spark.read.parquet.return_value = mock_df
    with patch("scripts.process_gold.spark", mock_spark):
        result = get_silver_data("fake_path")
        assert result == mock_df
        mock_spark.read.parquet.assert_called_once_with("fake_path")

# Teste de falha para get_silver_data (DataFrame vazio)
def test_get_silver_data_empty():
    mock_spark = MagicMock()
    mock_df = MagicMock()
    mock_df.isEmpty.return_value = True
    mock_spark.read.parquet.return_value = mock_df
    with patch("scripts.process_gold.spark", mock_spark):
        with pytest.raises(ValueError, match="Empty DataFrame: No data to process."):
            get_silver_data("fake_path")
        mock_spark.stop.assert_called_once()

# Teste de falha para get_silver_data (erro de leitura)
def test_get_silver_data_read_error():
    mock_spark = MagicMock()
    mock_spark.read.parquet.side_effect = Exception("read error")
    with patch("scripts.process_gold.spark", mock_spark):
        with pytest.raises(Exception, match="read error"):
            get_silver_data("fake_path")

# Teste de sucesso para process_gold
def test_process_gold_success():
    mock_df = MagicMock()
    mock_grouped = MagicMock()
    mock_counted = MagicMock()
    mock_renamed = MagicMock()
    mock_df.groupBy.return_value = mock_grouped
    mock_grouped.count.return_value = mock_counted
    mock_counted.withColumnRenamed.return_value = mock_renamed

    result = process_gold(mock_df)
    assert result == mock_renamed
    mock_df.groupBy.assert_called_once_with("country", "brewery_type")
    mock_grouped.count.assert_called_once()
    mock_counted.withColumnRenamed.assert_called_once_with("count", "brewery_count")

# Teste de falha para process_gold (erro de agregação)
def test_process_gold_aggregation_error():
    mock_df = MagicMock()
    mock_df.groupBy.side_effect = Exception("agg error")
    with pytest.raises(Exception, match="agg error"):
        process_gold(mock_df)

# Teste de sucesso para write_gold_data
def test_write_gold_data_success():
    mock_df = MagicMock()
    mock_write = mock_df.write.mode.return_value
    mock_write.parquet.return_value = None
    write_gold_data(mock_df, "fake_path")
    mock_df.write.mode.assert_called_once_with("overwrite")
    mock_write.parquet.assert_called_once_with("fake_path")

# Teste de falha para write_gold_data (erro de escrita)
def test_write_gold_data_error():
    mock_df = MagicMock()
    mock_write = mock_df.write.mode.return_value
    mock_write.parquet.side_effect = Exception("write error")
    with pytest.raises(Exception, match="write error"):
        write_gold_data(mock_df, "fake_path")