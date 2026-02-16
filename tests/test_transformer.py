"""
Tests for data transformer module - CORRIGIDO
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from src.transform.data_transformer import DataTransformer


@pytest.fixture
def sample_config():
    """Sample configuration for testing."""
    return {
        'spark': {
            'app_name': 'COVID19-ETL-Test',
            'master': 'local[1]',
            'config': {
                'spark.sql.shuffle.partitions': 2,
                'spark.driver.memory': '1g',
                'spark.executor.memory': '1g'
            }
        },
        'output': {
            'processed_dir': 'data/processed'
        },
        'processing': {
            'countries_of_interest': ['Brazil', 'United States']
        }
    }


@pytest.fixture
def transformer(sample_config, tmp_path):
    """Create transformer with temporary directory."""
    sample_config['output']['processed_dir'] = str(tmp_path)
    return DataTransformer(sample_config)


@pytest.fixture
def spark_session():
    """Create Spark session for testing."""
    spark = SparkSession.builder \
        .appName("test") \
        .master("local[1]") \
        .getOrCreate()
    yield spark
    spark.stop()


class TestDataTransformer:
    """Test cases for DataTransformer class."""
    
    def test_initialization(self, transformer):
        """Test transformer initialization."""
        assert transformer is not None
        assert transformer.spark is not None
        assert transformer.processed_dir.exists()
    
    def test_load_csv_to_spark(self, transformer, tmp_path):
        """Test loading CSV into Spark DataFrame."""
        # Create test CSV
        test_file = tmp_path / 'test.csv'
        df = pd.DataFrame({
            'col1': [1, 2, 3],
            'col2': ['a', 'b', 'c']
        })
        df.to_csv(test_file, index=False)
        
        # Load into Spark
        spark_df = transformer.load_csv_to_spark(test_file)
        
        assert spark_df is not None
        assert spark_df.count() == 3
        assert len(spark_df.columns) == 2
    
    def test_transform_covid_data(self, transformer, spark_session):
        """Test COVID data transformation."""
        # CORREÇÃO: Definir schema explícito para evitar CANNOT_DETERMINE_TYPE
        schema = StructType([
            StructField('date', StringType(), True),
            StructField('location', StringType(), True),
            StructField('total_cases', DoubleType(), True),
            StructField('total_deaths', DoubleType(), True),
            StructField('iso_code', StringType(), True)
        ])
        
        # Create sample data
        data = [
            ('2024-01-01', 'Brazil', 1000.0, 10.0, 'BRA'),
            ('2024-01-02', 'Brazil', 1100.0, 12.0, 'BRA')
        ]
        
        df = spark_session.createDataFrame(data, schema)
        
        # Transform
        transformed = transformer.transform_covid_data(df)
        
        # Check new columns exist
        assert 'case_fatality_rate' in transformed.columns
        assert 'year' in transformed.columns
        assert 'month' in transformed.columns
        
        # Verify calculations
        result = transformed.collect()
        assert len(result) == 2
        # First row: case_fatality_rate should be (10/1000)*100 = 1.0
        assert abs(result[0]['case_fatality_rate'] - 1.0) < 0.01
    
    def test_filter_countries_of_interest(self, transformer, spark_session):
        """Test filtering to specific countries."""
        # Define schema
        schema = StructType([
            StructField('location', StringType(), True),
            StructField('total_cases', IntegerType(), True)
        ])
        
        # Create sample data
        data = [
            ('Brazil', 1000),
            ('United States', 2000),
            ('Germany', 3000)
        ]
        
        df = spark_session.createDataFrame(data, schema)
        
        # Filter
        filtered = transformer.filter_countries_of_interest(
            df,
            ['Brazil', 'United States']
        )
        
        assert filtered.count() == 2
        locations = [row['location'] for row in filtered.collect()]
        assert 'Brazil' in locations
        assert 'United States' in locations
        assert 'Germany' not in locations
    
    def test_get_statistics(self, transformer, spark_session):
        """Test getting DataFrame statistics."""
        schema = StructType([
            StructField('location', StringType(), True),
            StructField('total_cases', IntegerType(), True)
        ])
        
        data = [('Brazil', 1000), ('USA', 2000)]
        df = spark_session.createDataFrame(data, schema)
        
        stats = transformer.get_statistics(df)
        
        assert stats['row_count'] == 2
        assert stats['column_count'] == 2
        assert 'location' in stats['columns']