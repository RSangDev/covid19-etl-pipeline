"""
Tests for data loader module - CORRIGIDO FINAL
"""

import pytest
from pathlib import Path
import pandas as pd
from src.load.data_loader import DataLoader


@pytest.fixture
def sample_config(tmp_path):
    """Sample configuration for testing."""
    db_path = tmp_path / 'test.db'
    return {
        'database': {
            'type': 'sqlite',
            'path': str(db_path),
            'tables': {
                'covid_cases': 'covid_cases',
                'vaccinations': 'vaccinations',
                'aggregated_stats': 'aggregated_stats'
            }
        },
        'output': {
            'processed_dir': str(tmp_path)
        }
    }


@pytest.fixture
def loader(sample_config):
    """Create loader instance."""
    return DataLoader(sample_config)


class TestDataLoader:
    """Test cases for DataLoader class."""
    
    def test_initialization(self, loader):
        """Test loader initialization."""
        assert loader is not None
        assert loader.engine is not None
        # CORREÇÃO: db_path é criado automaticamente no _create_engine
        assert loader.db_path.parent.exists()
    
    def test_create_tables(self, loader):
        """Test database table creation."""
        loader.create_tables()
        
        # Query to check if tables exist
        result = loader.query(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )
        
        table_names = result['name'].tolist()
        assert 'covid_cases' in table_names
        assert 'vaccinations' in table_names
        assert 'aggregated_stats' in table_names
        assert 'global_daily_stats' in table_names
    
    def test_load_parquet_to_df(self, loader, tmp_path):
        """Test loading Parquet file to DataFrame."""
        # Create test Parquet
        test_file = tmp_path / 'test.parquet'
        df = pd.DataFrame({
            'col1': [1, 2, 3],
            'col2': ['a', 'b', 'c']
        })
        df.to_parquet(test_file)
        
        # Load
        loaded_df = loader.load_parquet_to_df(test_file)
        
        assert loaded_df is not None
        assert len(loaded_df) == 3
        assert list(loaded_df.columns) == ['col1', 'col2']
    
    def test_query(self, loader):
        """Test SQL query execution."""
        loader.create_tables()
        
        # CORREÇÃO: Usar colunas que existem na tabela aggregated_stats
        test_df = pd.DataFrame({
            'location': ['Brazil', 'USA'],
            'iso_code': ['BRA', 'USA'],
            'last_updated': ['2024-01-01', '2024-01-01'],  # Mudado de 'date'
            'total_cases': [1000.0, 2000.0],
            'total_deaths': [10.0, 20.0]
        })
        
        test_df.to_sql('aggregated_stats', loader.engine, if_exists='append', index=False)
        
        # Query
        result = loader.query("SELECT * FROM aggregated_stats")
        
        assert len(result) == 2
        assert 'location' in result.columns
        assert 'last_updated' in result.columns  # Mudado de 'date'
    
    def test_close(self, loader):
        """Test closing database connection."""
        loader.close()
        # Should not raise exception
        assert True