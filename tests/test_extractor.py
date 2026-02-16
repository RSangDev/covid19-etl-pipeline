"""
Tests for data extractor module - CORRIGIDO
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path
import pandas as pd
from src.extract.data_extractor import DataExtractor


@pytest.fixture
def sample_config():
    """Sample configuration for testing."""
    return {
        'data_sources': {
            'owid_covid': {
                'url': 'https://example.com/covid.csv',
                'format': 'csv'
            },
            'owid_vaccinations': {
                'url': 'https://example.com/vaccinations.csv',
                'format': 'csv'
            }
        },
        'output': {
            'raw_dir': 'data/raw'
        }
    }


@pytest.fixture
def extractor(sample_config, tmp_path):
    """Create extractor with temporary directory."""
    sample_config['output']['raw_dir'] = str(tmp_path)
    return DataExtractor(sample_config)


class TestDataExtractor:
    """Test cases for DataExtractor class."""
    
    def test_initialization(self, extractor):
        """Test extractor initialization."""
        assert extractor is not None
        assert extractor.raw_dir.exists()
    
    @patch('src.extract.data_extractor.requests.get')
    def test_download_file_success(self, mock_get, extractor):
        """Test successful file download."""
        # Mock response
        mock_response = Mock()
        mock_response.content = b"test,data\n1,2\n"
        mock_response.headers = {'content-length': '16'}
        mock_response.raise_for_status = Mock()
        mock_response.iter_content = Mock(return_value=[b"test,data\n1,2\n"])
        mock_get.return_value = mock_response
        
        # Download file
        result = extractor.download_file(
            'https://example.com/test.csv',
            'test.csv'
        )
        
        assert result is not None
        assert result.exists()
        assert result.name == 'test.csv'
    
    @patch('src.extract.data_extractor.requests.get')
    @patch('src.extract.data_extractor.time.sleep')  # Mock sleep
    def test_download_file_retry_on_failure(self, mock_sleep, mock_get, extractor):
        """Test retry logic on download failure."""
        # CORREÇÃO: Usar side_effect com RequestException
        import requests
        mock_get.side_effect = requests.exceptions.RequestException("Network error")
        
        result = extractor.download_file(
            'https://example.com/test.csv',
            'test.csv',
            retries=2
        )
        
        assert result is None
        assert mock_get.call_count == 2
        assert mock_sleep.call_count == 1  # Só dorme entre tentativas
    
    def test_validate_extracted_data_valid(self, extractor, tmp_path):
        """Test validation of valid CSV file."""
        # Create a valid CSV
        test_file = tmp_path / 'valid.csv'
        df = pd.DataFrame({
            'col1': [1, 2, 3],
            'col2': ['a', 'b', 'c']
        })
        df.to_csv(test_file, index=False)
        
        assert extractor.validate_extracted_data(test_file) is True
    
    def test_validate_extracted_data_empty(self, extractor, tmp_path):
        """Test validation of empty CSV file."""
        # Create an empty CSV
        test_file = tmp_path / 'empty.csv'
        test_file.write_text('col1,col2\n')
        
        assert extractor.validate_extracted_data(test_file) is False
    
    @patch('src.extract.data_extractor.DataExtractor.download_file')
    def test_extract_owid_covid_data(self, mock_download, extractor, tmp_path):
        """Test COVID data extraction."""
        # CORREÇÃO: Criar arquivo real ao invés de só retornar Path
        test_file = tmp_path / 'covid.csv'
        test_file.write_text('test,data\n1,2\n')
        mock_download.return_value = test_file
        
        result = extractor.extract_owid_covid_data()
        
        assert result is not None
        assert result.exists()
        assert mock_download.called
    
    @patch('src.extract.data_extractor.DataExtractor.extract_owid_covid_data')
    @patch('src.extract.data_extractor.DataExtractor.extract_vaccination_data')
    def test_extract_all(self, mock_vacc, mock_covid, extractor, tmp_path):
        """Test extracting all data sources."""
        # CORREÇÃO: Criar arquivos reais
        covid_file = tmp_path / 'covid.csv'
        vacc_file = tmp_path / 'vacc.csv'
        
        covid_file.write_text('test,data\n1,2\n')
        vacc_file.write_text('test,data\n3,4\n')
        
        mock_covid.return_value = covid_file
        mock_vacc.return_value = vacc_file
        
        results = extractor.extract_all()
        
        assert 'covid_data' in results
        assert 'vaccination_data' in results
        assert results['covid_data'].exists()
        assert results['vaccination_data'].exists()
        assert mock_covid.called
        assert mock_vacc.called