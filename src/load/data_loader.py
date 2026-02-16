"""
Data loading module for COVID-19 ETL Pipeline.
Loads transformed data into SQLite database.
CORRIGIDO para SQLAlchemy 2.0
"""

import sqlite3
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from pathlib import Path
from typing import Dict, List, Optional
import pandas as pd
import logging
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DataLoader:
    """Loads transformed data into SQLite database."""
    
    def __init__(self, config: Dict):
        """
        Initialize the data loader.
        
        Args:
            config: Configuration dictionary
        """
        self.config = config
        self.db_config = config['database']
        self.db_path = Path(self.db_config['path'])
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Create database engine
        self.engine = self._create_engine()
    
    def _create_engine(self):
        """
        Create SQLAlchemy engine.
        
        Returns:
            SQLAlchemy Engine
        """
        db_url = f"sqlite:///{self.db_path}"
        logger.info(f"Creating database engine: {self.db_path}")
        
        engine = create_engine(db_url, echo=False)
        
        # CORREÇÃO: Criar arquivo do banco imediatamente
        self.db_path.touch(exist_ok=True)
        
        logger.info("✓ Database engine created")
        return engine
    
    def create_tables(self):
        """Create database tables if they don't exist."""
        logger.info("Creating database tables...")
        
        # CORREÇÃO: Usar begin() ao invés de connect() para auto-commit
        with self.engine.begin() as conn:
            # COVID cases table
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS covid_cases (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    location TEXT NOT NULL,
                    iso_code TEXT,
                    date DATE NOT NULL,
                    total_cases REAL,
                    new_cases REAL,
                    total_deaths REAL,
                    new_deaths REAL,
                    total_cases_per_million REAL,
                    new_cases_per_million REAL,
                    total_deaths_per_million REAL,
                    new_deaths_per_million REAL,
                    case_fatality_rate REAL,
                    reproduction_rate REAL,
                    icu_patients REAL,
                    hosp_patients REAL,
                    positive_rate REAL,
                    tests_per_case REAL,
                    total_tests REAL,
                    new_tests REAL,
                    population REAL,
                    population_density REAL,
                    median_age REAL,
                    aged_65_older REAL,
                    aged_70_older REAL,
                    gdp_per_capita REAL,
                    cardiovasc_death_rate REAL,
                    diabetes_prevalence REAL,
                    life_expectancy REAL,
                    year INTEGER,
                    month INTEGER,
                    week INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            
            # Vaccinations table
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS vaccinations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    location TEXT NOT NULL,
                    iso_code TEXT,
                    date DATE NOT NULL,
                    total_vaccinations REAL,
                    people_vaccinated REAL,
                    people_fully_vaccinated REAL,
                    total_boosters REAL,
                    daily_vaccinations REAL,
                    daily_vaccinations_per_million REAL,
                    total_vaccinations_per_hundred REAL,
                    people_vaccinated_per_hundred REAL,
                    people_fully_vaccinated_per_hundred REAL,
                    vaccination_rate REAL,
                    full_vaccination_rate REAL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            
            # Aggregated statistics table
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS aggregated_stats (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    location TEXT NOT NULL,
                    iso_code TEXT,
                    last_updated DATE,
                    total_cases REAL,
                    cumulative_new_cases REAL,
                    total_deaths REAL,
                    cumulative_new_deaths REAL,
                    avg_case_fatality_rate REAL,
                    population REAL,
                    cases_per_100k REAL,
                    deaths_per_100k REAL,
                    data_points INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            
            # Global daily statistics table
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS global_daily_stats (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    date DATE NOT NULL UNIQUE,
                    global_new_cases REAL,
                    global_new_deaths REAL,
                    global_total_cases REAL,
                    global_total_deaths REAL,
                    countries_reporting INTEGER,
                    global_new_cases_7day_avg REAL,
                    global_new_deaths_7day_avg REAL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            
            # Create indices for better query performance
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_covid_location_date 
                ON covid_cases(location, date)
            """))
            
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_vacc_location_date 
                ON vaccinations(location, date)
            """))
            
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_global_date 
                ON global_daily_stats(date)
            """))
            
            # CORREÇÃO: Remover conn.commit() - begin() faz auto-commit
        
        logger.info("✓ Database tables created successfully")
    
    def load_parquet_to_df(self, parquet_path: Path) -> pd.DataFrame:
        """
        Load Parquet file to Pandas DataFrame.
        
        Args:
            parquet_path: Path to Parquet file/directory
            
        Returns:
            Pandas DataFrame
        """
        logger.info(f"Loading Parquet: {parquet_path.name}...")
        
        df = pd.read_parquet(parquet_path)
        
        logger.info(f"✓ Loaded {len(df):,} rows, {len(df.columns)} columns")
        
        return df
    
    def load_covid_data(self, parquet_path: Path, chunk_size: int = 10000):
        """
        Load COVID data into database.
        
        Args:
            parquet_path: Path to transformed COVID Parquet data
            chunk_size: Number of rows to insert at once
        """
        logger.info("Loading COVID data into database...")
        
        df = self.load_parquet_to_df(parquet_path)
        
        # Select and rename columns to match database schema
        columns_map = {
            'location': 'location',
            'iso_code': 'iso_code',
            'date': 'date',
            'total_cases': 'total_cases',
            'new_cases': 'new_cases',
            'total_deaths': 'total_deaths',
            'new_deaths': 'new_deaths',
            'total_cases_per_million': 'total_cases_per_million',
            'new_cases_per_million': 'new_cases_per_million',
            'total_deaths_per_million': 'total_deaths_per_million',
            'new_deaths_per_million': 'new_deaths_per_million',
            'case_fatality_rate': 'case_fatality_rate',
            'reproduction_rate': 'reproduction_rate',
            'icu_patients': 'icu_patients',
            'hosp_patients': 'hosp_patients',
            'positive_rate': 'positive_rate',
            'tests_per_case': 'tests_per_case',
            'total_tests': 'total_tests',
            'new_tests': 'new_tests',
            'population': 'population',
            'population_density': 'population_density',
            'median_age': 'median_age',
            'aged_65_older': 'aged_65_older',
            'aged_70_older': 'aged_70_older',
            'gdp_per_capita': 'gdp_per_capita',
            'cardiovasc_death_rate': 'cardiovasc_death_rate',
            'diabetes_prevalence': 'diabetes_prevalence',
            'life_expectancy': 'life_expectancy',
            'year': 'year',
            'month': 'month',
            'week': 'week'
        }
        
        # Filter to available columns
        available_cols = [col for col in columns_map.keys() if col in df.columns]
        df_filtered = df[available_cols]
        
        # Clear existing data
        with self.engine.begin() as conn:
            conn.execute(text("DELETE FROM covid_cases"))
        
        # Load in chunks
        total_rows = len(df_filtered)
        loaded = 0
        
        for i in range(0, total_rows, chunk_size):
            chunk = df_filtered.iloc[i:i+chunk_size]
            chunk.to_sql('covid_cases', self.engine, if_exists='append', index=False)
            loaded += len(chunk)
            
            if loaded % 50000 == 0:
                logger.info(f"  Progress: {loaded:,}/{total_rows:,} rows ({loaded/total_rows*100:.1f}%)")
        
        logger.info(f"✓ Loaded {total_rows:,} rows into covid_cases table")
    
    def load_vaccination_data(self, parquet_path: Path, chunk_size: int = 10000):
        """
        Load vaccination data into database.
        
        Args:
            parquet_path: Path to transformed vaccination Parquet data
            chunk_size: Number of rows to insert at once
        """
        logger.info("Loading vaccination data into database...")
        
        df = self.load_parquet_to_df(parquet_path)
        
        # Select and rename columns
        columns_map = {
            'location': 'location',
            'iso_code': 'iso_code',
            'date': 'date',
            'total_vaccinations': 'total_vaccinations',
            'people_vaccinated': 'people_vaccinated',
            'people_fully_vaccinated': 'people_fully_vaccinated',
            'total_boosters': 'total_boosters',
            'daily_vaccinations': 'daily_vaccinations',
            'daily_vaccinations_per_million': 'daily_vaccinations_per_million',
            'total_vaccinations_per_hundred': 'total_vaccinations_per_hundred',
            'people_vaccinated_per_hundred': 'people_vaccinated_per_hundred',
            'people_fully_vaccinated_per_hundred': 'people_fully_vaccinated_per_hundred',
            'vaccination_rate': 'vaccination_rate',
            'full_vaccination_rate': 'full_vaccination_rate'
        }
        
        available_cols = [col for col in columns_map.keys() if col in df.columns]
        df_filtered = df[available_cols]
        
        # Clear existing data
        with self.engine.begin() as conn:
            conn.execute(text("DELETE FROM vaccinations"))
        
        # Load in chunks
        total_rows = len(df_filtered)
        loaded = 0
        
        for i in range(0, total_rows, chunk_size):
            chunk = df_filtered.iloc[i:i+chunk_size]
            chunk.to_sql('vaccinations', self.engine, if_exists='append', index=False)
            loaded += len(chunk)
            
            if loaded % 50000 == 0:
                logger.info(f"  Progress: {loaded:,}/{total_rows:,} rows ({loaded/total_rows*100:.1f}%)")
        
        logger.info(f"✓ Loaded {total_rows:,} rows into vaccinations table")
    
    def load_aggregated_stats(self, parquet_path: Path):
        """
        Load aggregated statistics into database.
        
        Args:
            parquet_path: Path to aggregated Parquet data
        """
        logger.info("Loading aggregated statistics into database...")
        
        df = self.load_parquet_to_df(parquet_path)
        
        # Clear existing data
        with self.engine.begin() as conn:
            conn.execute(text("DELETE FROM aggregated_stats"))
        
        # Load data
        df.to_sql('aggregated_stats', self.engine, if_exists='append', index=False)
        
        logger.info(f"✓ Loaded {len(df):,} rows into aggregated_stats table")
    
    def load_global_daily_stats(self, parquet_path: Path):
        """
        Load global daily statistics into database.
        
        Args:
            parquet_path: Path to global daily Parquet data
        """
        logger.info("Loading global daily statistics into database...")
        
        df = self.load_parquet_to_df(parquet_path)
        
        # Clear existing data
        with self.engine.begin() as conn:
            conn.execute(text("DELETE FROM global_daily_stats"))
        
        # Load data
        df.to_sql('global_daily_stats', self.engine, if_exists='append', index=False)
        
        logger.info(f"✓ Loaded {len(df):,} rows into global_daily_stats table")
    
    def load_all(self, transformed_data_paths: Dict[str, Path]):
        """
        Load all transformed data into database.
        
        Args:
            transformed_data_paths: Dictionary mapping dataset names to paths
        """
        logger.info("\n" + "="*70)
        logger.info("STARTING DATA LOADING PROCESS")
        logger.info("="*70 + "\n")
        
        # Create tables
        self.create_tables()
        
        # Load datasets
        if 'covid_filtered' in transformed_data_paths:
            self.load_covid_data(transformed_data_paths['covid_filtered'])
        
        if 'vaccinations' in transformed_data_paths:
            self.load_vaccination_data(transformed_data_paths['vaccinations'])
        
        if 'covid_by_country' in transformed_data_paths:
            self.load_aggregated_stats(transformed_data_paths['covid_by_country'])
        
        if 'covid_by_date' in transformed_data_paths:
            self.load_global_daily_stats(transformed_data_paths['covid_by_date'])
        
        logger.info("\n" + "="*70)
        logger.info("LOADING SUMMARY")
        logger.info("="*70)
        self.print_database_stats()
        logger.info("="*70 + "\n")
    
    def print_database_stats(self):
        """Print statistics about loaded data."""
        with self.engine.connect() as conn:
            tables = ['covid_cases', 'vaccinations', 'aggregated_stats', 'global_daily_stats']
            
            for table in tables:
                try:
                    result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
                    count = result.scalar()
                    logger.info(f"✓ {table}: {count:,} rows")
                except Exception as e:
                    logger.warning(f"Could not get count for {table}: {e}")
    
    def query(self, sql: str) -> pd.DataFrame:
        """
        Execute SQL query and return results as DataFrame.
        
        Args:
            sql: SQL query string
            
        Returns:
            Query results as DataFrame
        """
        return pd.read_sql(sql, self.engine)
    
    def close(self):
        """Close database connection."""
        if self.engine:
            self.engine.dispose()
            logger.info("✓ Database connection closed")


def main():
    """Main execution function for testing."""
    import yaml
    
    # Load config
    with open('config/config.yaml', 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    
    # Get processed data paths
    processed_dir = Path(config['output']['processed_dir'])
    
    transformed_paths = {
        'covid_filtered': processed_dir / 'covid_filtered_countries',
        'vaccinations': processed_dir / 'vaccinations_transformed',
        'covid_by_country': processed_dir / 'covid_by_country',
        'covid_by_date': processed_dir / 'covid_by_date'
    }
    
    # Check if files exist
    existing_paths = {k: v for k, v in transformed_paths.items() if v.exists()}
    
    if not existing_paths:
        logger.error("No transformed data found. Run transformation first.")
        return
    
    # Load data
    loader = DataLoader(config)
    
    try:
        loader.load_all(existing_paths)
        logger.info("✓ Data loading complete!")
    finally:
        loader.close()


if __name__ == "__main__":
    main()