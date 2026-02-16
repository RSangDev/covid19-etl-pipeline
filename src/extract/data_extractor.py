"""
Data extraction module for COVID-19 ETL Pipeline.
Downloads data from Our World in Data and other public sources.
"""

import requests
import pandas as pd
import logging
from pathlib import Path
from typing import Dict, Optional
from datetime import datetime
import time

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class DataExtractor:
    """Extracts COVID-19 data from public sources."""

    def __init__(self, config: Dict):
        """
        Initialize the data extractor.

        Args:
            config: Configuration dictionary with data sources
        """
        self.config = config
        self.data_sources = config["data_sources"]
        self.raw_dir = Path(config["output"]["raw_dir"])
        self.raw_dir.mkdir(parents=True, exist_ok=True)

    def download_file(
        self, url: str, filename: str, timeout: int = 60, retries: int = 3
    ) -> Optional[Path]:
        """
        Download a file from URL with retry logic.

        Args:
            url: URL to download from
            filename: Local filename to save
            timeout: Request timeout in seconds
            retries: Number of retry attempts

        Returns:
            Path to downloaded file or None if failed
        """
        filepath = self.raw_dir / filename

        for attempt in range(retries):
            try:
                logger.info(
                    f"Downloading {filename} (attempt {attempt + 1}/{retries})..."
                )

                response = requests.get(url, timeout=timeout, stream=True)
                response.raise_for_status()

                # Get file size for progress
                total_size = int(response.headers.get("content-length", 0))

                with open(filepath, "wb") as f:
                    if total_size == 0:
                        f.write(response.content)
                    else:
                        downloaded = 0
                        for chunk in response.iter_content(chunk_size=8192):
                            if chunk:
                                f.write(chunk)
                                downloaded += len(chunk)
                                progress = (downloaded / total_size) * 100
                                if downloaded % (1024 * 1024) == 0:  # Log every MB
                                    logger.info(f"Progress: {progress:.1f}%")

                logger.info(f"✓ Successfully downloaded {filename}")
                logger.info(f"  Size: {filepath.stat().st_size / (1024*1024):.2f} MB")
                return filepath

            except requests.exceptions.RequestException as e:
                logger.warning(f"Attempt {attempt + 1} failed: {e}")
                if attempt < retries - 1:
                    wait_time = 2**attempt  # Exponential backoff
                    logger.info(f"Waiting {wait_time}s before retry...")
                    time.sleep(wait_time)
                else:
                    logger.error(
                        f"Failed to download {filename} after {retries} attempts"
                    )
                    return None

    def extract_owid_covid_data(self) -> Optional[Path]:
        """
        Extract main COVID-19 data from Our World in Data.

        Returns:
            Path to downloaded CSV file
        """
        source = self.data_sources["owid_covid"]
        filename = f"owid_covid_data_{datetime.now().strftime('%Y%m%d')}.csv"

        logger.info("=" * 60)
        logger.info("EXTRACTING: Our World in Data - COVID-19 Main Dataset")
        logger.info("=" * 60)

        return self.download_file(source["url"], filename)

    def extract_vaccination_data(self) -> Optional[Path]:
        """
        Extract vaccination data from Our World in Data.

        Returns:
            Path to downloaded CSV file
        """
        source = self.data_sources["owid_vaccinations"]
        filename = f"owid_vaccinations_{datetime.now().strftime('%Y%m%d')}.csv"

        logger.info("=" * 60)
        logger.info("EXTRACTING: Our World in Data - Vaccination Data")
        logger.info("=" * 60)

        return self.download_file(source["url"], filename)

    def extract_all(self) -> Dict[str, Optional[Path]]:
        """
        Extract all configured data sources.

        Returns:
            Dictionary mapping source names to file paths
        """
        results = {}

        logger.info("\n" + "=" * 70)
        logger.info("STARTING DATA EXTRACTION PROCESS")
        logger.info("=" * 70 + "\n")

        start_time = time.time()

        # Extract COVID main data
        results["covid_data"] = self.extract_owid_covid_data()

        # Extract vaccination data
        results["vaccination_data"] = self.extract_vaccination_data()

        elapsed_time = time.time() - start_time

        # Summary
        logger.info("\n" + "=" * 70)
        logger.info("EXTRACTION SUMMARY")
        logger.info("=" * 70)

        successful = sum(1 for path in results.values() if path is not None)
        total = len(results)

        for name, path in results.items():
            status = "✓ SUCCESS" if path else "✗ FAILED"
            logger.info(f"{status}: {name}")
            if path:
                size_mb = path.stat().st_size / (1024 * 1024)
                logger.info(f"  → {path.name} ({size_mb:.2f} MB)")

        logger.info(f"\nTotal: {successful}/{total} successful")
        logger.info(f"Time elapsed: {elapsed_time:.2f} seconds")
        logger.info("=" * 70 + "\n")

        return results

    def validate_extracted_data(self, filepath: Path) -> bool:
        """
        Validate that extracted data is usable.

        Args:
            filepath: Path to CSV file to validate

        Returns:
            True if data is valid, False otherwise
        """
        try:
            # Try to load and check basic properties
            df = pd.read_csv(filepath, nrows=100)

            if df.empty:
                logger.error(f"File {filepath.name} is empty")
                return False

            logger.info(f"✓ Validation passed for {filepath.name}")
            logger.info(f"  Columns: {len(df.columns)}")
            logger.info(f"  Sample rows loaded: {len(df)}")

            return True

        except Exception as e:
            logger.error(f"Validation failed for {filepath.name}: {e}")
            return False


def main():
    """Main execution function for testing."""
    import yaml

    # Load config
    with open("config/config.yaml", "r") as f:
        config = yaml.safe_load(f)

    # Extract data
    extractor = DataExtractor(config)
    results = extractor.extract_all()

    # Validate
    for name, path in results.items():
        if path:
            extractor.validate_extracted_data(path)


if __name__ == "__main__":
    main()
