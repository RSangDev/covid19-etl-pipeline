"""
COVID-19 ETL Pipeline - Main Orchestrator
Executes the complete ETL process: Extract -> Transform -> Load
"""

import yaml
import logging
import sys
from pathlib import Path
from datetime import datetime
import time

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from extract.data_extractor import DataExtractor # noqa
from transform.data_transformer import DataTransformer # noqa
from load.data_loader import DataLoader # noqa

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("logs/pipeline.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


class COVID19Pipeline:
    """Orchestrates the complete COVID-19 ETL pipeline."""

    def __init__(self, config_path: str = "config/config.yaml"):
        """
        Initialize the pipeline.

        Args:
            config_path: Path to configuration file
        """
        self.config = self._load_config(config_path)
        self.extractor = DataExtractor(self.config)
        self.transformer = DataTransformer(self.config)
        self.loader = DataLoader(self.config)

    def _load_config(self, config_path: str) -> dict:
        """Load configuration from YAML file."""
        logger.info(f"Loading configuration from {config_path}")

        with open(config_path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)

        logger.info("✓ Configuration loaded successfully")
        return config

    def run_extract(self):
        """Execute extraction phase."""
        logger.info("\n" + "=" * 80)
        logger.info("PHASE 1: DATA EXTRACTION")
        logger.info("=" * 80)

        start_time = time.time()

        # Extract data
        results = self.extractor.extract_all()

        # Validate
        valid_data = {}
        for name, path in results.items():
            if path and self.extractor.validate_extracted_data(path):
                valid_data[name] = path

        elapsed = time.time() - start_time

        logger.info(f"\n✓ Extraction complete in {elapsed:.2f} seconds")
        logger.info(f"  Valid datasets: {len(valid_data)}/{len(results)}")

        return valid_data

    def run_transform(self, extracted_data: dict):
        """
        Execute transformation phase.

        Args:
            extracted_data: Dictionary of extracted file paths
        """
        logger.info("\n" + "=" * 80)
        logger.info("PHASE 2: DATA TRANSFORMATION (PySpark)")
        logger.info("=" * 80)

        start_time = time.time()

        covid_data = extracted_data.get("covid_data")
        vacc_data = extracted_data.get("vaccination_data")

        if not covid_data:
            raise Exception("COVID data not found in extracted data")

        # Transform data
        transformed_data = self.transformer.transform_and_save_all(
            covid_data, vacc_data
        )

        elapsed = time.time() - start_time

        logger.info(f"\n✓ Transformation complete in {elapsed:.2f} seconds")
        logger.info(f"  Processed datasets: {len(transformed_data)}")

        return transformed_data

    def run_load(self, transformed_data: dict):
        """
        Execute loading phase.

        Args:
            transformed_data: Dictionary of transformed file paths
        """
        logger.info("\n" + "=" * 80)
        logger.info("PHASE 3: DATA LOADING (SQLite)")
        logger.info("=" * 80)

        start_time = time.time()

        # Load data into database
        self.loader.load_all(transformed_data)

        elapsed = time.time() - start_time

        logger.info(f"\n✓ Loading complete in {elapsed:.2f} seconds")

    def run_full_pipeline(self):
        """Execute the complete ETL pipeline."""
        logger.info("\n" + "=" * 80)
        logger.info("COVID-19 ETL PIPELINE - FULL EXECUTION")
        logger.info(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("=" * 80)

        pipeline_start = time.time()

        try:
            # Phase 1: Extract
            extracted_data = self.run_extract()

            # Phase 2: Transform
            transformed_data = self.run_transform(extracted_data)

            # Phase 3: Load
            self.run_load(transformed_data)

            # Cleanup
            self.transformer.stop_spark()
            self.loader.close()

            pipeline_elapsed = time.time() - pipeline_start

            logger.info("\n" + "=" * 80)
            logger.info("PIPELINE EXECUTION SUMMARY")
            logger.info("=" * 80)
            logger.info("✓ ALL PHASES COMPLETED SUCCESSFULLY")
            logger.info(f"Total execution time: {pipeline_elapsed:.2f} seconds")
            logger.info(f"Finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            logger.info("=" * 80 + "\n")

            return True

        except Exception as e:
            logger.error(f"\n❌ Pipeline failed: {e}")
            logger.exception("Full traceback:")

            # Cleanup
            try:
                self.transformer.stop_spark()
                self.loader.close()
            except Exception:
                logger.warning("Error during pipeline cleanup")

            return False


def main():
    """Main entry point."""
    # Create logs directory
    Path("logs").mkdir(exist_ok=True)

    logger.info("=" * 80)
    logger.info("COVID-19 ETL PIPELINE")
    logger.info("Scalable data processing with PySpark")
    logger.info("=" * 80 + "\n")

    # Run pipeline
    pipeline = COVID19Pipeline()
    success = pipeline.run_full_pipeline()

    # Exit with appropriate code
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
