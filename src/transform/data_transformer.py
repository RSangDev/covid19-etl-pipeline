"""
Data transformation module for COVID-19 ETL Pipeline.
Uses PySpark for scalable processing of large datasets.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from pathlib import Path
from typing import Dict, List, Optional
import logging

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class DataTransformer:
    """Transforms COVID-19 data using PySpark for scalability."""

    def __init__(self, config: Dict):
        """
        Initialize the data transformer.

        Args:
            config: Configuration dictionary
        """
        self.config = config
        self.processed_dir = Path(config["output"]["processed_dir"])
        self.processed_dir.mkdir(parents=True, exist_ok=True)

        # Initialize Spark Session
        self.spark = self._create_spark_session()

    def _create_spark_session(self) -> SparkSession:
        """
        Create and configure Spark session.

        Returns:
            Configured SparkSession
        """
        spark_config = self.config["spark"]

        logger.info("Initializing Spark Session...")

        spark = (
            SparkSession.builder.appName(spark_config["app_name"])
            .master(spark_config["master"])
            .config(
                "spark.sql.shuffle.partitions",
                spark_config["config"]["spark.sql.shuffle.partitions"],
            )
            .config(
                "spark.driver.memory", spark_config["config"]["spark.driver.memory"]
            )
            .config(
                "spark.executor.memory", spark_config["config"]["spark.executor.memory"]
            )
            .getOrCreate()
        )

        # Set log level
        spark.sparkContext.setLogLevel("WARN")

        logger.info(f"✓ Spark Session created: {spark_config['app_name']}")
        logger.info(f"  Master: {spark_config['master']}")
        logger.info(f"  Driver Memory: {spark_config['config']['spark.driver.memory']}")

        return spark

    def load_csv_to_spark(self, filepath: Path) -> Optional[DataFrame]:
        """
        Load CSV file into Spark DataFrame.

        Args:
            filepath: Path to CSV file

        Returns:
            Spark DataFrame or None if failed
        """
        try:
            logger.info(f"Loading {filepath.name} into Spark...")

            df = (
                self.spark.read.option("header", "true")
                .option("inferSchema", "true")
                .csv(str(filepath))
            )

            count = df.count()
            logger.info(f"✓ Loaded {count:,} rows, {len(df.columns)} columns")

            return df

        except Exception as e:
            logger.error(f"Failed to load {filepath.name}: {e}")
            return None

    def transform_covid_data(self, df: DataFrame) -> DataFrame:
        """
        Transform COVID-19 main dataset.

        Args:
            df: Raw Spark DataFrame

        Returns:
            Transformed DataFrame
        """
        logger.info("Transforming COVID-19 data...")

        # Convert date column
        df = df.withColumn("date", F.to_date(F.col("date")))

        # Cast numeric columns
        numeric_cols = [
            "total_cases",
            "new_cases",
            "total_deaths",
            "new_deaths",
            "total_cases_per_million",
            "new_cases_per_million",
            "total_deaths_per_million",
            "new_deaths_per_million",
            "reproduction_rate",
            "icu_patients",
            "hosp_patients",
            "positive_rate",
            "tests_per_case",
            "total_tests",
            "new_tests",
            "population",
            "population_density",
            "median_age",
            "aged_65_older",
            "aged_70_older",
            "gdp_per_capita",
            "cardiovasc_death_rate",
            "diabetes_prevalence",
            "life_expectancy",
        ]

        for col in numeric_cols:
            if col in df.columns:
                df = df.withColumn(col, F.col(col).cast(DoubleType()))

        # Add derived columns
        df = df.withColumn(
            "case_fatality_rate",
            F.when(
                F.col("total_cases") > 0,
                (F.col("total_deaths") / F.col("total_cases")) * 100,
            ).otherwise(None),
        )

        df = df.withColumn("year", F.year(F.col("date")))

        df = df.withColumn("month", F.month(F.col("date")))

        df = df.withColumn("week", F.weekofyear(F.col("date")))

        logger.info("✓ COVID-19 data transformation complete")

        return df

    def transform_vaccination_data(self, df: DataFrame) -> DataFrame:
        """
        Transform vaccination dataset.

        Args:
            df: Raw Spark DataFrame

        Returns:
            Transformed DataFrame
        """
        logger.info("Transforming vaccination data...")

        # Convert date column
        df = df.withColumn("date", F.to_date(F.col("date")))

        # Cast numeric columns
        numeric_cols = [
            "total_vaccinations",
            "people_vaccinated",
            "people_fully_vaccinated",
            "total_boosters",
            "daily_vaccinations",
            "daily_vaccinations_per_million",
            "total_vaccinations_per_hundred",
            "people_vaccinated_per_hundred",
            "people_fully_vaccinated_per_hundred",
        ]

        for col in numeric_cols:
            if col in df.columns:
                df = df.withColumn(col, F.col(col).cast(DoubleType()))

        # Add derived columns
        df = df.withColumn(
            "vaccination_rate",
            F.when(
                F.col("people_vaccinated").isNotNull(),
                (F.col("people_vaccinated") / F.col("total_vaccinations")) * 100,
            ).otherwise(None),
        )

        df = df.withColumn(
            "full_vaccination_rate",
            F.when(
                F.col("people_fully_vaccinated").isNotNull(),
                (F.col("people_fully_vaccinated") / F.col("people_vaccinated")) * 100,
            ).otherwise(None),
        )

        logger.info("✓ Vaccination data transformation complete")

        return df

    def aggregate_by_country(self, df: DataFrame) -> DataFrame:
        """
        Aggregate statistics by country.

        Args:
            df: Transformed DataFrame

        Returns:
            Aggregated DataFrame
        """
        logger.info("Aggregating data by country...")

        agg_df = df.groupBy("location", "iso_code").agg(
            F.max("date").alias("last_updated"),
            F.max("total_cases").alias("total_cases"),
            F.sum("new_cases").alias("cumulative_new_cases"),
            F.max("total_deaths").alias("total_deaths"),
            F.sum("new_deaths").alias("cumulative_new_deaths"),
            F.avg("case_fatality_rate").alias("avg_case_fatality_rate"),
            F.max("population").alias("population"),
            F.count("*").alias("data_points"),
        )

        # Calculate additional metrics
        agg_df = agg_df.withColumn(
            "cases_per_100k", (F.col("total_cases") / F.col("population")) * 100000
        )

        agg_df = agg_df.withColumn(
            "deaths_per_100k", (F.col("total_deaths") / F.col("population")) * 100000
        )

        # Sort by total cases
        agg_df = agg_df.orderBy(F.col("total_cases").desc())

        logger.info("✓ Country aggregation complete")

        return agg_df

    def aggregate_by_date(self, df: DataFrame) -> DataFrame:
        """
        Aggregate global statistics by date.

        Args:
            df: Transformed DataFrame

        Returns:
            Aggregated DataFrame
        """
        logger.info("Aggregating data by date (global)...")

        agg_df = df.groupBy("date").agg(
            F.sum("new_cases").alias("global_new_cases"),
            F.sum("new_deaths").alias("global_new_deaths"),
            F.sum("total_cases").alias("global_total_cases"),
            F.sum("total_deaths").alias("global_total_deaths"),
            F.count("location").alias("countries_reporting"),
        )

        # Calculate 7-day moving average
        from pyspark.sql.window import Window

        window_spec = Window.orderBy("date").rowsBetween(-6, 0)

        agg_df = agg_df.withColumn(
            "global_new_cases_7day_avg", F.avg("global_new_cases").over(window_spec)
        )

        agg_df = agg_df.withColumn(
            "global_new_deaths_7day_avg", F.avg("global_new_deaths").over(window_spec)
        )

        agg_df = agg_df.orderBy("date")

        logger.info("✓ Date aggregation complete")

        return agg_df

    def filter_countries_of_interest(
        self, df: DataFrame, countries: List[str]
    ) -> DataFrame:
        """
        Filter DataFrame to specific countries.

        Args:
            df: Spark DataFrame
            countries: List of country names

        Returns:
            Filtered DataFrame
        """
        logger.info(f"Filtering to {len(countries)} countries of interest...")

        filtered_df = df.filter(F.col("location").isin(countries))

        count = filtered_df.count()
        logger.info(f"✓ Filtered to {count:,} rows")

        return filtered_df

    def save_to_parquet(self, df: DataFrame, filename: str) -> Path:
        """
        Save DataFrame to Parquet format.

        Args:
            df: Spark DataFrame
            filename: Output filename (without extension)

        Returns:
            Path to saved file
        """
        output_path = self.processed_dir / filename

        logger.info(f"Saving to Parquet: {filename}...")

        # Remove existing directory if it exists
        import shutil

        if output_path.exists():
            shutil.rmtree(output_path)

        df.write.mode("overwrite").parquet(str(output_path))

        logger.info(f"✓ Saved to {output_path}")

        return output_path

    def transform_and_save_all(
        self, covid_data_path: Path, vaccination_data_path: Path
    ) -> Dict[str, Path]:
        """
        Execute complete transformation pipeline.

        Args:
            covid_data_path: Path to raw COVID data CSV
            vaccination_data_path: Path to raw vaccination data CSV

        Returns:
            Dictionary mapping dataset names to output paths
        """
        logger.info("\n" + "=" * 70)
        logger.info("STARTING DATA TRANSFORMATION PROCESS")
        logger.info("=" * 70 + "\n")

        results = {}

        try:
            # 1. Load COVID data
            covid_df = self.load_csv_to_spark(covid_data_path)
            if covid_df is None:
                raise Exception("Failed to load COVID data")

            # 2. Transform COVID data
            covid_df = self.transform_covid_data(covid_df)

            # 3. Save full COVID dataset
            results["covid_full"] = self.save_to_parquet(
                covid_df, "covid_data_transformed"
            )

            # 4. Aggregate by country
            country_agg = self.aggregate_by_country(covid_df)
            results["covid_by_country"] = self.save_to_parquet(
                country_agg, "covid_by_country"
            )

            # 5. Aggregate by date
            date_agg = self.aggregate_by_date(covid_df)
            results["covid_by_date"] = self.save_to_parquet(date_agg, "covid_by_date")

            # 6. Filter countries of interest
            countries_of_interest = self.config["processing"]["countries_of_interest"]
            filtered_df = self.filter_countries_of_interest(
                covid_df, countries_of_interest
            )
            results["covid_filtered"] = self.save_to_parquet(
                filtered_df, "covid_filtered_countries"
            )

            # 7. Load vaccination data
            vacc_df = self.load_csv_to_spark(vaccination_data_path)
            if vacc_df is not None:
                # 8. Transform vaccination data
                vacc_df = self.transform_vaccination_data(vacc_df)

                # 9. Save vaccination dataset
                results["vaccinations"] = self.save_to_parquet(
                    vacc_df, "vaccinations_transformed"
                )

            logger.info("\n" + "=" * 70)
            logger.info("TRANSFORMATION SUMMARY")
            logger.info("=" * 70)
            logger.info(f"✓ Processed {len(results)} datasets successfully")
            for name, path in results.items():
                logger.info(f"  → {name}: {path.name}")
            logger.info("=" * 70 + "\n")

            return results

        except Exception as e:
            logger.error(f"Transformation failed: {e}")
            raise

        finally:
            # Don't stop Spark here, might be reused
            pass

    def stop_spark(self):
        """Stop Spark session."""
        if self.spark:
            logger.info("Stopping Spark session...")
            self.spark.stop()
            logger.info("✓ Spark session stopped")

    def get_statistics(self, df: DataFrame) -> Dict:
        """
        Get basic statistics from DataFrame.

        Args:
            df: Spark DataFrame

        Returns:
            Dictionary with statistics
        """
        return {
            "row_count": df.count(),
            "column_count": len(df.columns),
            "columns": df.columns,
        }


def main():
    """Main execution function for testing."""
    import yaml

    # Load config
    with open("config/config.yaml", "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    # Get most recent raw data files
    raw_dir = Path(config["output"]["raw_dir"])
    covid_files = sorted(raw_dir.glob("owid_covid_data_*.csv"))
    vacc_files = sorted(raw_dir.glob("owid_vaccinations_*.csv"))

    if not covid_files:
        logger.error("No COVID data files found. Run extraction first.")
        return

    covid_data_path = covid_files[-1]  # Most recent
    vacc_data_path = vacc_files[-1] if vacc_files else None

    # Transform data
    transformer = DataTransformer(config)

    try:
        results = transformer.transform_and_save_all(covid_data_path, vacc_data_path)
        logger.info(f"✓ Transformation complete! Processed {len(results)} datasets")
    finally:
        transformer.stop_spark()


if __name__ == "__main__":
    main()
