
from pathlib import Path
import logging
from pyspark.sql import SparkSession

from city_mobility.schemas import SCHEMAS
from city_mobility.extract import extract_datasets

# I keep the log format simple so I can quickly understand
# which step the pipeline is currently performing.
logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s: %(message)s"
)

logger = logging.getLogger(__name__)

def main():
    # I find the project directory using this file's location.
    # This way, my paths don't depend on which folder I opened
    # in the terminal before running the program.
    project_dir = Path(__file__).resolve().parent

    # My original CSV files are stored in the data/raw folder.
    data_dir = project_dir / "data" / "raw"

    # I create one Spark session for this pipeline.
    # The same session will be used to read all five datasets.
    spark = (
        SparkSession.builder
        .appName("CityMobilityPipeline")
        .getOrCreate()
    )

    try:
        logger.info("Starting City Mobility pipeline")

        # I pass the folder and schemas to my reusable extraction
        # function instead of writing five separate read operations.
        datasets = extract_datasets(
            spark=spark,
            data_dir=data_dir,
            schemas=SCHEMAS
        )

        # I inspect the extracted DataFrames to make sure that
        # Spark has read the expected columns and data types.
        for dataset_name, df in datasets.items():
            logger.info("Previewing dataset: %s", dataset_name)

            # I check the structure first.
            df.printSchema()

            # I look at a few rows to understand what was read.
            df.show(5, truncate=False)

        # I now have all five DataFrames ready for the next stage.
        # I haven't cleaned or removed any records here.
        logger.info("Extraction stage completed successfully")

    except Exception:
        # I record the error before allowing the failure
        # to propagate, so the run doesn't look successful.
        logger.exception("City Mobility pipeline failed")
        raise

    finally:
        # I stop Spark after the extraction script finishes
        spark.stop()


if __name__ == "__main__":
    main()
