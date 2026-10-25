
import logging
from pathlib import Path


logger = logging.getLogger(__name__)


def read_csv(spark, file_path, schema):
    # I check the path first so that a spelling mistake
    # doesn't turn into a confusing Spark error.
    file_path = Path(file_path)

    logger.info("Reading file: %s", file_path.name)

    if not file_path.is_file():
        # I stop here if the file doesn't exist because
        # Spark cannot extract a file that isn't available.
        raise FileNotFoundError(
            f"Could not find the CSV file: {file_path.resolve()}"
        )

    try:
        # I use the explicit schema to control the structure
        # of the DataFrame instead of asking Spark to infer it.
        df = (
            spark.read
            .option("header", True)
            .option("mode", "PERMISSIVE")
            .schema(schema)
            .csv(str(file_path))
        )

        # Spark reads lazily, so this means the DataFrame
        # has been defined, not that every row has been checked.
        logger.info("DataFrame created for %s", file_path.name)

        return df

    except Exception:
        # I log the failure and raise it again so that the
        # calling code knows extraction did not succeed.
        logger.exception("Failed to read %s", file_path.name)
        raise


def extract_datasets(spark, data_dir, schemas):
    # I keep the file names here so that the same function
    # can extract all the datasets using one common process.
    file_names = {
        "stations": "stations.csv",
        "bikes": "bikes.csv",
        "users": "users.csv",
        "trips": "trips.csv",
        "maintenance": "maintenance.csv"
    }

    extracted_data = {}

    logger.info("Starting extraction of all datasets")

    for dataset_name, file_name in file_names.items():
        # I build the file path and find its matching schema.
        file_path = Path(data_dir) / file_name

        if dataset_name not in schemas:
            raise KeyError(
                f"No explicit schema found for dataset: {dataset_name}"
            )

        schema = schemas[dataset_name]

        # I reuse the same function instead of writing
        # separate CSV-reading code for every dataset.
        df = read_csv(spark, file_path, schema)

        extracted_data[dataset_name] = df

        logger.info("Extraction prepared for %s", dataset_name)

    logger.info("All dataset DataFrames have been created")

    return extracted_data
