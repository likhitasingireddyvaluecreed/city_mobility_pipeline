
import json
import logging
from pathlib import Path

from pyspark.sql import functions as F


logger = logging.getLogger(__name__)


# I list the columns that should uniquely identify records.
# If the same key appears more than once, I want to investigate it.
KEY_COLUMNS = {
    "stations": "station_id",
    "bikes": "bike_id",
    "users": "user_id",
    "trips": "trip_id",
    "maintenance": "maintenance_id"
}


# I list columns where different spellings or letter cases
# might represent the same category.
CATEGORY_COLUMNS = {
    "bikes": ["bike_type", "status"],
    "users": ["membership", "home_district"],
    "trips": ["trip_status"],
    "maintenance": ["issue_type", "resolution"],
    "stations": ["district"]
}


# I list numeric fields that need range and conversion checks.
NUMERIC_COLUMNS = {
    "stations": ["capacity"],
    "bikes": ["purchase_year"],
    "trips": ["duration_minutes"],
    "maintenance": ["repair_minutes"]
}


# These are initial business-rule limits for this dataset.
NUMERIC_RULES = {
    "stations": {
        "capacity": {"min": 0}
    },
    "bikes": {
        "purchase_year": {"min": 2000, "max": 2100}
    },
    "trips": {
        "duration_minutes": {"min": 0, "max": 1440}
    },
    "maintenance": {
        "repair_minutes": {"min": 0, "max": 10080}
    }
}


# I define which columns should refer to records in another dataset.
# A reference that does not exist in the parent dataset needs investigation.
REFERENCE_RULES = [
    ("trips", "user_id", "users", "user_id"),
    ("trips", "bike_id", "bikes", "bike_id"),
    ("trips", "start_station_id", "stations", "station_id"),
    ("trips", "end_station_id", "stations", "station_id"),
    ("maintenance", "bike_id", "bikes", "bike_id")
]


def get_count(df):
    # I use one small helper whenever I need the number of rows.
    return df.count()


def count_missing_values(df, column_name):
    # I treat nulls, empty strings, whitespace and common placeholders
    # as missing so they are not overlooked during profiling.
    value = F.trim(F.col(column_name).cast("string"))

    missing_condition = (
        F.col(column_name).isNull()
        | (value == "")
        | (F.lower(value).isin(
            "null", "none", "na", "n/a", "nan", "unknown"
        ))
    )

    return df.filter(missing_condition).count()


def count_duplicate_rows(df):
    # I check full-row duplicates without assuming that every
    # repeated value in an individual column is an error.
    if not df.columns:
        return 0

    duplicate_groups = (
        df.groupBy(*df.columns)
        .count()
        .filter(F.col("count") > 1)
    )

    # This returns the number of extra copies, not the number
    # of rows that belong to duplicate groups.
    result = duplicate_groups.agg(
        F.sum(F.col("count") - 1).alias("duplicate_count")
    ).first()

    return int(result["duplicate_count"] or 0)


def count_duplicate_keys(df, key_column):
    # I check repeated identifiers only when a key is configured.
    # A missing key is handled separately by missing-value profiling.
    if key_column not in df.columns:
        raise ValueError(f"Key column not found: {key_column}")

    duplicate_keys = (
        df.filter(F.col(key_column).isNotNull())
        .groupBy(key_column)
        .count()
        .filter(F.col("count") > 1)
    )

    return duplicate_keys.count()


def profile_categories(df, dataset_name, findings):
    # I inspect category frequencies so that unusual values
    # and inconsistent spellings become visible.
    category_summary = {}

    for column_name in CATEGORY_COLUMNS.get(dataset_name, []):
        if column_name not in df.columns:
            continue

        value = F.trim(F.col(column_name).cast("string"))

        counts = (
            df.groupBy(value.alias("value"))
            .count()
            .orderBy(F.desc("count"))
        )

        category_summary[column_name] = [
            {
                "value": row["value"],
                "count": row["count"]
            }
            for row in counts.limit(100).collect()
        ]

        # I compare trimmed, lowercase versions of categories.
        # Multiple raw values with the same normalized value may
        # indicate inconsistent capitalization or whitespace.
        normalized_values = (
            df.filter(F.col(column_name).isNotNull())
            .select(
                F.col(column_name).cast("string").alias("raw_value"),
                F.lower(F.trim(F.col(column_name).cast("string"))).alias(
                    "normalized_value"
                )
            )
            .filter(F.col("normalized_value") != "")
            .distinct()
        )

        inconsistent_groups = (
            normalized_values
            .groupBy("normalized_value")
            .agg(F.countDistinct("raw_value").alias("raw_variants"))
            .filter(F.col("raw_variants") > 1)
        )

        variant_count = inconsistent_groups.count()

        if variant_count:
            findings.append({
                "dataset": dataset_name,
                "check": "inconsistent_category_format",
                "column": column_name,
                "count": variant_count,
                "details": (
                    "Different raw values share the same "
                    "trimmed, lowercase representation."
                )
            })

    return category_summary


def profile_numeric_columns(df, dataset_name, findings):
    # I safely attempt numeric conversion so malformed text
    # can be reported instead of crashing the profiling step.
    numeric_summary = {}

    for column_name in NUMERIC_COLUMNS.get(dataset_name, []):
        if column_name not in df.columns:
            continue

        raw_value = F.trim(F.col(column_name).cast("string"))

        parsed_value = F.expr(
            f"try_cast(`{column_name}` AS DOUBLE)"
        )

        numeric_df = df.select(
            raw_value.alias("raw_value"),
            parsed_value.alias("numeric_value")
        )

        non_missing = (
            F.col("raw_value").isNotNull()
            & (F.col("raw_value") != "")
        )

        malformed_count = numeric_df.filter(
            non_missing & F.col("numeric_value").isNull()
        ).count()

        missing_count = count_missing_values(df, column_name)

        stats = numeric_df.agg(
            F.min("numeric_value").alias("minimum"),
            F.max("numeric_value").alias("maximum"),
            F.avg("numeric_value").alias("average")
        ).first()

        numeric_summary[column_name] = {
            "missing_count": missing_count,
            "malformed_count": malformed_count,
            "minimum": stats["minimum"],
            "maximum": stats["maximum"],
            "average": stats["average"]
        }

        if malformed_count:
            findings.append({
                "dataset": dataset_name,
                "check": "malformed_numeric_value",
                "column": column_name,
                "count": malformed_count,
                "details": "A non-empty value could not be converted to a number."
            })

        rules = NUMERIC_RULES.get(dataset_name, {}).get(column_name, {})
        invalid_condition = F.lit(False)

        if "min" in rules:
            invalid_condition = (
                invalid_condition
                | (F.col("numeric_value") < rules["min"])
            )

        if "max" in rules:
            invalid_condition = (
                invalid_condition
                | (F.col("numeric_value") > rules["max"])
            )

        invalid_count = numeric_df.filter(
            F.col("numeric_value").isNotNull() & invalid_condition
        ).count()

        if invalid_count:
            findings.append({
                "dataset": dataset_name,
                "check": "numeric_value_out_of_range",
                "column": column_name,
                "count": invalid_count,
                "details": f"Configured rule: {rules}"
            })

    return numeric_summary


def profile_trip_timestamps(df, findings):
    # I check trip timestamps separately because a timestamp may
    # be missing, malformed or inconsistent with the trip duration.
    timestamp_summary = {}

    for column_name in ["start_time", "end_time"]:
        if column_name not in df.columns:
            continue

        raw_value = F.trim(F.col(column_name).cast("string"))

        parsed_value = F.to_timestamp(
            F.col(column_name).cast("string")
        )

        timestamp_df = df.select(
            raw_value.alias("raw_value"),
            parsed_value.alias("parsed_value")
        )

        missing_count = count_missing_values(df, column_name)

        malformed_count = timestamp_df.filter(
            F.col("raw_value").isNotNull()
            & (F.col("raw_value") != "")
            & F.col("parsed_value").isNull()
        ).count()

        timestamp_summary[column_name] = {
            "missing_count": missing_count,
            "malformed_count": malformed_count
        }

        if malformed_count:
            findings.append({
                "dataset": "trips",
                "check": "malformed_timestamp",
                "column": column_name,
                "count": malformed_count,
                "details": "A non-empty timestamp could not be parsed."
            })

    if not all(
        column in df.columns
        for column in ["start_time", "end_time", "duration_minutes"]
    ):
        return timestamp_summary

    # I compare the duration recorded in the file with the duration
    # calculated from the two timestamps, where both timestamps parse.
    checked_df = (
        df.withColumn(
            "_start_ts",
            F.to_timestamp(F.col("start_time").cast("string"))
        )
        .withColumn(
            "_end_ts",
            F.to_timestamp(F.col("end_time").cast("string"))
        )
        .withColumn(
            "_recorded_duration",
            F.expr("try_cast(`duration_minutes` AS DOUBLE)")
        )
        .withColumn(
            "_calculated_duration",
            (
                F.col("_end_ts").cast("long")
                - F.col("_start_ts").cast("long")
            ) / 60.0
        )
    )

    negative_time_count = checked_df.filter(
        F.col("_start_ts").isNotNull()
        & F.col("_end_ts").isNotNull()
        & (F.col("_end_ts") < F.col("_start_ts"))
    ).count()

    if negative_time_count:
        findings.append({
            "dataset": "trips",
            "check": "end_before_start",
            "column": "start_time,end_time",
            "count": negative_time_count,
            "details": "The parsed end timestamp occurs before the start timestamp."
        })

    # A one-minute tolerance allows small rounding differences.
    mismatch_count = checked_df.filter(
        F.col("_start_ts").isNotNull()
        & F.col("_end_ts").isNotNull()
        & F.col("_recorded_duration").isNotNull()
        & (
            F.abs(
                F.col("_recorded_duration")
                - F.col("_calculated_duration")
            ) > 1
        )
    ).count()

    if mismatch_count:
        findings.append({
            "dataset": "trips",
            "check": "duration_timestamp_mismatch",
            "column": "duration_minutes",
            "count": mismatch_count,
            "details": (
                "Recorded duration differs from timestamp-derived duration "
                "by more than one minute."
            )
        })

    timestamp_summary["duration_timestamp_comparison"] = {
        "mismatch_count": mismatch_count,
        "end_before_start_count": negative_time_count,
        "comparison_tolerance_minutes": 1
    }

    return timestamp_summary


def profile_dataset(dataset_name, df, findings):
    # I collect the main quality indicators for one dataset.
    logger.info("Profiling dataset: %s", dataset_name)

    row_count = get_count(df)

    missing_summary = {}

    for column_name in df.columns:
        missing_count = count_missing_values(df, column_name)

        missing_summary[column_name] = missing_count

        if missing_count:
            findings.append({
                "dataset": dataset_name,
                "check": "missing_values",
                "column": column_name,
                "count": missing_count,
                "details": "Null, blank or recognized placeholder values."
            })

    duplicate_row_count = count_duplicate_rows(df)

    if duplicate_row_count:
        findings.append({
            "dataset": dataset_name,
            "check": "duplicate_rows",
            "column": None,
            "count": duplicate_row_count,
            "details": "Number of extra copies of identical full rows."
        })

    key_column = KEY_COLUMNS.get(dataset_name)
    duplicate_key_count = 0

    if key_column and key_column in df.columns:
        duplicate_key_count = count_duplicate_keys(df, key_column)

        if duplicate_key_count:
            findings.append({
                "dataset": dataset_name,
                "check": "duplicate_key",
                "column": key_column,
                "count": duplicate_key_count,
                "details": "Number of key values that occur more than once."
            })

    category_summary = profile_categories(
        df, dataset_name, findings
    )

    numeric_summary = profile_numeric_columns(
        df, dataset_name, findings
    )

    timestamp_summary = {}

    if dataset_name == "trips":
        timestamp_summary = profile_trip_timestamps(df, findings)

    logger.info(
        "Finished profiling %s: %s rows",
        dataset_name,
        row_count
    )

    return {
        "row_count": row_count,
        "column_count": len(df.columns),
        "columns": df.columns,
        "schema": df.schema.simpleString(),
        "missing_values": missing_summary,
        "duplicate_extra_rows": duplicate_row_count,
        "duplicate_key_values": duplicate_key_count,
        "key_column": key_column,
        "categories": category_summary,
        "numeric_columns": numeric_summary,
        "timestamps": timestamp_summary
    }


def profile_references(datasets, findings):
    # I check whether IDs in child datasets exist in their parent datasets.
    # I do not remove unmatched records; I only report them.
    reference_summary = []

    for (
        child_name,
        child_column,
        parent_name,
        parent_column
    ) in REFERENCE_RULES:

        if child_name not in datasets or parent_name not in datasets:
            continue

        child_df = datasets[child_name]
        parent_df = datasets[parent_name]

        if child_column not in child_df.columns:
            continue

        if parent_column not in parent_df.columns:
            continue

        child_ids = (
            child_df
            .filter(F.col(child_column).isNotNull())
            .select(F.col(child_column).alias("_reference_id"))
        )

        parent_ids = (
            parent_df
            .filter(F.col(parent_column).isNotNull())
            .select(F.col(parent_column).alias("_reference_id"))
            .distinct()
        )

        unmatched_count = (
            child_ids.join(
                parent_ids,
                on="_reference_id",
                how="left_anti"
            )
            .count()
        )

        reference_summary.append({
            "child_dataset": child_name,
            "child_column": child_column,
            "parent_dataset": parent_name,
            "parent_column": parent_column,
            "unmatched_count": unmatched_count
        })

        if unmatched_count:
            findings.append({
                "dataset": child_name,
                "check": "unmatched_reference",
                "column": child_column,
                "count": unmatched_count,
                "details": (
                    f"Some values do not exist in "
                    f"{parent_name}.{parent_column}."
                )
            })

    return reference_summary


def run_profiling(datasets, output_dir="outputs/profiling"):
    # I profile every extracted DataFrame and then check relationships
    # between the datasets.
    logger.info("Starting data profiling")

    findings = []
    dataset_summary = {}

    for dataset_name, df in datasets.items():
        dataset_summary[dataset_name] = profile_dataset(
            dataset_name,
            df,
            findings
        )

    reference_summary = profile_references(datasets, findings)

    # I keep the reports outside the raw input folder so the original
    # CSV files are not changed by profiling.
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    report = {
        "datasets": dataset_summary,
        "references": reference_summary,
        "findings_count": len(findings),
        "findings": findings
    }

    report_path = output_path / "quality_report.json"

    with report_path.open("w", encoding="utf-8") as report_file:
        json.dump(report, report_file, indent=2, default=str)

    # I also write a compact findings file so it is easy to review
    # the issues without opening the full quality report.
    findings_path = output_path / "findings.json"

    with findings_path.open("w", encoding="utf-8") as findings_file:
        json.dump(findings, findings_file, indent=2, default=str)

    logger.info("Profiling completed")
    logger.info("Quality report saved to: %s", report_path.resolve())
    logger.info("Findings report saved to: %s", findings_path.resolve())
    logger.info("Total finding entries: %s", len(findings))

    return report
