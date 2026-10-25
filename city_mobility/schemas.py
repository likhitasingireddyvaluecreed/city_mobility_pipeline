
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    DoubleType,
    DateType,
    TimestampType,
    BooleanType
)

# I keep all the dataset schemas in one place.
# This helps me maintain the expected structure without
# mixing schema definitions with extraction logic.


# I read station details such as station name, district
# and capacity without converting raw values too early.
stations_schema = StructType([
    StructField("station_id", StringType(), True),
    StructField("station_name", StringType(), True),
    StructField("district", StringType(), True),
    StructField("capacity", StringType(), True)
])


# I keep bike details as raw values so that unexpected
# purchase years or statuses can be checked later.
bikes_schema = StructType([
    StructField("bike_id", StringType(), True),
    StructField("bike_type", StringType(), True),
    StructField("purchase_year", StringType(), True),
    StructField("status", StringType(), True)
])


# I keep signup_date as text during extraction because
# I want to check date formats before converting them.
users_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("membership", StringType(), True),
    StructField("signup_date", StringType(), True),
    StructField("home_district", StringType(), True)
])


# I keep trip timestamps and duration as text for now.
# This lets me investigate invalid timestamps and
# negative durations before converting their data types.
trips_schema = StructType([
    StructField("trip_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("bike_id", StringType(), True),
    StructField("start_station_id", StringType(), True),
    StructField("end_station_id", StringType(), True),
    StructField("start_time", StringType(), True),
    StructField("end_time", StringType(), True),
    StructField("duration_minutes", StringType(), True),
    StructField("trip_status", StringType(), True)
])


# I keep maintenance values as text so that invalid
# repair durations or timestamps are not converted early.
maintenance_schema = StructType([
    StructField("maintenance_id", StringType(), True),
    StructField("bike_id", StringType(), True),
    StructField("reported_at", StringType(), True),
    StructField("issue_type", StringType(), True),
    StructField("repair_minutes", StringType(), True),
    StructField("resolution", StringType(), True)
])


# I map each dataset name to its matching schema.
# The extraction function uses this mapping to choose
# the correct schema for each CSV file.
SCHEMAS = {
    "stations": stations_schema,
    "bikes": bikes_schema,
    "users": users_schema,
    "trips": trips_schema,
    "maintenance": maintenance_schema
}