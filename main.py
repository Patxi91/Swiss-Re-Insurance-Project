from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, DecimalType, DateType, TimestampType, IntegerType
from decimal import Decimal
from pyspark.sql.functions import col, when, regexp_replace, current_timestamp, lit, date_format, udf, to_date, to_timestamp
from datetime import datetime, date
import requests

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Contracts and Claims DataFrames") \
    .getOrCreate()

# Define the schemas
contract_schema = StructType([
    StructField("SOURCE_SYSTEM", StringType(), True),  # PK
    StructField("CONTRACT_ID", LongType(), True),  # PK
    StructField("CONTRACT_TYPE", StringType(), True),
    StructField("INSURED_PERIOD_FROM", DateType(), True),
    StructField("INSURED_PERIOD_TO", DateType(), True),
    StructField("CREATION_DATE", TimestampType(), True)
])

claims_schema = StructType([
    StructField("SOURCE_SYSTEM", StringType(), True),  # PK
    StructField("CLAIM_ID", StringType(), True),  # PK
    StructField("CONTRACT_SOURCE_SYSTEM", StringType(), True),  # PK
    StructField("CONTRACT_ID", LongType(), True),  # PK
    StructField("CLAIM_TYPE", StringType(), True),
    StructField("DATE_OF_LOSS", DateType(), True),
    StructField("AMOUNT", DecimalType(16, 5), True),
    StructField("CREATION_DATE", TimestampType(), True)
])

# Helper functions
def to_date_obj(date_str):
    """Convert string to date object."""
    return datetime.strptime(date_str, "%d.%m.%Y").date()


def to_timestamp_obj(ts_str):
    """Convert string to timestamp object."""
    return datetime.strptime(ts_str, "%d.%m.%Y %H:%M")


def to_timestamp_from_iso(ts_str):
    """Convert ISO string to timestamp object."""
    return datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")


def fetch_digest(claim_id):
    """Fetch digest via API."""
    url = f"https://api.hashify.net/hash/md4/hex?value={claim_id}"
    try:
        response = requests.get(url)  # API call
        response.raise_for_status()
        data = response.json()
        return data.get("Digest", "")  # Return the "Digest" field
    except requests.exceptions.RequestException as e:
        print(f"Error fetching NSE_ID for CLAIM_ID {claim_id}: {e}")
        return None  # Return None if there's an error


fetch_digest_udf = udf(fetch_digest, StringType())  # Register the function as a PySpark UDF allowing the API function to be applied across the DataFrame rows

# Contract data
contract_data = [
    ("Contract_SR_Europa_3", 408124123, "Direct", to_date_obj("01.01.2015"), to_date_obj("01.01.2099"), to_timestamp_obj("17.01.2022 13:42")),
    ("Contract_SR_Europa_3", 46784575, "Direct", to_date_obj("01.01.2015"), to_date_obj("01.01.2099"), to_timestamp_obj("17.01.2022 13:42")),
    ("Contract_SR_Europa_3", 97563756, "", to_date_obj("01.01.2015"), to_date_obj("01.01.2099"), to_timestamp_obj("17.01.2022 13:42")),
    ("Contract_SR_Europa_3", 13767503, "Reinsurance", to_date_obj("01.01.2015"), to_date_obj("01.01.2099"), to_timestamp_obj("17.01.2022 13:42")),
    ("Contract_SR_Europa_3", 656948536, "", to_date_obj("01.01.2015"), to_date_obj("01.01.2099"), to_timestamp_obj("17.01.2022 13:42"))
]

# Claims data
claims_data = [
    ("Claim_SR_Europa_3", "CL_68545123", "Contract_SR_Europa_3", 97563756, "2", to_date_obj("14.02.2021"), Decimal("523.21"), to_timestamp_from_iso("2022-01-17 14:45:00")),
    ("Claim_SR_Europa_3", "CL_962234", "Contract_SR_Europa_4", 408124123, "1", to_date_obj("30.01.2021"), Decimal("52369.0"), to_timestamp_from_iso("2022-01-17 14:46:00")),
    ("Claim_SR_Europa_3", "CL_895168", "Contract_SR_Europa_3", 13767503, "", to_date_obj("02.09.2020"), Decimal("98465"), to_timestamp_from_iso("2022-01-17 14:45:00")),
    ("Claim_SR_Europa_3", "CX_12066501", "Contract_SR_Europa_3", 656948536, "2", to_date_obj("04.01.2022"), Decimal("9000"), to_timestamp_from_iso("2022-01-17 14:45:00")),
    ("Claim_SR_Europa_3", "RX_9845163", "Contract_SR_Europa_3", 656948536, "2", to_date_obj("04.06.2015"), Decimal("11000"), to_timestamp_from_iso("2022-01-17 14:45:00")),
    ("Claim_SR_Europa_3", "CL_39904634", "Contract_SR_Europa_3", 656948536, "2", to_date_obj("04.11.2020"), Decimal("11000"), to_timestamp_from_iso("2022-01-17 14:46:00")),
    ("Claim_SR_Europa_3", "U_7065313", "Contract_SR_Europa_3", 46589516, "1", to_date_obj("29.09.2021"), Decimal("11000"), to_timestamp_from_iso("2022-01-17 14:46:00"))
]

# Create DataFrames
contract_df = spark.createDataFrame(data=contract_data, schema=contract_schema)
claims_df = spark.createDataFrame(data=claims_data, schema=claims_schema)

# Show schemas and contents
contract_df.printSchema()
contract_df.show()
claims_df.printSchema()
claims_df.show()

# Testing Input DataFrames Function
def test_contracts_and_claims():
    """Test basic assertions for contracts and claims."""
    # Test basic assertions for contracts
    assert contract_df.count() == 5  # Total contracts
    assert contract_df.filter(col("CONTRACT_TYPE") == "").count() == 2  # Contracts with empty CONTRACT_TYPE
    assert isinstance(contract_df.schema["CONTRACT_ID"].dataType, LongType)  # CONTRACT_ID should be LongType

    # Test basic assertions for claims
    assert claims_df.count() == 7  # Total claims
    assert claims_df.filter(col("CLAIM_TYPE") == "").count() == 1  # Claims with empty CLAIM_TYPE
    assert isinstance(claims_df.schema["CONTRACT_ID"].dataType, LongType)  # CONTRACT_ID should be LongType

    # Test join logic between contracts and claims
    joined = contract_df.join(claims_df, contract_df["CONTRACT_ID"] == claims_df["CONTRACT_ID"])
    assert joined.count() == 6  # One claim does not have a matching contract


# Additional tests for PK constraints since Spark does not have native, inbuilt primary key (PK) constraint enforcement as relational DBs do.
def test_primary_keys():
    """Test primary key uniqueness for contracts and claims."""
    # CONTRACT primary key: Ensure uniqueness of SOURCE_SYSTEM + CONTRACT_ID
    assert contract_df.select("SOURCE_SYSTEM", "CONTRACT_ID").distinct().count() == contract_df.count(), \
        "CONTRACT PK constraint violated: SOURCE_SYSTEM + CONTRACT_ID is not unique"

    # CLAIM primary key: Ensure uniqueness of SOURCE_SYSTEM, CLAIM_ID, CONTRACT_SOURCE_SYSTEM, CONTRACT_ID
    assert claims_df.select("SOURCE_SYSTEM", "CLAIM_ID", "CONTRACT_SOURCE_SYSTEM", "CONTRACT_ID").distinct().count() == claims_df.count(), \
        "CLAIM PK constraint violated: SOURCE_SYSTEM, CLAIM_ID, CONTRACT_SOURCE_SYSTEM, CONTRACT_ID is not unique"

if __name__ == "__main__":
    
    # Test Input Data
    test_contracts_and_claims()
    test_primary_keys()
    print("All Input Data tests passed!")

    # Step 1: Join contracts and claims
    joined_df = claims_df.alias("cl").join(
        contract_df.alias("co"),
        (col("cl.CONTRACT_ID") == col("co.CONTRACT_ID")) &
        (col("cl.CONTRACT_SOURCE_SYSTEM") == col("co.SOURCE_SYSTEM")),
        how="inner"
    )
    joined_df.show()

    # Step 2: Build the TRANSACTIONS DataFrame
    transactions_df = joined_df.select(
        # CONTRACT_SOURCE_SYSTEM: Default value
        lit("Europe 3").alias("CONTRACT_SOURCE_SYSTEM"),

        # CONTRACT_SOURCE_SYSTEM_ID: from contracts
        contract_df["CONTRACT_ID"].alias("CONTRACT_SOURCE_SYSTEM_ID"),

        # SOURCE_SYSTEM_ID: CLAIM_ID without prefix
        regexp_replace(claims_df["CLAIM_ID"], "^[A-Z_]+", "").cast(IntegerType()).alias("SOURCE_SYSTEM_ID"),

        # TRANSACTION_TYPE logic
        when(claims_df["CLAIM_TYPE"] == "2", "Corporate")
        .when(claims_df["CLAIM_TYPE"] == "1", "Private")
        .otherwise("Unknown").alias("TRANSACTION_TYPE"),  # assume value is always either 2, 1 or empty

        # TRANSACTION_DIRECTION logic
        when(claims_df["CLAIM_ID"].like("CL%"), "COINSURANCE")
        .when(claims_df["CLAIM_ID"].like("RX%"), "REINSURANCE")
        .otherwise("UNKNOWN").alias("TRANSACTION_DIRECTION"),  # Unknown for other values not specified (i.e.: CX)

        # CONFORMED_VALUE: just amount
        claims_df["AMOUNT"].alias("CONFORMED_VALUE"),

        # BUSINESS_DATE: format as yyyy-MM-dd
        to_date(date_format(claims_df["DATE_OF_LOSS"], "yyyy-MM-dd")).alias("BUSINESS_DATE"),

        # CREATION_DATE: format as yyyy-MM-dd HH:mm:ss
        to_timestamp(date_format(claims_df["CREATION_DATE"], "yyyy-MM-dd HH:mm:ss")).alias("CREATION_DATE"),

        # SYSTEM_TIMESTAMP: current timestamp
        to_timestamp(current_timestamp()).alias("SYSTEM_TIMESTAMP"),

        # NSE_ID: Fetch unique ID via REST API for each CLAIM_ID without prefix
        fetch_digest_udf(regexp_replace(col("cl.CLAIM_ID"), "^[A-Z_]+", "")).alias("NSE_ID")
    )
    transactions_df.show()
    
    # Testing Output Dataframe Schema
    def test_transactions_df_schema():
        # Step 1: Check if the schema matches the expected schema
        expected_schema = {
            "CONTRACT_SOURCE_SYSTEM": StringType(),
            "CONTRACT_SOURCE_SYSTEM_ID": LongType(),
            "SOURCE_SYSTEM_ID": IntegerType(),
            "TRANSACTION_TYPE": StringType(),
            "TRANSACTION_DIRECTION": StringType(),
            "CONFORMED_VALUE": DecimalType(16, 5),
            "BUSINESS_DATE": DateType(),
            "CREATION_DATE": TimestampType(),
            "SYSTEM_TIMESTAMP": TimestampType(),
            "NSE_ID": StringType()
        }

        # Get actual schema of the transactions_df
        actual_schema = {field.name: field.dataType for field in transactions_df.schema.fields}

        # Check if the actual schema matches the expected schema
        for column, expected_type in expected_schema.items():
            assert column in actual_schema, f"Column {column} is missing in the DataFrame"
            
            # Compare the type names for correct matching
            assert actual_schema[column].typeName() == expected_type.typeName(), \
                f"Column {column} type mismatch: expected {expected_type.typeName()}, but got {actual_schema[column].typeName()}"

        # Step 2: Check Primary Key Constraints (should not have null values)
        pk_columns = ["CONTRACT_SOURCE_SYSTEM", "CONTRACT_SOURCE_SYSTEM_ID", "NSE_ID"]

        for pk in pk_columns:
            null_count = transactions_df.filter(col(pk).isNull()).count()
            assert null_count == 0, f"Primary Key column {pk} contains {null_count} null values"

        # Step 3: Test the "TRANSACTION_TYPE" for non-nullability
        transaction_type_null_count = transactions_df.filter(col("TRANSACTION_TYPE").isNull()).count()
        assert transaction_type_null_count == 0, f"TRANSACTION_TYPE contains {transaction_type_null_count} null values"

        # Step 4: Check if the nullable columns allow null values
        nullable_columns = ["TRANSACTION_DIRECTION", "CONFORMED_VALUE", "BUSINESS_DATE", "CREATION_DATE", "SYSTEM_TIMESTAMP"]

        for column in nullable_columns:
            # Ensure column allows nulls (test with at least one row with null values)
            null_count = transactions_df.filter(col(column).isNull()).count()
            if null_count == 0:
                print(f"Warning: Nullable column {column} contains no null values, which might be unexpected.")

        # Step 5: Check that the output format is correct
        # For example: Checking decimal precision and scale for "CONFORMED_VALUE"
        conf_value_sample = transactions_df.select("CONFORMED_VALUE").first()["CONFORMED_VALUE"]
        assert isinstance(conf_value_sample, Decimal), "CONFORMED_VALUE is not of type Decimal"

        # Round the value to 5 decimal places and check
        assert round(conf_value_sample, 5) == conf_value_sample, "CONFORMED_VALUE does not have 5 decimal places"

        # Step 6: Check Business Date Format
        business_dates = transactions_df.select("BUSINESS_DATE").distinct().collect()
        for row in business_dates:
            assert isinstance(row["BUSINESS_DATE"], date), \
                f"BUSINESS_DATE {row['BUSINESS_DATE']} is not of type date"

        # Step 7: Check the format of CREATION_DATE
        creation_dates = transactions_df.select("CREATION_DATE").distinct().collect()
        for row in creation_dates:
            assert isinstance(row["CREATION_DATE"], datetime), \
                f"CREATION_DATE {row['CREATION_DATE']} is not of type timestamp"
        
        return True # All tests passed
    
    # Test Output Data
    if test_transactions_df_schema():
        print("All Output Schema tests on TRANSACTIONS passed!")
        transactions_df.write.json("TRANSACTIONS.json", mode="overwrite")  # Save the DataFrame as JSON file for readability (.parquet otherwise)
    else:
        raise ValueError("Schema validation failed. The DataFrame does not match the expected schema.")

    spark.stop()  # Stop Spark session after the task is complete
