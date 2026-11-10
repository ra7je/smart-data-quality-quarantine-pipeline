
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DecimalType, TimestampType
)
from pyspark.sql.functions import col, lit

from src.validation.schema_validation import (
    validate_transaction_schema,
    display_schema_validation
)
from src.validation.business_validation import validate_transactions
from src.validation.duplicate_validation import validate_duplicates


def read_bronze_data(spark, connection):

    cursor = connection.cursor()

    try:
        cursor.execute("""
            SELECT EVENT_ID, CUSTOMER_ID, EVENT_TYPE, AMOUNT,
                   EVENT_TIMESTAMP, SOURCE_FILE, RUN_ID,
                   INGESTED_AT, RAW_PAYLOAD
            FROM SMART_DATA_QUALITY_DB.BRONZE.RAW_TRANSACTIONS
        """)

        transaction_rows = cursor.fetchall()

        transaction_schema = StructType([
            StructField("EVENT_ID", StringType(), True),
            StructField("CUSTOMER_ID", StringType(), True),
            StructField("EVENT_TYPE", StringType(), True),
            StructField("AMOUNT", DecimalType(18, 2), True),
            StructField("EVENT_TIMESTAMP", TimestampType(), True),
            StructField("SOURCE_FILE", StringType(), True),
            StructField("RUN_ID", StringType(), True),
            StructField("INGESTED_AT", TimestampType(), True),
            StructField("RAW_PAYLOAD", StringType(), True)
        ])

        transactions_df = spark.createDataFrame(
            transaction_rows, transaction_schema
        )

        cursor.execute("""
            SELECT CUSTOMER_ID
            FROM SMART_DATA_QUALITY_DB.BRONZE.CUSTOMERS
        """)

        customer_rows = cursor.fetchall()

        customers_df = spark.createDataFrame(
            customer_rows,
            StructType([
                StructField("CUSTOMER_ID", StringType(), True)
            ])
        )

        return transactions_df, customers_df

    finally:
        cursor.close()


def read_existing_silver_ids(spark, connection):

    cursor = connection.cursor()

    try:
        cursor.execute("""
            SELECT EVENT_ID
            FROM SMART_DATA_QUALITY_DB.SILVER.VALID_TRANSACTIONS
        """)

        rows = cursor.fetchall()

        return spark.createDataFrame(
            rows,
            StructType([
                StructField("EVENT_ID", StringType(), True)
            ])
        )

    finally:
        cursor.close()


def run_silver_validation(spark, connection):

    print("\nSILVER DATA QUALITY ENGINE STARTED")

    transactions_df, customers_df = read_bronze_data(
        spark, connection
    )

    existing_ids_df = read_existing_silver_ids(
        spark, connection
    )

    # Validate schema.
    schema_result = validate_transaction_schema(transactions_df)

    if not display_schema_validation(schema_result):
        raise ValueError("Transaction schema validation failed")

    # Validate business rules.
    business_valid_df, business_rejected_df = validate_transactions(
        transactions_df, customers_df
    )

    # Validate duplicates.
    final_valid_df, duplicate_rejected_df = validate_duplicates(
        business_valid_df, existing_ids_df
    )

    # Build one small summary aggregation instead of repeatedly
    # calling count() and show() throughout the validation modules.
    summary_df = (
        final_valid_df
        .select(lit("VALID").alias("RESULT"))
        .unionByName(
            business_rejected_df.select(
                lit("BUSINESS_REJECTED").alias("RESULT")
            )
        )
        .unionByName(
            duplicate_rejected_df.select(
                lit("DUPLICATE_REJECTED").alias("RESULT")
            )
        )
    )

    summary = {
        row["RESULT"]: row["count"]
        for row in summary_df.groupBy("RESULT").count().collect()
    }

    valid_count = summary.get("VALID", 0)
    business_count = summary.get("BUSINESS_REJECTED", 0)
    duplicate_count = summary.get("DUPLICATE_REJECTED", 0)
    rejected_count = business_count + duplicate_count
    total_count = valid_count + rejected_count

    print("\n========== SILVER VALIDATION REPORT ==========")
    print("Total input records:", total_count)
    print("Valid records:", valid_count)
    print("Business rejected:", business_count)
    print("Duplicate rejected:", duplicate_count)
    print("Total rejected:", rejected_count)
    print("Reconciliation:", total_count == valid_count + rejected_count)
    print("==============================================")

    return (
        final_valid_df,
        business_rejected_df,
        duplicate_rejected_df
    )
