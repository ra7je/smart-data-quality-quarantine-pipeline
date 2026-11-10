
import time

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DecimalType, TimestampType
)
from pyspark.sql.functions import lit

from src.snowflake.connection import get_snowflake_connection
from src.validation.schema_validation import (
    validate_transaction_schema
)
from src.validation.business_validation import (
    validate_transactions
)
from src.validation.duplicate_validation import (
    validate_duplicates
)


RUN_ID = "BULK_100K_20261110"


def main():

    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("DataQuality100KBenchmark")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

    connection = get_snowflake_connection()
    cursor = connection.cursor()

    try:
        start_time = time.perf_counter()

        # Read only the generated 100K transaction batch.
        cursor.execute("""
            SELECT EVENT_ID, CUSTOMER_ID, EVENT_TYPE,
                   AMOUNT, EVENT_TIMESTAMP, SOURCE_FILE,
                   RUN_ID, INGESTED_AT, RAW_PAYLOAD
            FROM SMART_DATA_QUALITY_DB.BRONZE.RAW_TRANSACTIONS
            WHERE RUN_ID = %s
        """, (RUN_ID,))

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
            transaction_rows,
            transaction_schema
        )

        # Read generated customer reference records.
        cursor.execute("""
            SELECT CUSTOMER_ID
            FROM SMART_DATA_QUALITY_DB.BRONZE.CUSTOMERS
            WHERE SOURCE_FILE LIKE '%customers_1000.csv%'
        """)

        customer_rows = cursor.fetchall()

        customers_df = spark.createDataFrame(
            customer_rows,
            StructType([
                StructField("CUSTOMER_ID", StringType(), True)
            ])
        )

        # Read already-published transaction IDs.
        cursor.execute("""
            SELECT EVENT_ID
            FROM SMART_DATA_QUALITY_DB.SILVER.VALID_TRANSACTIONS
            WHERE EVENT_ID IS NOT NULL
        """)

        existing_rows = cursor.fetchall()

        existing_df = spark.createDataFrame(
            existing_rows,
            StructType([
                StructField("EVENT_ID", StringType(), True)
            ])
        )

        print("\nRunning schema validation...")

        schema_result = validate_transaction_schema(
            transactions_df
        )

        if not schema_result["is_valid"]:
            raise ValueError(
                f"Schema validation failed: {schema_result}"
            )

        print("\nRunning business validation...")

        business_valid_df, business_rejected_df = (
            validate_transactions(
                transactions_df,
                customers_df
            )
        )

        print("\nRunning duplicate validation...")

        final_valid_df, duplicate_rejected_df = (
            validate_duplicates(
                business_valid_df,
                existing_df
            )
        )

        # Aggregate counts in one Spark action.
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

        counts = {
            row["RESULT"]: row["count"]
            for row in summary_df.groupBy("RESULT").count().collect()
        }

        input_count = len(transaction_rows)
        valid_count = counts.get("VALID", 0)
        business_count = counts.get("BUSINESS_REJECTED", 0)
        duplicate_count = counts.get("DUPLICATE_REJECTED", 0)
        rejected_count = business_count + duplicate_count

        elapsed = time.perf_counter() - start_time

        print("\n========== 100K BENCHMARK REPORT ==========")
        print("Input records:", input_count)
        print("Valid records:", valid_count)
        print("Business rejected:", business_count)
        print("Duplicate rejected:", duplicate_count)
        print("Total rejected:", rejected_count)
        print("Reconciled:", input_count == valid_count + rejected_count)
        print("Validation and read time (seconds):", round(elapsed, 2))
        print("===========================================")

        if input_count != 100000:
            raise RuntimeError(
                f"Expected 100000 records, received {input_count}"
            )

        if input_count != valid_count + rejected_count:
            raise RuntimeError("Record reconciliation failed")

    finally:
        cursor.close()
        connection.close()
        spark.stop()


if __name__ == "__main__":
    main()
