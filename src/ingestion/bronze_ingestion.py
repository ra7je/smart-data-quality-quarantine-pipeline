
import hashlib
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, struct, to_json, to_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, DecimalType
)

from src.snowflake.connection import get_snowflake_connection


# --------------------------------------------------
# 1. CONFIGURATION
# --------------------------------------------------

CUSTOMERS_FILE = "data/input/customers.csv"
TRANSACTIONS_FILE = "data/input/transactions.json"

BATCH_SIZE = 1000


# --------------------------------------------------
# 2. GENERATE A DETERMINISTIC RUN ID
# --------------------------------------------------

def generate_run_id(file_path):

    file_content = Path(file_path).read_bytes()

    file_hash = hashlib.sha256(file_content).hexdigest()

    return "TRANSACTIONS_" + file_hash[:20]


# --------------------------------------------------
# 3. CREATE SPARK SESSION
# --------------------------------------------------

def create_spark_session():

    return (
        SparkSession.builder
        .appName("YottaFlex_Bronze_Ingestion")
        .master("local[*]")
        .getOrCreate()
    )


# --------------------------------------------------
# 4. LOAD CUSTOMERS INTO SNOWFLAKE
# --------------------------------------------------

def load_customers(spark, connection):

    customer_schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("customer_name", StringType(), True),
        StructField("email", StringType(), True)
    ])

    customers_df = (
        spark.read
        .option("header", True)
        .schema(customer_schema)
        .csv(CUSTOMERS_FILE)
        .withColumn("source_file", lit("customers.csv"))
    )

    print("Customer records:", customers_df.count())

    cursor = connection.cursor()

    insert_sql = """
        INSERT INTO SMART_DATA_QUALITY_DB.BRONZE.CUSTOMERS
        (
            CUSTOMER_ID,
            CUSTOMER_NAME,
            EMAIL,
            CREATED_AT,
            SOURCE_FILE
        )
        SELECT %s, %s, %s, CURRENT_TIMESTAMP(), %s
        WHERE NOT EXISTS (
            SELECT 1
            FROM SMART_DATA_QUALITY_DB.BRONZE.CUSTOMERS
            WHERE CUSTOMER_ID = %s
        )
    """

    batch = []
    inserted = 0

    try:

        cursor.execute("BEGIN")

        for row in customers_df.toLocalIterator():

            batch.append((
                row.customer_id,
                row.customer_name,
                row.email,
                row.source_file,
                row.customer_id
            ))

            if len(batch) >= BATCH_SIZE:

                for record in batch:
                    cursor.execute(insert_sql, record)
                    inserted += cursor.rowcount if cursor.rowcount > 0 else 0

                batch.clear()

        if batch:

            for record in batch:
                cursor.execute(insert_sql, record)
                inserted += cursor.rowcount if cursor.rowcount > 0 else 0

        cursor.execute("COMMIT")

        print("Customer ingestion completed.")
        print("Customers inserted:", inserted)

    except Exception:

        cursor.execute("ROLLBACK")
        raise

    finally:

        cursor.close()


# --------------------------------------------------
# 5. LOAD TRANSACTIONS INTO SNOWFLAKE
# --------------------------------------------------

def load_transactions(spark, connection):

    transaction_schema = StructType([
        StructField("event_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("amount", DecimalType(18, 2), True),
        StructField("event_timestamp", StringType(), True)
    ])

    run_id = generate_run_id(TRANSACTIONS_FILE)

    transactions_df = (
        spark.read
        .schema(transaction_schema)
        .json(TRANSACTIONS_FILE)
        .withColumn(
            "event_timestamp",
            to_timestamp(col("event_timestamp"))
        )
        .withColumn(
            "source_file",
            lit("transactions.json")
        )
        .withColumn("run_id", lit(run_id))
    )

    payload_columns = [
        "event_id",
        "customer_id",
        "event_type",
        "amount",
        "event_timestamp"
    ]

    transactions_df = transactions_df.withColumn(
        "raw_payload",
        to_json(struct(*[col(c) for c in payload_columns]))
    )

    print("Transaction records:", transactions_df.count())
    print("Run ID:", run_id)

    cursor = connection.cursor()

    insert_sql = """
        INSERT INTO SMART_DATA_QUALITY_DB.BRONZE.RAW_TRANSACTIONS
        (
            EVENT_ID,
            CUSTOMER_ID,
            EVENT_TYPE,
            AMOUNT,
            EVENT_TIMESTAMP,
            SOURCE_FILE,
            RUN_ID,
            RAW_PAYLOAD
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """

    batch = []
    inserted = 0

    try:

        cursor.execute("BEGIN")

        # Remove previous records for the same file version.
        cursor.execute(
            """
            DELETE FROM SMART_DATA_QUALITY_DB.BRONZE.RAW_TRANSACTIONS
            WHERE RUN_ID = %s
            """,
            (run_id,)
        )

        for row in transactions_df.toLocalIterator():

            batch.append((
                row.event_id,
                row.customer_id,
                row.event_type,
                row.amount,
                row.event_timestamp,
                row.source_file,
                row.run_id,
                row.raw_payload
            ))

            if len(batch) >= BATCH_SIZE:

                for record in batch:
                    cursor.execute(insert_sql, record)
                    inserted += cursor.rowcount if cursor.rowcount > 0 else 0

                batch.clear()

        if batch:

            for record in batch:
                cursor.execute(insert_sql, record)
                inserted += cursor.rowcount if cursor.rowcount > 0 else 0

        cursor.execute("COMMIT")

        print("Transaction ingestion completed.")
        print("Transaction records inserted:", inserted)

    except Exception:

        cursor.execute("ROLLBACK")
        raise

    finally:

        cursor.close()


# --------------------------------------------------
# 6. MAIN EXECUTION
# --------------------------------------------------

def main():

    spark = create_spark_session()
    connection = None

    try:

        print("Starting Bronze ingestion...")

        connection = get_snowflake_connection()

        load_customers(spark, connection)

        load_transactions(spark, connection)

        print("BRONZE INGESTION COMPLETED SUCCESSFULLY")

    except Exception as error:

        print("BRONZE INGESTION FAILED:", error)
        raise

    finally:

        if connection is not None:
            connection.close()

        spark.stop()


if __name__ == "__main__":
    main()
