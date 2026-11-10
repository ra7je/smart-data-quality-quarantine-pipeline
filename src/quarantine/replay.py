
import json
import uuid
from datetime import datetime

from pyspark.sql.types import StructType, StructField, StringType

from src.validation.business_validation import validate_transactions
from src.validation.duplicate_validation import validate_duplicates


def get_snowflake_dataframe(spark, connection, query, schema):

    cursor = connection.cursor()

    try:
        cursor.execute(query)
        rows = cursor.fetchall()
        return spark.createDataFrame(rows, schema)

    finally:
        cursor.close()


def replay_quarantined_records(spark, connection):

    correction_file = "data/corrected/transactions_corrected.json"

    print("\n========== QUARANTINE REPLAY STARTED ==========")

    # 1. Read corrected records.

    corrected_df = (
        spark.read
        .option("multiLine", "false")
        .json(correction_file)
    )

    if "QUARANTINE_ID" not in corrected_df.columns:
        raise ValueError(
            "Correction file must contain QUARANTINE_ID."
        )

    corrected_df = corrected_df.cache()

    if corrected_df.limit(1).count() == 0:
        print("No corrected records found.")
        corrected_df.unpersist()
        return

    # 2. Read customer reference data.

    customers_df = get_snowflake_dataframe(
        spark,
        connection,
        """
        SELECT CUSTOMER_ID
        FROM SMART_DATA_QUALITY_DB.BRONZE.CUSTOMERS
        """,
        StructType([
            StructField("CUSTOMER_ID", StringType(), True)
        ])
    )

    # 3. Validate corrected transactions.

    business_valid_df, business_rejected_df = (
        validate_transactions(corrected_df, customers_df)
    )

    existing_ids_df = get_snowflake_dataframe(
        spark,
        connection,
        """
        SELECT EVENT_ID
        FROM SMART_DATA_QUALITY_DB.SILVER.VALID_TRANSACTIONS
        """,
        StructType([
            StructField("EVENT_ID", StringType(), True)
        ])
    )

    final_valid_df, duplicate_rejected_df = validate_duplicates(
        business_valid_df,
        existing_ids_df
    )

    # 4. Publish successfully corrected records.

    cursor = connection.cursor()
    replayed_count = 0
    failed_count = 0

    try:

        for row in final_valid_df.toLocalIterator():

            replay_id = str(uuid.uuid4())

            cursor.execute(
                """
                MERGE INTO SMART_DATA_QUALITY_DB.SILVER.VALID_TRANSACTIONS T
                USING (
                    SELECT %s AS EVENT_ID
                ) S
                ON T.EVENT_ID = S.EVENT_ID
                WHEN NOT MATCHED THEN INSERT (
                    EVENT_ID,
                    CUSTOMER_ID,
                    EVENT_TYPE,
                    AMOUNT,
                    EVENT_TIMESTAMP,
                    SOURCE_FILE,
                    RUN_ID
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    row["EVENT_ID"],
                    row["EVENT_ID"],
                    row["CUSTOMER_ID"],
                    row["EVENT_TYPE"],
                    row["AMOUNT"],
                    row["EVENT_TIMESTAMP"],
                    row["SOURCE_FILE"],
                    row["RUN_ID"]
                )
            )

            quarantine_id = row["QUARANTINE_ID"]

            cursor.execute(
                """
                UPDATE SMART_DATA_QUALITY_DB.SILVER.QUARANTINE_RECORDS
                SET STATUS = 'REPLAYED',
                    REPLAY_COUNT = REPLAY_COUNT + 1,
                    UPDATED_AT = CURRENT_TIMESTAMP()
                WHERE QUARANTINE_ID = %s
                  AND STATUS = 'PENDING'
                """,
                (quarantine_id,)
            )

            cursor.execute(
                """
                INSERT INTO SMART_DATA_QUALITY_DB.AUDIT.REPLAY_HISTORY (
                    QUARANTINE_ID,
                    EVENT_ID,
                    REPLAY_ID,
                    REPLAY_STATUS,
                    ERROR_REASON
                )
                VALUES (%s, %s, %s, %s, %s)
                """,
                (
                    quarantine_id,
                    row["EVENT_ID"],
                    replay_id,
                    "SUCCESS",
                    None
                )
            )

            replayed_count += 1

        # 5. Record corrected rows that still fail validation.

        rejected_df = (
            business_rejected_df
            .select(
                "QUARANTINE_ID",
                "EVENT_ID",
                "VALIDATION_ERRORS"
            )
            .unionByName(
                duplicate_rejected_df.select(
                    "QUARANTINE_ID",
                    "EVENT_ID",
                    duplicate_rejected_df["DUPLICATE_ERROR"].alias(
                        "VALIDATION_ERRORS"
                    )
                )
            )
        )

        for row in rejected_df.toLocalIterator():

            replay_id = str(uuid.uuid4())

            cursor.execute(
                """
                INSERT INTO SMART_DATA_QUALITY_DB.AUDIT.REPLAY_HISTORY (
                    QUARANTINE_ID,
                    EVENT_ID,
                    REPLAY_ID,
                    REPLAY_STATUS,
                    ERROR_REASON
                )
                VALUES (%s, %s, %s, %s, %s)
                """,
                (
                    row["QUARANTINE_ID"],
                    row["EVENT_ID"],
                    replay_id,
                    "FAILED",
                    row["VALIDATION_ERRORS"]
                )
            )

            failed_count += 1

        connection.commit()

    except Exception:
        connection.rollback()
        raise

    finally:
        cursor.close()
        corrected_df.unpersist()

    print("\n========== REPLAY REPORT ==========")
    print("Successfully replayed:", replayed_count)
    print("Still rejected:", failed_count)
    print("===================================")
