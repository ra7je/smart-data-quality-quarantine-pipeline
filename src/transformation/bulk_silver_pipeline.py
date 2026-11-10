
import time
from src.snowflake.connection import get_snowflake_connection


# =========================================================
# CONFIGURATION
# =========================================================

DATABASE = "SMART_DATA_QUALITY_DB"

BATCH_RUN_ID = "BULK_100K_20261110"

BRONZE_TABLE = (
    f"{DATABASE}.BRONZE.RAW_TRANSACTIONS"
)

CUSTOMER_TABLE = (
    f"{DATABASE}.BRONZE.CUSTOMERS"
)

SILVER_TABLE = (
    f"{DATABASE}.SILVER.VALID_TRANSACTIONS"
)

QUARANTINE_TABLE = (
    f"{DATABASE}.SILVER.QUARANTINE_RECORDS"
)

AUDIT_TABLE = (
    f"{DATABASE}.AUDIT.PIPELINE_RUNS"
)

TEMP_TABLE = (
    f"{DATABASE}.SILVER.TMP_BULK_CLASSIFIED_100K"
)


# =========================================================
# CREATE CLASSIFIED TEMPORARY TABLE
# All validation runs inside Snowflake.
# =========================================================

CREATE_CLASSIFIED_TABLE_SQL = f"""
CREATE OR REPLACE TEMPORARY TABLE {TEMP_TABLE} AS

WITH base AS (

    SELECT
        TRIM(EVENT_ID) AS EVENT_ID,
        TRIM(CUSTOMER_ID) AS CUSTOMER_ID,
        UPPER(TRIM(EVENT_TYPE)) AS EVENT_TYPE,
        AMOUNT,
        EVENT_TIMESTAMP,
        SOURCE_FILE,
        RUN_ID,
        RAW_PAYLOAD,
        INGESTED_AT

    FROM {BRONZE_TABLE}

    WHERE RUN_ID = %s
),

business_checked AS (

    SELECT
        b.*,

        RTRIM(
            CONCAT(
                IFF(
                    EVENT_ID IS NULL OR EVENT_ID = '',
                    'EVENT_ID is missing; ',
                    ''
                ),
                IFF(
                    CUSTOMER_ID IS NULL OR CUSTOMER_ID = '',
                    'CUSTOMER_ID is missing; ',
                    ''
                ),
                IFF(
                    EVENT_TYPE IS NULL
                    OR EVENT_TYPE NOT IN ('CREDIT', 'DEBIT'),
                    'Invalid EVENT_TYPE; ',
                    ''
                ),
                IFF(
                    AMOUNT IS NULL OR AMOUNT <= 0,
                    'AMOUNT must be greater than zero; ',
                    ''
                ),
                IFF(
                    EVENT_TIMESTAMP IS NULL,
                    'EVENT_TIMESTAMP is missing; ',
                    ''
                ),
                IFF(
                    CUSTOMER_ID IS NOT NULL
                    AND CUSTOMER_ID <> ''
                    AND NOT EXISTS (
                        SELECT 1
                        FROM {CUSTOMER_TABLE} c
                        WHERE c.CUSTOMER_ID = b.CUSTOMER_ID
                    ),
                    'CUSTOMER_ID does not exist; ',
                    ''
                )
            ),
            '; '
        ) AS BUSINESS_ERROR

    FROM base b
),

business_valid AS (

    SELECT *
    FROM business_checked
    WHERE BUSINESS_ERROR = ''

),

ranked_valid AS (

    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY EVENT_ID
            ORDER BY INGESTED_AT, EVENT_TIMESTAMP
        ) AS BATCH_ROW_NUMBER

    FROM business_valid

),

classified AS (

    -- Business-rule failures
    SELECT
        EVENT_ID,
        CUSTOMER_ID,
        EVENT_TYPE,
        AMOUNT,
        EVENT_TIMESTAMP,
        SOURCE_FILE,
        RUN_ID,
        RAW_PAYLOAD,
        BUSINESS_ERROR AS REJECTION_REASON

    FROM business_checked
    WHERE BUSINESS_ERROR <> ''

    UNION ALL

    -- Duplicate and previously processed records
    SELECT
        EVENT_ID,
        CUSTOMER_ID,
        EVENT_TYPE,
        AMOUNT,
        EVENT_TIMESTAMP,
        SOURCE_FILE,
        RUN_ID,
        RAW_PAYLOAD,

        CASE

            WHEN BATCH_ROW_NUMBER > 1
                THEN 'Duplicate EVENT_ID within batch'

            WHEN EXISTS (
                SELECT 1
                FROM {SILVER_TABLE} s
                WHERE s.EVENT_ID = ranked_valid.EVENT_ID
            )
                THEN 'EVENT_ID already exists in Silver'

            ELSE NULL

        END AS REJECTION_REASON

    FROM ranked_valid

)

SELECT
    EVENT_ID,
    CUSTOMER_ID,
    EVENT_TYPE,
    AMOUNT,
    EVENT_TIMESTAMP,
    SOURCE_FILE,
    RUN_ID,
    RAW_PAYLOAD,
    REJECTION_REASON,

    SHA2(
        CONCAT(
            COALESCE(RUN_ID, ''), '|',
            COALESCE(EVENT_ID, ''), '|',
            COALESCE(REJECTION_REASON, ''), '|',
            COALESCE(RAW_PAYLOAD, '')
        ),
        256
    ) AS QUARANTINE_ID

FROM classified
"""


# =========================================================
# GET PIPELINE COUNTS
# =========================================================

def get_pipeline_counts(cursor):

    cursor.execute(f"""
        SELECT
            COUNT(*) AS INPUT_COUNT,

            COUNT_IF(
                REJECTION_REASON IS NULL
            ) AS VALID_COUNT,

            COUNT_IF(
                REJECTION_REASON IS NOT NULL
            ) AS REJECTED_COUNT,

            COUNT_IF(
                REJECTION_REASON LIKE '%Duplicate%'
                OR REJECTION_REASON LIKE '%already exists%'
            ) AS DUPLICATE_COUNT,

            COUNT_IF(
                REJECTION_REASON LIKE '%CUSTOMER_ID%'
                OR REJECTION_REASON LIKE '%EVENT_TYPE%'
                OR REJECTION_REASON LIKE '%AMOUNT%'
                OR REJECTION_REASON LIKE '%EVENT_TIMESTAMP%'
            ) AS BUSINESS_REJECTED_COUNT

        FROM {TEMP_TABLE}
    """)

    row = cursor.fetchone()

    return {
        "input": row[0],
        "valid": row[1],
        "rejected": row[2],
        "duplicates": row[3],
        "business_rejected": row[4]
    }


# =========================================================
# PUBLISH VALID RECORDS INTO SILVER
# =========================================================

def publish_valid_records(cursor):

    cursor.execute(f"""
        MERGE INTO {SILVER_TABLE} target

        USING (
            SELECT
                EVENT_ID,
                CUSTOMER_ID,
                EVENT_TYPE,
                AMOUNT,
                EVENT_TIMESTAMP,
                SOURCE_FILE,
                RUN_ID

            FROM {TEMP_TABLE}

            WHERE REJECTION_REASON IS NULL

            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY EVENT_ID
                ORDER BY EVENT_TIMESTAMP
            ) = 1

        ) source

        ON target.EVENT_ID = source.EVENT_ID

        WHEN NOT MATCHED THEN INSERT (
            EVENT_ID,
            CUSTOMER_ID,
            EVENT_TYPE,
            AMOUNT,
            EVENT_TIMESTAMP,
            SOURCE_FILE,
            RUN_ID
        )

        VALUES (
            source.EVENT_ID,
            source.CUSTOMER_ID,
            source.EVENT_TYPE,
            source.AMOUNT,
            source.EVENT_TIMESTAMP,
            source.SOURCE_FILE,
            source.RUN_ID
        )
    """)


# =========================================================
# PUBLISH REJECTED RECORDS INTO QUARANTINE
# =========================================================

def publish_quarantine_records(cursor):

    cursor.execute(f"""
        MERGE INTO {QUARANTINE_TABLE} target

        USING (
            SELECT
                QUARANTINE_ID,
                EVENT_ID,
                CUSTOMER_ID,
                RAW_PAYLOAD,
                REJECTION_REASON,
                SOURCE_FILE,
                RUN_ID

            FROM {TEMP_TABLE}

            WHERE REJECTION_REASON IS NOT NULL

            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY QUARANTINE_ID
                ORDER BY EVENT_ID
            ) = 1

        ) source

        ON target.QUARANTINE_ID = source.QUARANTINE_ID

        WHEN NOT MATCHED THEN INSERT (
            QUARANTINE_ID,
            EVENT_ID,
            CUSTOMER_ID,
            RAW_PAYLOAD,
            ERROR_REASON,
            SOURCE_FILE,
            RUN_ID,
            STATUS,
            REPLAY_COUNT
        )

        VALUES (
            source.QUARANTINE_ID,
            source.EVENT_ID,
            source.CUSTOMER_ID,
            source.RAW_PAYLOAD,
            source.REJECTION_REASON,
            source.SOURCE_FILE,
            source.RUN_ID,
            'PENDING',
            0
        )
    """)


# =========================================================
# WRITE AUDIT METRICS
# =========================================================

def write_audit_record(cursor, counts, execution_seconds):

    audit_run_id = f"{BATCH_RUN_ID}_SILVER"

    cursor.execute(f"""
        MERGE INTO {AUDIT_TABLE} target

        USING (
            SELECT
                %s AS RUN_ID,
                %s AS SOURCE_FILE,
                %s AS INPUT_COUNT,
                %s AS VALID_COUNT,
                %s AS REJECTED_COUNT,
                %s AS DUPLICATE_COUNT,
                %s AS EXECUTION_SECONDS

        ) source

        ON target.RUN_ID = source.RUN_ID

        WHEN MATCHED THEN UPDATE SET
            SOURCE_FILE = source.SOURCE_FILE,
            END_TIME = CURRENT_TIMESTAMP(),
            INPUT_COUNT = source.INPUT_COUNT,
            VALID_COUNT = source.VALID_COUNT,
            REJECTED_COUNT = source.REJECTED_COUNT,
            DUPLICATE_COUNT = source.DUPLICATE_COUNT,
            RUN_STATUS = 'SUCCESS',
            ERROR_MESSAGE = NULL,
            EXECUTION_SECONDS = source.EXECUTION_SECONDS

        WHEN NOT MATCHED THEN INSERT (
            RUN_ID,
            SOURCE_FILE,
            START_TIME,
            END_TIME,
            INPUT_COUNT,
            VALID_COUNT,
            REJECTED_COUNT,
            DUPLICATE_COUNT,
            RUN_STATUS,
            ERROR_MESSAGE,
            EXECUTION_SECONDS
        )

        VALUES (
            source.RUN_ID,
            source.SOURCE_FILE,
            CURRENT_TIMESTAMP(),
            CURRENT_TIMESTAMP(),
            source.INPUT_COUNT,
            source.VALID_COUNT,
            source.REJECTED_COUNT,
            source.DUPLICATE_COUNT,
            'SUCCESS',
            NULL,
            source.EXECUTION_SECONDS
        )
    """, (
        audit_run_id,
        "transactions_100k.json",
        counts["input"],
        counts["valid"],
        counts["rejected"],
        counts["duplicates"],
        execution_seconds
    ))


# =========================================================
# MAIN PIPELINE
# =========================================================

def run_bulk_silver_pipeline():

    start_time = time.time()

    connection = None
    cursor = None

    try:

        print("\n====================================")
        print("SMART DATA QUALITY BULK PIPELINE")
        print("====================================")

        connection = get_snowflake_connection()
        cursor = connection.cursor()

        # Step 1: Build classification table.
        print("\n1. Validating Bronze batch inside Snowflake...")

        cursor.execute(
            CREATE_CLASSIFIED_TABLE_SQL,
            (BATCH_RUN_ID,)
        )

        # Step 2: Get validation metrics.
        print("2. Calculating data quality metrics...")

        counts = get_pipeline_counts(cursor)

        if counts["input"] == 0:
            raise ValueError(
                f"No records found for {BATCH_RUN_ID}"
            )

        if counts["input"] != (
            counts["valid"] + counts["rejected"]
        ):
            raise ValueError(
                "Input reconciliation failed."
            )

        print("\nVALIDATION RESULTS")
        print("------------------")
        print(f"Input: {counts['input']}")
        print(f"Valid: {counts['valid']}")
        print(f"Business rejected: {counts['business_rejected']}")
        print(f"Duplicates: {counts['duplicates']}")
        print(f"Total rejected: {counts['rejected']}")

        # Step 3: Publish records and audit atomically.
        print("\n3. Publishing valid records and quarantine...")

        execution_seconds = round(
            time.time() - start_time, 2
        )

        cursor.execute("BEGIN")

        publish_valid_records(cursor)

        publish_quarantine_records(cursor)

        write_audit_record(
            cursor,
            counts,
            execution_seconds
        )

        connection.commit()

        execution_seconds = round(
            time.time() - start_time, 2
        )

        print("\n====================================")
        print("BULK PIPELINE COMPLETED SUCCESSFULLY")
        print("====================================")
        print(f"Input records: {counts['input']}")
        print(f"Valid records: {counts['valid']}")
        print(f"Rejected records: {counts['rejected']}")
        print(f"Execution time: {execution_seconds} seconds")

    except Exception as error:

        if connection is not None:
            try:
                connection.rollback()
            except Exception:
                pass

        print("\nBULK PIPELINE FAILED")
        print(str(error))

        raise

    finally:

        if cursor is not None:
            cursor.close()

        if connection is not None:
            connection.close()


if __name__ == "__main__":
    run_bulk_silver_pipeline()
