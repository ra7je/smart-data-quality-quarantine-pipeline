
import hashlib
import time
from datetime import datetime


def publish_to_snowflake(
    valid_df,
    business_rejected_df,
    duplicate_rejected_df,
    connection
):
    """
    Publish validated transactions to Silver,
    rejected records to Quarantine, and metrics to Audit.
    """

    started_at = datetime.now()
    start_clock = time.time()

    cursor = connection.cursor()

    try:
        valid_count = valid_df.count()
        business_count = business_rejected_df.count()
        duplicate_count = duplicate_rejected_df.count()

        input_count = valid_count + business_count + duplicate_count
        rejected_count = business_count + duplicate_count

        run_id = hashlib.sha256(
            f"{started_at.isoformat()}".encode()
        ).hexdigest()[:32]

        # ------------------------------------------
        # 1. PUBLISH VALID RECORDS USING MERGE
        # ------------------------------------------

        valid_sql = """
            MERGE INTO SMART_DATA_QUALITY_DB.SILVER.VALID_TRANSACTIONS T
            USING (
                SELECT
                    %s AS EVENT_ID,
                    %s AS CUSTOMER_ID,
                    %s AS EVENT_TYPE,
                    %s AS AMOUNT,
                    %s AS EVENT_TIMESTAMP,
                    %s AS SOURCE_FILE,
                    %s AS RUN_ID
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
            VALUES (
                S.EVENT_ID,
                S.CUSTOMER_ID,
                S.EVENT_TYPE,
                S.AMOUNT,
                S.EVENT_TIMESTAMP,
                S.SOURCE_FILE,
                S.RUN_ID
            )
        """

        for row in valid_df.select(
            "EVENT_ID",
            "CUSTOMER_ID",
            "EVENT_TYPE",
            "AMOUNT",
            "EVENT_TIMESTAMP",
            "SOURCE_FILE",
            "RUN_ID"
        ).toLocalIterator():

            cursor.execute(
                valid_sql,
                (
                    row["EVENT_ID"],
                    row["CUSTOMER_ID"],
                    row["EVENT_TYPE"],
                    row["AMOUNT"],
                    row["EVENT_TIMESTAMP"],
                    row["SOURCE_FILE"],
                    row["RUN_ID"]
                )
            )

        # ------------------------------------------
        # 2. PUBLISH REJECTED RECORDS
        # ------------------------------------------

        quarantine_sql = """
            INSERT INTO SMART_DATA_QUALITY_DB.SILVER.QUARANTINE_RECORDS (
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
            SELECT
                %s, %s, %s, %s, %s, %s, %s, 'PENDING', 0
            WHERE NOT EXISTS (
                SELECT 1
                FROM SMART_DATA_QUALITY_DB.SILVER.QUARANTINE_RECORDS
                WHERE QUARANTINE_ID = %s
            )
        """

        def publish_rejections(dataframe, error_column):

            selected_columns = [
                "EVENT_ID",
                "CUSTOMER_ID",
                "RAW_PAYLOAD",
                "SOURCE_FILE",
                "RUN_ID",
                error_column
            ]

            for row in dataframe.select(
                *selected_columns
            ).toLocalIterator():

                event_id = row["EVENT_ID"]
                customer_id = row["CUSTOMER_ID"]
                raw_payload = row["RAW_PAYLOAD"]
                source_file = row["SOURCE_FILE"]
                source_run_id = row["RUN_ID"]
                error_reason = row[error_column]

                identity = "|".join([
                    str(event_id or ""),
                    str(source_run_id or ""),
                    str(error_reason or "")
                ])

                quarantine_id = hashlib.sha256(
                    identity.encode()
                ).hexdigest()

                cursor.execute(
                    quarantine_sql,
                    (
                        quarantine_id,
                        event_id,
                        customer_id,
                        raw_payload,
                        error_reason,
                        source_file,
                        source_run_id,
                        quarantine_id
                    )
                )

        publish_rejections(
            business_rejected_df,
            "VALIDATION_ERRORS"
        )

        publish_rejections(
            duplicate_rejected_df,
            "DUPLICATE_ERROR"
        )

        # ------------------------------------------
        # 3. WRITE PIPELINE AUDIT
        # ------------------------------------------

        ended_at = datetime.now()
        execution_seconds = time.time() - start_clock

        cursor.execute(
            """
            INSERT INTO SMART_DATA_QUALITY_DB.AUDIT.PIPELINE_RUNS (
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
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                run_id,
                "transactions.json",
                started_at,
                ended_at,
                input_count,
                valid_count,
                rejected_count,
                duplicate_count,
                "SUCCESS",
                None,
                execution_seconds
            )
        )

        connection.commit()

        print("\n========== SNOWFLAKE PUBLISH REPORT ==========")
        print("Pipeline run ID:", run_id)
        print("Input records:", input_count)
        print("Published valid records:", valid_count)
        print("Business quarantine records:", business_count)
        print("Duplicate quarantine records:", duplicate_count)
        print("Total rejected records:", rejected_count)
        print("Execution seconds:", round(execution_seconds, 2))
        print("Status: SUCCESS")
        print("==============================================")

        return run_id

    except Exception:
        connection.rollback()
        raise

    finally:
        cursor.close()
