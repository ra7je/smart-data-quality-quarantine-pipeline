
import time

from src.snowflake.connection import get_snowflake_connection
from src.transformation.bulk_silver_pipeline import (
    run_bulk_silver_pipeline
)


# =========================================================
# CONFIGURATION
# =========================================================

DATABASE = "SMART_DATA_QUALITY_DB"

BATCH_RUN_ID = "BULK_100K_20261110"

AUDIT_RUN_ID = f"{BATCH_RUN_ID}_SILVER"

SILVER_TABLE = (
    f"{DATABASE}.SILVER.VALID_TRANSACTIONS"
)

QUARANTINE_TABLE = (
    f"{DATABASE}.SILVER.QUARANTINE_RECORDS"
)

AUDIT_TABLE = (
    f"{DATABASE}.AUDIT.PIPELINE_RUNS"
)

MAX_ATTEMPTS = 2


# =========================================================
# CHECK WHETHER THE BATCH IS ALREADY SUCCESSFUL
# =========================================================

def get_batch_status():

    connection = get_snowflake_connection()
    cursor = connection.cursor()

    try:

        cursor.execute(f"""
            SELECT RUN_STATUS
            FROM {AUDIT_TABLE}
            WHERE RUN_ID = %s
            ORDER BY END_TIME DESC
            LIMIT 1
        """, (AUDIT_RUN_ID,))

        audit_row = cursor.fetchone()

        if audit_row and audit_row[0] == "SUCCESS":
            return "SUCCESS"

        # A previous partial or failed publication must not
        # be silently treated as a fresh batch.
        cursor.execute(f"""
            SELECT COUNT(*)
            FROM {SILVER_TABLE}
            WHERE RUN_ID = %s
        """, (BATCH_RUN_ID,))

        silver_count = cursor.fetchone()[0]

        cursor.execute(f"""
            SELECT COUNT(*)
            FROM {QUARANTINE_TABLE}
            WHERE RUN_ID = %s
        """, (BATCH_RUN_ID,))

        quarantine_count = cursor.fetchone()[0]

        if silver_count > 0 or quarantine_count > 0:
            return "PARTIAL_OR_UNAUDITED"

        return "NOT_PROCESSED"

    finally:
        cursor.close()
        connection.close()


# =========================================================
# RECORD PIPELINE FAILURE
# =========================================================

def record_failure(error_message):

    connection = get_snowflake_connection()
    cursor = connection.cursor()

    try:

        cursor.execute(f"""
            MERGE INTO {AUDIT_TABLE} target

            USING (
                SELECT
                    %s AS RUN_ID,
                    %s AS SOURCE_FILE,
                    %s AS ERROR_MESSAGE
            ) source

            ON target.RUN_ID = source.RUN_ID

            WHEN MATCHED THEN UPDATE SET
                END_TIME = CURRENT_TIMESTAMP(),
                RUN_STATUS = 'FAILED',
                ERROR_MESSAGE = source.ERROR_MESSAGE

            WHEN NOT MATCHED THEN INSERT (
                RUN_ID,
                SOURCE_FILE,
                START_TIME,
                END_TIME,
                RUN_STATUS,
                ERROR_MESSAGE
            )
            VALUES (
                source.RUN_ID,
                source.SOURCE_FILE,
                CURRENT_TIMESTAMP(),
                CURRENT_TIMESTAMP(),
                'FAILED',
                source.ERROR_MESSAGE
            )
        """, (
            AUDIT_RUN_ID,
            "transactions_100k.json",
            error_message[:4000]
        ))

        connection.commit()

    finally:
        cursor.close()
        connection.close()


# =========================================================
# RECOVERY CONTROLLER
# =========================================================

def recover_bulk_pipeline():

    print("\n====================================")
    print("BULK PIPELINE RECOVERY CONTROLLER")
    print("====================================")

    status = get_batch_status()

    if status == "SUCCESS":

        print(
            "\nThis batch already has a successful audit record."
        )
        print("No retry is required.")
        return

    if status == "PARTIAL_OR_UNAUDITED":

        raise RuntimeError(
            "Silver or Quarantine already contains records for "
            "this batch, but no successful audit record exists. "
            "Automatic retry stopped to prevent incorrect "
            "quarantine classification. Reconcile this batch "
            "before retrying."
        )

    for attempt in range(1, MAX_ATTEMPTS + 1):

        print(f"\nAttempt {attempt} of {MAX_ATTEMPTS}")

        try:

            run_bulk_silver_pipeline()

            print("\nRecovery controller: SUCCESS")
            return

        except Exception as error:

            print(f"\nAttempt {attempt} failed: {error}")

            # Check whether the pipeline actually committed
            # successfully before deciding to retry.
            try:

                latest_status = get_batch_status()

                if latest_status == "SUCCESS":
                    print(
                        "\nSuccessful audit found. "
                        "No additional retry will be attempted."
                    )
                    return

            except Exception as status_error:

                print(
                    "Could not verify final batch status: "
                    f"{status_error}"
                )

                raise RuntimeError(
                    "Retry stopped because commit status "
                    "could not be verified."
                ) from error

            try:
                record_failure(str(error))
            except Exception as audit_error:
                print(
                    "Could not record failure in audit: "
                    f"{audit_error}"
                )

            if attempt == MAX_ATTEMPTS:
                raise RuntimeError(
                    "Maximum retry attempts reached."
                ) from error

            print("\nWaiting before retry...")
            time.sleep(5)

    raise RuntimeError("Recovery controller exited unexpectedly.")


# =========================================================
# EXECUTION
# =========================================================

if __name__ == "__main__":
    recover_bulk_pipeline()
