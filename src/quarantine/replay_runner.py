
import json
import uuid
from datetime import datetime

from src.snowflake.connection import get_snowflake_connection


def replay_corrected_transactions():

    correction_file = "data/corrected/transactions_corrected.json"

    with open(correction_file, "r", encoding="utf-8") as file:
        corrections = [
            json.loads(line)
            for line in file
            if line.strip()
        ]

    connection = get_snowflake_connection()
    cursor = connection.cursor()

    success_count = 0
    failed_count = 0

    try:
        for correction in corrections:

            event_id = correction.get("EVENT_ID")
            replay_id = str(uuid.uuid4())

            if not event_id:
                print("Skipped correction without EVENT_ID")
                failed_count += 1
                continue

            # 1. Find the original pending quarantine record.

            cursor.execute("""
                SELECT QUARANTINE_ID, RUN_ID, STATUS
                FROM SMART_DATA_QUALITY_DB.SILVER.QUARANTINE_RECORDS
                WHERE EVENT_ID = %s
                  AND SOURCE_FILE = %s
                  AND STATUS = 'PENDING'
                ORDER BY CREATED_AT
                LIMIT 1
            """, (
                event_id,
                correction.get("SOURCE_FILE")
            ))

            quarantine = cursor.fetchone()

            if quarantine is None:
                print(f"{event_id}: No pending quarantine record found")
                failed_count += 1
                continue

            quarantine_id, original_run_id, status = quarantine

            # 2. Read corrected values.

            customer_id = correction.get("CUSTOMER_ID")
            event_type = correction.get("EVENT_TYPE", "").upper()
            amount = correction.get("AMOUNT")

            try:
                event_timestamp = datetime.fromisoformat(
                    correction["EVENT_TIMESTAMP"]
                )
            except (KeyError, ValueError):
                event_timestamp = None

            # 3. Validate corrected record.

            errors = []

            if not customer_id or not str(customer_id).strip():
                errors.append("CUSTOMER_ID is missing")
            else:
                cursor.execute("""
                    SELECT COUNT(*)
                    FROM SMART_DATA_QUALITY_DB.BRONZE.CUSTOMERS
                    WHERE CUSTOMER_ID = %s
                """, (customer_id,))

                if cursor.fetchone()[0] == 0:
                    errors.append("CUSTOMER_ID does not exist")

            if event_type not in ("CREDIT", "DEBIT"):
                errors.append("Invalid EVENT_TYPE")

            if amount is None or amount <= 0:
                errors.append("AMOUNT must be greater than zero")

            if event_timestamp is None:
                errors.append("EVENT_TIMESTAMP is missing or invalid")

            # 4. If still invalid, record failed replay.

            if errors:

                cursor.execute("""
                    INSERT INTO SMART_DATA_QUALITY_DB.AUDIT.REPLAY_HISTORY (
                        QUARANTINE_ID,
                        EVENT_ID,
                        REPLAY_ID,
                        REPLAY_STATUS,
                        ERROR_REASON
                    )
                    VALUES (%s, %s, %s, %s, %s)
                """, (
                    quarantine_id,
                    event_id,
                    replay_id,
                    "FAILED",
                    "; ".join(errors)
                ))

                failed_count += 1
                print(f"{event_id}: Replay failed - {'; '.join(errors)}")
                continue

            # 5. Publish corrected transaction using MERGE.

            cursor.execute("""
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
            """, (
                event_id,
                customer_id,
                event_type,
                amount,
                event_timestamp,
                correction.get("SOURCE_FILE"),
                original_run_id
            ))

            # 6. Mark original quarantine record as replayed.

            cursor.execute("""
                UPDATE SMART_DATA_QUALITY_DB.SILVER.QUARANTINE_RECORDS
                SET STATUS = 'REPLAYED',
                    REPLAY_COUNT = REPLAY_COUNT + 1,
                    UPDATED_AT = CURRENT_TIMESTAMP()
                WHERE QUARANTINE_ID = %s
                  AND STATUS = 'PENDING'
            """, (quarantine_id,))

            if cursor.rowcount != 1:
                raise RuntimeError(
                    f"Could not update pending quarantine record for {event_id}"
                )

            # 7. Record replay history.

            cursor.execute("""
                INSERT INTO SMART_DATA_QUALITY_DB.AUDIT.REPLAY_HISTORY (
                    QUARANTINE_ID,
                    EVENT_ID,
                    REPLAY_ID,
                    REPLAY_STATUS,
                    ERROR_REASON
                )
                VALUES (%s, %s, %s, %s, %s)
            """, (
                quarantine_id,
                event_id,
                replay_id,
                "SUCCESS",
                None
            ))

            success_count += 1
            print(f"{event_id}: Successfully replayed")

        connection.commit()

        print("\n========== REPLAY SUMMARY ==========")
        print("Successful replays:", success_count)
        print("Failed replays:", failed_count)
        print("====================================")

    except Exception:
        connection.rollback()
        raise

    finally:
        cursor.close()
        connection.close()


if __name__ == "__main__":
    replay_corrected_transactions()
