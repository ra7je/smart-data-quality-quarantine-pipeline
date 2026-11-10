
from src.snowflake.connection import get_snowflake_connection


# =========================================================
# CONFIGURATION
# =========================================================

DATABASE = "SMART_DATA_QUALITY_DB"
BATCH_RUN_ID = "BULK_100K_20261110"
AUDIT_RUN_ID = f"{BATCH_RUN_ID}_SILVER"

BRONZE = f"{DATABASE}.BRONZE.RAW_TRANSACTIONS"
SILVER = f"{DATABASE}.SILVER.VALID_TRANSACTIONS"
QUARANTINE = f"{DATABASE}.SILVER.QUARANTINE_RECORDS"
GOLD = f"{DATABASE}.GOLD.FACT_TRANSACTIONS"
AUDIT = f"{DATABASE}.AUDIT.PIPELINE_RUNS"


# =========================================================
# QUERY HELPER
# =========================================================

def fetch_scalar(cursor, query, params=None):
    cursor.execute(query, params or ())
    result = cursor.fetchone()
    return result[0] if result else 0


# =========================================================
# HEALTH CHECK
# =========================================================

def run_health_check():

    connection = None
    cursor = None
    results = []

    try:
        connection = get_snowflake_connection()
        cursor = connection.cursor()

        print("\n======================================")
        print("SMART DATA QUALITY PIPELINE MONITOR")
        print("======================================")

        # 1. Layer counts

        bronze_count = fetch_scalar(
            cursor,
            f"""
            SELECT COUNT(*)
            FROM {BRONZE}
            WHERE RUN_ID = %s
            """,
            (BATCH_RUN_ID,)
        )

        silver_count = fetch_scalar(
            cursor,
            f"""
            SELECT COUNT(*)
            FROM {SILVER}
            WHERE RUN_ID = %s
            """,
            (BATCH_RUN_ID,)
        )

        quarantine_count = fetch_scalar(
            cursor,
            f"""
            SELECT COUNT(*)
            FROM {QUARANTINE}
            WHERE RUN_ID = %s
            """,
            (BATCH_RUN_ID,)
        )

        gold_count = fetch_scalar(
            cursor,
            f"""
            SELECT COUNT(*)
            FROM {GOLD}
            WHERE RUN_ID = %s
            """,
            (BATCH_RUN_ID,)
        )

        print("\nLAYER COUNTS")
        print("--------------------------------------")
        print(f"Bronze     : {bronze_count}")
        print(f"Silver     : {silver_count}")
        print(f"Quarantine : {quarantine_count}")
        print(f"Gold       : {gold_count}")

        # 2. Bronze reconciliation

        bronze_reconciled = (
            bronze_count == silver_count + quarantine_count
        )

        results.append((
            "Bronze reconciliation",
            bronze_reconciled
        ))

        # 3. Silver and Gold reconciliation

        gold_reconciled = (
            silver_count == gold_count
        )

        results.append((
            "Silver-Gold reconciliation",
            gold_reconciled
        ))

        # 4. Duplicate Silver records

        silver_duplicates = fetch_scalar(
            cursor,
            f"""
            SELECT COUNT(*)
            FROM (
                SELECT EVENT_ID
                FROM {SILVER}
                WHERE RUN_ID = %s
                GROUP BY EVENT_ID
                HAVING COUNT(*) > 1
            )
            """,
            (BATCH_RUN_ID,)
        )

        results.append((
            "Silver duplicate check",
            silver_duplicates == 0
        ))

        # 5. Duplicate quarantine IDs

        quarantine_duplicates = fetch_scalar(
            cursor,
            f"""
            SELECT COUNT(*)
            FROM (
                SELECT QUARANTINE_ID
                FROM {QUARANTINE}
                WHERE RUN_ID = %s
                GROUP BY QUARANTINE_ID
                HAVING COUNT(*) > 1
            )
            """,
            (BATCH_RUN_ID,)
        )

        results.append((
            "Quarantine duplicate check",
            quarantine_duplicates == 0
        ))

        # 6. Duplicate Gold records

        gold_duplicates = fetch_scalar(
            cursor,
            f"""
            SELECT COUNT(*)
            FROM (
                SELECT EVENT_ID
                FROM {GOLD}
                WHERE RUN_ID = %s
                GROUP BY EVENT_ID
                HAVING COUNT(*) > 1
            )
            """,
            (BATCH_RUN_ID,)
        )

        results.append((
            "Gold duplicate check",
            gold_duplicates == 0
        ))

        # 7. Audit record status

        cursor.execute(
            f"""
            SELECT
                COUNT(*),
                MAX(RUN_STATUS)
            FROM {AUDIT}
            WHERE RUN_ID = %s
            """,
            (AUDIT_RUN_ID,)
        )

        audit_count, audit_status = cursor.fetchone()

        audit_ok = (
            audit_count == 1
            and audit_status == "SUCCESS"
        )

        results.append((
            "Pipeline audit status",
            audit_ok
        ))

        # 8. Audit count reconciliation

        cursor.execute(
            f"""
            SELECT
                INPUT_COUNT,
                VALID_COUNT,
                REJECTED_COUNT
            FROM {AUDIT}
            WHERE RUN_ID = %s
            """,
            (AUDIT_RUN_ID,)
        )

        audit_row = cursor.fetchone()

        if audit_row:
            audit_input, audit_valid, audit_rejected = audit_row

            audit_counts_ok = (
                audit_input == bronze_count
                and audit_valid == silver_count
                and audit_rejected == quarantine_count
                and audit_input == audit_valid + audit_rejected
            )
        else:
            audit_counts_ok = False

        results.append((
            "Audit count reconciliation",
            audit_counts_ok
        ))

        # 9. Print results

        print("\nHEALTH CHECK RESULTS")
        print("--------------------------------------")

        for check_name, passed in results:
            print(
                f"{check_name:<32} "
                f"{'PASS' if passed else 'FAIL'}"
            )

        failed_checks = [
            name for name, passed in results if not passed
        ]

        print("\n======================================")

        if not failed_checks:
            print("OVERALL STATUS: PASS")
            print("All batch health checks passed.")
        else:
            print("OVERALL STATUS: FAIL")
            print("Failed checks:")

            for check in failed_checks:
                print(f"- {check}")

        print("======================================")

        return len(failed_checks) == 0

    except Exception as error:

        print("\nHEALTH MONITOR ERROR")
        print(str(error))
        raise

    finally:

        if cursor is not None:
            cursor.close()

        if connection is not None:
            connection.close()


if __name__ == "__main__":
    passed = run_health_check()

    if not passed:
        raise SystemExit(1)
