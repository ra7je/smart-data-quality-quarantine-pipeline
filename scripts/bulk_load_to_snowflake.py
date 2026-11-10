
from pathlib import Path
from datetime import datetime

from src.snowflake.connection import get_snowflake_connection


ROOT = Path(__file__).resolve().parent.parent

CUSTOMER_FILE = ROOT / "data" / "sample" / "customers_1000.csv"
TRANSACTION_FILE = ROOT / "data" / "sample" / "transactions_100k.json"

STAGE = "SMART_DATA_QUALITY_DB.BRONZE.DQ_BULK_STAGE"

RUN_ID = "BULK_100K_20261110"


def upload_file(cursor, file_path):

    if not file_path.exists():
        raise FileNotFoundError(
            f"Generated data file not found: {file_path}"
        )

    local_uri = "file://" + file_path.resolve().as_posix()

    cursor.execute(
        f"""
        PUT '{local_uri}'
        @{STAGE}
        AUTO_COMPRESS = TRUE
        OVERWRITE = TRUE
        """
    )

    for result in cursor.fetchall():
        print("Uploaded:", result[0], "| Status:", result[6])


def load_customers(cursor):

    print("\nLoading customer data...")

    cursor.execute(f"""
        COPY INTO SMART_DATA_QUALITY_DB.BRONZE.CUSTOMERS
        (
            CUSTOMER_ID,
            CUSTOMER_NAME,
            EMAIL,
            CREATED_AT,
            SOURCE_FILE,
            INGESTED_AT
        )
        FROM (
            SELECT
                $1::VARCHAR,
                $2::VARCHAR,
                $3::VARCHAR,
                CURRENT_TIMESTAMP()::TIMESTAMP_NTZ,
                METADATA$FILENAME,
                CURRENT_TIMESTAMP()::TIMESTAMP_NTZ
            FROM @{STAGE}
        )
        FILE_FORMAT = (
            TYPE = CSV
            SKIP_HEADER = 1
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        )
        PATTERN = '.*customers_1000[.]csv[.]gz'
        ON_ERROR = 'ABORT_STATEMENT'
        FORCE = FALSE
    """)

    for row in cursor.fetchall():
        print("Customer load:", row)


def load_transactions(cursor):

    print("\nLoading transaction data...")

    cursor.execute(f"""
        COPY INTO SMART_DATA_QUALITY_DB.BRONZE.RAW_TRANSACTIONS
        (
            EVENT_ID,
            CUSTOMER_ID,
            EVENT_TYPE,
            AMOUNT,
            EVENT_TIMESTAMP,
            SOURCE_FILE,
            RUN_ID,
            INGESTED_AT,
            RAW_PAYLOAD
        )
        FROM (
            SELECT
                $1:event_id::VARCHAR,
                $1:customer_id::VARCHAR,
                $1:event_type::VARCHAR,
                TRY_TO_DECIMAL($1:amount::VARCHAR, 18, 2),
                TRY_TO_TIMESTAMP_NTZ(
                    $1:event_timestamp::VARCHAR
                ),
                'transactions_100k.json',
                '{RUN_ID}',
                CURRENT_TIMESTAMP()::TIMESTAMP_NTZ,
                TO_VARCHAR($1)
            FROM @{STAGE}
        )
        FILE_FORMAT = (TYPE = JSON)
        PATTERN = '.*transactions_100k[.]json[.]gz'
        ON_ERROR = 'ABORT_STATEMENT'
        FORCE = FALSE
    """)

    for row in cursor.fetchall():
        print("Transaction load:", row)


def verify_load(cursor):

    print("\n========== BULK LOAD VERIFICATION ==========")

    cursor.execute(f"""
        SELECT COUNT(*)
        FROM SMART_DATA_QUALITY_DB.BRONZE.RAW_TRANSACTIONS
        WHERE RUN_ID = '{RUN_ID}'
    """)

    transaction_count = cursor.fetchone()[0]

    cursor.execute("""
        SELECT COUNT(DISTINCT CUSTOMER_ID)
        FROM SMART_DATA_QUALITY_DB.BRONZE.CUSTOMERS
        WHERE SOURCE_FILE LIKE '%customers_1000.csv%'
    """)

    customer_count = cursor.fetchone()[0]

    print("Bulk transactions loaded:", transaction_count)
    print("Distinct bulk customers:", customer_count)

    print("============================================")

    if transaction_count != 100000:
        raise RuntimeError(
            f"Expected 100000 transactions, found {transaction_count}"
        )

    if customer_count != 1000:
        raise RuntimeError(
            f"Expected 1000 customers, found {customer_count}"
        )


def main():

    connection = get_snowflake_connection()
    cursor = connection.cursor()

    try:

        cursor.execute(
            f"CREATE STAGE IF NOT EXISTS {STAGE}"
        )

        print("\nUploading generated files...")

        upload_file(cursor, CUSTOMER_FILE)
        upload_file(cursor, TRANSACTION_FILE)

        load_customers(cursor)
        load_transactions(cursor)

        connection.commit()

        verify_load(cursor)

        print("\nBULK LOAD COMPLETED SUCCESSFULLY.")

    except Exception:

        connection.rollback()
        raise

    finally:

        cursor.close()
        connection.close()


if __name__ == "__main__":
    main()
