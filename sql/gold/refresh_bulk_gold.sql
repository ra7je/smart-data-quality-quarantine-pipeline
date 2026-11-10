
-- =========================================================
-- SMART DATA QUALITY PIPELINE
-- BULK 100K GOLD REFRESH
-- =========================================================


-- =========================================================
-- STEP 1: MERGE SILVER TRANSACTIONS INTO GOLD FACT
-- =========================================================

MERGE INTO SMART_DATA_QUALITY_DB.GOLD.FACT_TRANSACTIONS target

USING (
    SELECT
        EVENT_ID,
        CUSTOMER_ID,
        EVENT_TYPE,
        AMOUNT,
        EVENT_TIMESTAMP,
        SOURCE_FILE,
        RUN_ID

    FROM SMART_DATA_QUALITY_DB.SILVER.VALID_TRANSACTIONS

    WHERE RUN_ID = 'BULK_100K_20261110'

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
);


-- =========================================================
-- STEP 2: REFRESH DAILY TRANSACTION SUMMARY
-- Uses the actual existing table columns.
-- =========================================================

MERGE INTO SMART_DATA_QUALITY_DB.GOLD.DAILY_TRANSACTION_SUMMARY target

USING (

    WITH affected_dates AS (

        SELECT DISTINCT
            TO_DATE(EVENT_TIMESTAMP) AS TRANSACTION_DATE

        FROM SMART_DATA_QUALITY_DB.GOLD.FACT_TRANSACTIONS

        WHERE RUN_ID = 'BULK_100K_20261110'

    )

    SELECT
        TO_DATE(f.EVENT_TIMESTAMP) AS TRANSACTION_DATE,

        COUNT(*) AS TOTAL_TRANSACTIONS,

        SUM(f.AMOUNT) AS TOTAL_AMOUNT,

        SUM(
            CASE
                WHEN f.EVENT_TYPE = 'CREDIT'
                THEN f.AMOUNT
                ELSE 0
            END
        ) AS TOTAL_CREDIT,

        SUM(
            CASE
                WHEN f.EVENT_TYPE = 'DEBIT'
                THEN f.AMOUNT
                ELSE 0
            END
        ) AS TOTAL_DEBIT

    FROM SMART_DATA_QUALITY_DB.GOLD.FACT_TRANSACTIONS f

    INNER JOIN affected_dates d
        ON TO_DATE(f.EVENT_TIMESTAMP) = d.TRANSACTION_DATE

    GROUP BY
        TO_DATE(f.EVENT_TIMESTAMP)

) source

ON target.TRANSACTION_DATE = source.TRANSACTION_DATE

WHEN MATCHED THEN UPDATE SET

    TOTAL_TRANSACTIONS = source.TOTAL_TRANSACTIONS,

    TOTAL_AMOUNT = source.TOTAL_AMOUNT,

    TOTAL_CREDIT = source.TOTAL_CREDIT,

    TOTAL_DEBIT = source.TOTAL_DEBIT

WHEN NOT MATCHED THEN INSERT (
    TRANSACTION_DATE,
    TOTAL_TRANSACTIONS,
    TOTAL_AMOUNT,
    TOTAL_CREDIT,
    TOTAL_DEBIT
)

VALUES (
    source.TRANSACTION_DATE,
    source.TOTAL_TRANSACTIONS,
    source.TOTAL_AMOUNT,
    source.TOTAL_CREDIT,
    source.TOTAL_DEBIT
);


-- =========================================================
-- STEP 3: VERIFY GOLD FACT TABLE
-- =========================================================

SELECT
    RUN_ID,
    COUNT(*) AS GOLD_TRANSACTION_COUNT,
    SUM(AMOUNT) AS TOTAL_AMOUNT

FROM SMART_DATA_QUALITY_DB.GOLD.FACT_TRANSACTIONS

WHERE RUN_ID = 'BULK_100K_20261110'

GROUP BY RUN_ID;


-- =========================================================
-- STEP 4: VERIFY DAILY TRANSACTION SUMMARY
-- =========================================================

SELECT
    TRANSACTION_DATE,
    TOTAL_TRANSACTIONS,
    TOTAL_AMOUNT,
    TOTAL_CREDIT,
    TOTAL_DEBIT

FROM SMART_DATA_QUALITY_DB.GOLD.DAILY_TRANSACTION_SUMMARY

ORDER BY TRANSACTION_DATE;


-- =========================================================
-- STEP 5: VERIFY CREDIT AND DEBIT RECONCILIATION
-- =========================================================

SELECT
    SUM(TOTAL_AMOUNT) AS TOTAL_AMOUNT,

    SUM(TOTAL_CREDIT) AS TOTAL_CREDIT,

    SUM(TOTAL_DEBIT) AS TOTAL_DEBIT,

    SUM(TOTAL_CREDIT + TOTAL_DEBIT) AS CREDIT_DEBIT_TOTAL,

    CASE
        WHEN SUM(TOTAL_AMOUNT) =
             SUM(TOTAL_CREDIT + TOTAL_DEBIT)
        THEN 'PASS'
        ELSE 'FAIL'
    END AS RECONCILIATION_STATUS

FROM SMART_DATA_QUALITY_DB.GOLD.DAILY_TRANSACTION_SUMMARY;
