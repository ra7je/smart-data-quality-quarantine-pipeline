
SELECT
    EVENT_ID,
    CUSTOMER_ID,
    EVENT_TYPE,
    AMOUNT,
    EVENT_TIMESTAMP,
    TRANSACTION_DATE,
    SOURCE_FILE,
    RUN_ID
FROM {{ ref('stg_valid_transactions') }}
