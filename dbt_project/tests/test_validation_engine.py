
import pytest

from datetime import datetime
from decimal import Decimal

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DecimalType,
    TimestampType
)

from src.validation.schema_validation import (
    validate_transaction_schema
)

from src.validation.business_validation import (
    validate_transactions
)

from src.validation.duplicate_validation import (
    validate_duplicates
)


@pytest.fixture(scope="module")
def spark():

    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("DataQualityTests")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

    yield session

    session.stop()


@pytest.fixture
def transaction_schema():

    return StructType([
        StructField("EVENT_ID", StringType(), True),
        StructField("CUSTOMER_ID", StringType(), True),
        StructField("EVENT_TYPE", StringType(), True),
        StructField("AMOUNT", DecimalType(18, 2), True),
        StructField("EVENT_TIMESTAMP", TimestampType(), True),
        StructField("SOURCE_FILE", StringType(), True),
        StructField("RUN_ID", StringType(), True),
        StructField("INGESTED_AT", TimestampType(), True),
        StructField("RAW_PAYLOAD", StringType(), True)
    ])


def sample_transaction(
    event_id,
    customer_id,
    event_type="CREDIT",
    amount=Decimal("100.00")
):

    timestamp = datetime(2026, 11, 10, 10, 0, 0)

    return (
        event_id,
        customer_id,
        event_type,
        amount,
        timestamp,
        "transactions.json",
        "TEST_RUN",
        timestamp,
        '{"test": true}'
    )


@pytest.fixture
def customers_df(spark):

    schema = StructType([
        StructField("CUSTOMER_ID", StringType(), True)
    ])

    return spark.createDataFrame(
        [("C101",), ("C102",)],
        schema
    )


def test_schema_validation_passes(
    spark,
    transaction_schema
):

    df = spark.createDataFrame(
        [
            sample_transaction("TXN001", "C101")
        ],
        transaction_schema
    )

    result = validate_transaction_schema(df)

    assert result["is_valid"] is True
    assert result["missing_columns"] == []
    assert result["type_mismatches"] == []


def test_business_validation_rejects_invalid_records(
    spark,
    transaction_schema,
    customers_df
):

    df = spark.createDataFrame(
        [
            sample_transaction("TXN001", "C101"),
            sample_transaction("TXN002", "C999"),
            sample_transaction(
                "TXN003",
                "C101",
                amount=Decimal("-50.00")
            ),
            sample_transaction("TXN004", None)
        ],
        transaction_schema
    )

    valid_df, rejected_df = validate_transactions(
        df,
        customers_df
    )

    assert valid_df.count() == 1
    assert rejected_df.count() == 3


def test_duplicate_event_is_rejected(
    spark,
    transaction_schema
):

    df = spark.createDataFrame(
        [
            sample_transaction("TXN007", "C101"),
            sample_transaction("TXN007", "C101")
        ],
        transaction_schema
    )

    existing_schema = StructType([
        StructField("EVENT_ID", StringType(), True)
    ])

    existing_df = spark.createDataFrame(
        [],
        existing_schema
    )

    valid_df, rejected_df = validate_duplicates(
        df,
        existing_df
    )

    assert valid_df.count() == 1
    assert rejected_df.count() == 1


def test_empty_input(
    spark,
    transaction_schema,
    customers_df
):

    empty_df = spark.createDataFrame(
        [],
        transaction_schema
    )

    valid_df, rejected_df = validate_transactions(
        empty_df,
        customers_df
    )

    assert valid_df.count() == 0
    assert rejected_df.count() == 0


def test_already_processed_event_is_rejected(
    spark,
    transaction_schema
):

    df = spark.createDataFrame(
        [
            sample_transaction("TXN001", "C101")
        ],
        transaction_schema
    )

    existing_schema = StructType([
        StructField("EVENT_ID", StringType(), True)
    ])

    existing_df = spark.createDataFrame(
        [("TXN001",)],
        existing_schema
    )

    valid_df, rejected_df = validate_duplicates(
        df,
        existing_df
    )

    assert valid_df.count() == 0
    assert rejected_df.count() == 1
