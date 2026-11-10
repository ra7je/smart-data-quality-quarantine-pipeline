
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, lit, trim, when, concat_ws, upper
)


def standardize_columns(dataframe: DataFrame) -> DataFrame:
    for column in dataframe.columns:
        if column != column.upper():
            dataframe = dataframe.withColumnRenamed(
                column, column.upper()
            )
    return dataframe


def validate_transactions(
    transactions_df: DataFrame,
    customers_df: DataFrame
):
    transactions_df = standardize_columns(transactions_df)
    customers_df = standardize_columns(customers_df)

    customer_lookup = (
        customers_df
        .select(col("CUSTOMER_ID").alias("_MATCH_CUSTOMER_ID"))
        .filter(col("_MATCH_CUSTOMER_ID").isNotNull())
        .distinct()
    )

    joined_df = transactions_df.join(
        customer_lookup,
        transactions_df["CUSTOMER_ID"]
        == customer_lookup["_MATCH_CUSTOMER_ID"],
        "left"
    )

    customer_missing = (
        col("CUSTOMER_ID").isNull()
        | (trim(col("CUSTOMER_ID")) == "")
    )

    customer_not_found = (
        ~customer_missing
        & col("_MATCH_CUSTOMER_ID").isNull()
    )

    event_id_missing = (
        col("EVENT_ID").isNull()
        | (trim(col("EVENT_ID")) == "")
    )

    invalid_event_type = (
        col("EVENT_TYPE").isNull()
        | (~upper(trim(col("EVENT_TYPE"))).isin("CREDIT", "DEBIT"))
    )

    amount_missing = col("AMOUNT").isNull()

    invalid_amount = (
        col("AMOUNT").isNotNull()
        & (col("AMOUNT") <= 0)
    )

    timestamp_missing = col("EVENT_TIMESTAMP").isNull()

    validated_df = joined_df.withColumn(
        "VALIDATION_ERRORS",
        concat_ws(
            "; ",
            when(event_id_missing, lit("EVENT_ID is missing")),
            when(customer_missing, lit("CUSTOMER_ID is missing")),
            when(customer_not_found, lit("CUSTOMER_ID does not exist")),
            when(invalid_event_type, lit("Invalid EVENT_TYPE")),
            when(amount_missing, lit("AMOUNT is NULL")),
            when(invalid_amount, lit("AMOUNT must be greater than zero")),
            when(timestamp_missing, lit("EVENT_TIMESTAMP is missing or invalid"))
        )
    ).drop("_MATCH_CUSTOMER_ID")

    valid_df = (
        validated_df
        .filter(col("VALIDATION_ERRORS") == "")
        .drop("VALIDATION_ERRORS")
    )

    quarantine_df = validated_df.filter(
        col("VALIDATION_ERRORS") != ""
    )

    return valid_df, quarantine_df
