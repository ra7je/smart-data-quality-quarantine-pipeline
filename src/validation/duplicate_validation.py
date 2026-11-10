
from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import col, row_number, lit


def standardize_columns(dataframe: DataFrame) -> DataFrame:
    for column in dataframe.columns:
        if column != column.upper():
            dataframe = dataframe.withColumnRenamed(
                column, column.upper()
            )
    return dataframe


def validate_duplicates(
    transactions_df: DataFrame,
    existing_transactions_df: DataFrame = None
):
    transactions_df = standardize_columns(transactions_df)

    missing_id_df = (
        transactions_df
        .filter(col("EVENT_ID").isNull() | (col("EVENT_ID") == ""))
        .withColumn("DUPLICATE_ERROR", lit("EVENT_ID is missing"))
    )

    valid_id_df = transactions_df.filter(
        col("EVENT_ID").isNotNull()
        & (col("EVENT_ID") != "")
    )

    window_spec = Window.partitionBy("EVENT_ID").orderBy(
        col("SOURCE_FILE").asc(),
        col("EVENT_TIMESTAMP").asc_nulls_first()
    )

    ranked_df = valid_id_df.withColumn(
        "ROW_NUMBER",
        row_number().over(window_spec)
    )

    batch_duplicates_df = (
        ranked_df
        .filter(col("ROW_NUMBER") > 1)
        .drop("ROW_NUMBER")
        .withColumn(
            "DUPLICATE_ERROR",
            lit("Duplicate EVENT_ID within input batch")
        )
    )

    unique_batch_df = ranked_df.filter(
        col("ROW_NUMBER") == 1
    ).drop("ROW_NUMBER")

    if existing_transactions_df is not None:
        existing_transactions_df = standardize_columns(
            existing_transactions_df
        )

        existing_ids_df = (
            existing_transactions_df
            .select("EVENT_ID")
            .filter(col("EVENT_ID").isNotNull())
            .distinct()
        )

        new_transactions_df = unique_batch_df.join(
            existing_ids_df,
            on="EVENT_ID",
            how="left_anti"
        )

        already_processed_df = (
            unique_batch_df
            .join(existing_ids_df, on="EVENT_ID", how="inner")
            .withColumn(
                "DUPLICATE_ERROR",
                lit("EVENT_ID already exists in Silver")
            )
        )

    else:
        new_transactions_df = unique_batch_df

        already_processed_df = (
            unique_batch_df.limit(0)
            .withColumn(
                "DUPLICATE_ERROR",
                lit("EVENT_ID already exists in Silver")
            )
        )

    rejected_df = (
        missing_id_df
        .unionByName(batch_duplicates_df, allowMissingColumns=True)
        .unionByName(already_processed_df, allowMissingColumns=True)
    )

    return new_transactions_df, rejected_df
