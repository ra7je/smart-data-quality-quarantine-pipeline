
from src.ingestion.bronze_ingestion import create_spark_session
from src.snowflake.connection import get_snowflake_connection

from src.transformation.silver_transformation import (
    run_silver_validation
)

from src.transformation.snowflake_publisher import (
    publish_to_snowflake
)


def main():

    spark = None
    connection = None

    try:
        spark = create_spark_session()
        connection = get_snowflake_connection()

        # Run validation.
        (
            valid_df,
            business_rejected_df,
            duplicate_rejected_df
        ) = run_silver_validation(spark, connection)

        # Publish results into Snowflake.
        publish_to_snowflake(
            valid_df,
            business_rejected_df,
            duplicate_rejected_df,
            connection
        )

        print("\nSILVER PIPELINE COMPLETED SUCCESSFULLY.")

    except Exception as error:
        print("\nPIPELINE FAILED:", str(error))
        raise

    finally:
        if connection is not None:
            connection.close()

        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()
