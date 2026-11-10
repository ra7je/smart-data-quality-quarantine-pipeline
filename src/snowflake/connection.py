import snowflake.connector
import getpass


def get_snowflake_connection():

    password = getpass.getpass("Enter Snowflake password: ")

    connection = snowflake.connector.connect(
        account="SP49612.ap-southeast-7.aws",
        user="MADUGULAAMANI",
        password=password,
        warehouse="COMPUTE_WH",
        database="SMART_DATA_QUALITY_DB",
        schema="BRONZE"
    )

    print("Snowflake connected successfully!")

    return connection