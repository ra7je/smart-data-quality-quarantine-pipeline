from src.snowflake.connection import get_snowflake_connection

try:
    connection = get_snowflake_connection()

    cursor = connection.cursor()

    cursor.execute("""
        SELECT
            CURRENT_DATABASE(),
            CURRENT_SCHEMA(),
            CURRENT_WAREHOUSE(),
            CURRENT_USER()
    """)

    result = cursor.fetchone()

    print("Snowflake connection successful!")
    print("Database:", result[0])
    print("Schema:", result[1])
    print("Warehouse:", result[2])
    print("User:", result[3])

    cursor.close()
    connection.close()

except Exception as error:
    print("Connection failed:", error)
