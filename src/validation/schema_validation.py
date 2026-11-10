
from pyspark.sql import DataFrame


# ==================================================
# 1. EXPECTED BRONZE TRANSACTION SCHEMA
# ==================================================

EXPECTED_SCHEMA = {
    "EVENT_ID": ["string"],
    "CUSTOMER_ID": ["string"],
    "EVENT_TYPE": ["string"],
    "AMOUNT": ["decimal(18,2)", "decimal"],
    "EVENT_TIMESTAMP": ["timestamp", "timestamp_ntz"],
    "SOURCE_FILE": ["string"],
    "RUN_ID": ["string"],
    "INGESTED_AT": ["timestamp", "timestamp_ntz"],
    "RAW_PAYLOAD": ["string"]
}


# ==================================================
# 2. VALIDATE TRANSACTION DATAFRAME SCHEMA
# ==================================================

def validate_transaction_schema(
    dataframe: DataFrame
) -> dict:

    print("\nStarting transaction schema validation...")

    actual_fields = {
        field.name.upper(): field.dataType.simpleString().lower()
        for field in dataframe.schema.fields
    }

    expected_columns = set(EXPECTED_SCHEMA.keys())
    actual_columns = set(actual_fields.keys())

    missing_columns = sorted(
        expected_columns - actual_columns
    )

    unexpected_columns = sorted(
        actual_columns - expected_columns
    )

    type_mismatches = []

    # Check data types of existing expected columns.

    for column, allowed_types in EXPECTED_SCHEMA.items():

        if column not in actual_fields:
            continue

        actual_type = actual_fields[column]

        if not any(
            actual_type == allowed_type
            or (
                allowed_type == "decimal"
                and actual_type.startswith("decimal(")
            )
            for allowed_type in allowed_types
        ):

            type_mismatches.append({
                "column": column,
                "expected": allowed_types,
                "actual": actual_type
            })

    is_valid = (
        len(missing_columns) == 0
        and len(type_mismatches) == 0
    )

    result = {
        "is_valid": is_valid,
        "missing_columns": missing_columns,
        "unexpected_columns": unexpected_columns,
        "type_mismatches": type_mismatches
    }

    return result


# ==================================================
# 3. DISPLAY VALIDATION RESULTS
# ==================================================

def display_schema_validation(result: dict):

    print("\n" + "=" * 55)
    print("TRANSACTION SCHEMA VALIDATION REPORT")
    print("=" * 55)

    if result["is_valid"]:

        print("STATUS: PASSED")
        print("All required columns and data types are valid.")

    else:

        print("STATUS: FAILED")

    if result["missing_columns"]:

        print("\nMissing columns:")

        for column in result["missing_columns"]:
            print(" -", column)

    if result["type_mismatches"]:

        print("\nData type mismatches:")

        for mismatch in result["type_mismatches"]:

            print(
                f" - {mismatch['column']}: "
                f"Expected {mismatch['expected']}, "
                f"Received {mismatch['actual']}"
            )

    if result["unexpected_columns"]:

        print("\nAdditional columns:")

        for column in result["unexpected_columns"]:
            print(" -", column)

    print("=" * 55)

    return result["is_valid"]
