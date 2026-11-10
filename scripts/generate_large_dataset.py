
import csv
import json
from pathlib import Path
from datetime import datetime, timedelta
from decimal import Decimal


# Project root
PROJECT_ROOT = Path(__file__).resolve().parent.parent

OUTPUT_DIR = PROJECT_ROOT / "data" / "sample"

CUSTOMER_FILE = OUTPUT_DIR / "customers_1000.csv"
TRANSACTION_FILE = OUTPUT_DIR / "transactions_100k.json"

CUSTOMER_COUNT = 1000
TRANSACTION_COUNT = 100_000


def generate_customers():

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    with CUSTOMER_FILE.open(
        "w", newline="", encoding="utf-8"
    ) as file:

        writer = csv.writer(file)

        writer.writerow([
            "customer_id",
            "customer_name",
            "email"
        ])

        for i in range(1, CUSTOMER_COUNT + 1):

            writer.writerow([
                f"C{i:04d}",
                f"Customer_{i}",
                f"customer{i}@example.com"
            ])

    print(f"Customers generated: {CUSTOMER_COUNT}")


def generate_transactions():

    base_time = datetime(2026, 11, 10, 9, 0, 0)

    previous_record = None

    with TRANSACTION_FILE.open(
        "w", encoding="utf-8"
    ) as file:

        for i in range(1, TRANSACTION_COUNT + 1):

            # Create intentional duplicate records.
            if i % 1000 == 0:

                record = previous_record.copy()

            else:

                customer_id = f"C{((i - 1) % CUSTOMER_COUNT) + 1:04d}"

                amount = Decimal(
                    f"{100 + (i % 900)}.00"
                )

                # Inject separate business-quality failures.
                if i % 1000 == 1:
                    customer_id = "C9999"

                elif i % 1000 == 2:
                    amount = Decimal("-50.00")

                elif i % 1000 == 3:
                    customer_id = None

                record = {
                    "event_id": f"TXN{i:06d}",
                    "customer_id": customer_id,
                    "event_type": (
                        "CREDIT" if i % 2 == 0 else "DEBIT"
                    ),
                    "amount": float(amount),
                    "event_timestamp": (
                        base_time + timedelta(minutes=i)
                    ).isoformat()
                }

            file.write(
                json.dumps(record) + "\n"
            )

            previous_record = record

    print(f"Transactions generated: {TRANSACTION_COUNT}")


def main():

    print("\nLARGE DATASET GENERATION STARTED")

    generate_customers()
    generate_transactions()

    print("\nGENERATION COMPLETED")
    print("Customer file:", CUSTOMER_FILE)
    print("Transaction file:", TRANSACTION_FILE)
    print("Transaction count:", TRANSACTION_COUNT)

    print("\nExpected injected quality issues:")
    print("Unknown customer IDs: 100")
    print("Negative amounts: 100")
    print("Missing customer IDs: 100")
    print("Duplicate records: 100")


if __name__ == "__main__":
    main()
