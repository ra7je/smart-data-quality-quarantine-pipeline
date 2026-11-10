# Smart Data Quality & Quarantine Pipeline

**YottaFlex.ai Hackathon Project**

## About the Project

One of the challenges in data engineering is dealing with incorrect data coming from different sources. Sometimes transaction files contain missing values, invalid customer IDs, negative amounts, or duplicate records. If we process everything without checking, these issues can affect the final reports.

I developed this project to handle those problems as part of the data pipeline itself.

The pipeline takes customer and transaction data from CSV and JSON files, loads it into Snowflake, checks the records against different validation rules, and separates clean records from records that need attention.

Instead of simply rejecting incorrect records, I also implemented a quarantine mechanism where those records are stored along with their error reasons. Once the issues are corrected, eligible records can be processed again using the replay functionality.

The clean data is then transformed into analytical tables, which can be used for reporting and further analysis.

## Technologies I Used

| Technology | What I used it for |
|---|---|
| Python | Building the pipeline and handling processing logic |
| PySpark | Working with transaction data and applying validation rules |
| Snowflake | Storing raw, clean, rejected, and analytical data |
| SQL | Data validation, transformations, and aggregations |
| dbt | Building and testing analytical models |
| Pandas | Supporting data processing |
| Git & GitHub | Managing project code |

## How the Project Works

The project follows a Bronze, Silver, and Gold architecture.

```text
       Customer CSV / Transaction JSON
                     |
                     v
                Bronze Layer
                     |
                     v
              Schema Validation
                     |
                     v
             Business Validation
                     |
                     v
              Duplicate Checking
                     |
               +-----+-----+
               |           |
               v           v
           Valid Data   Invalid Data
               |           |
               v           v
           Silver      Quarantine
               |           |
               |       Correction
               |           |
               |         Replay
               |           |
               +-----+-----+
                     |
                     v
                  Gold Layer
                     |
                     v
              Analytical Reports
                     |
                     v
                  dbt Tests
                     |
                     v
              Pipeline Monitoring
```

## 1. Data Ingestion

I started by preparing customer and transaction source files.

- Customer information is provided through CSV files.
- Transaction information is provided through JSON files.
- Raw records are loaded into Snowflake Bronze tables.
- For the larger dataset, I used Snowflake stages and `COPY INTO` for bulk loading.

The main ingestion files are:

- `src/ingestion/bronze_ingestion.py`
- `scripts/bulk_load_to_snowflake.py`
- `scripts/generate_large_dataset.py`

## 2. Data Validation

After ingestion, the records go through validation.

I divided validation into three main areas.

**Schema validation**

Checks whether the incoming records contain the required fields and expected data types.

**Business validation**

Checks whether the transaction information makes sense according to the business rules.

The rules include:

- Event ID should not be empty.
- Customer ID should not be empty.
- Customer ID must exist in the customer table.
- Event type should be either CREDIT or DEBIT.
- Transaction amount must be greater than zero.
- Transaction timestamp should be available.

**Duplicate validation**

Checks whether an event ID is repeated in the incoming data or already exists in the Silver layer.

Files used:

- `src/validation/schema_validation.py`
- `src/validation/business_validation.py`
- `src/validation/duplicate_validation.py`

## 3. Bronze, Silver and Gold Layers

I separated the data into different layers so that each stage has a clear responsibility.

**Bronze:** Stores the incoming raw customer and transaction records.

**Silver:** Stores records that have passed the required validation checks.

**Gold:** Contains the final transaction-level and aggregated data used for analytics.

This separation also makes it easier to identify where a data quality issue occurred.

## 4. Quarantine and Replay

One important part of this project is that invalid records are not simply discarded.

I created a separate quarantine table in Snowflake to store rejected records and their error reasons.

For example, if a transaction contains a negative amount or an unknown customer ID, the record is moved to quarantine.

After correcting the issue, the replay process can validate the record again and process it if it passes the required checks.

I also maintain replay history so that replay attempts and their outcomes can be tracked.

Main tables:

- `SILVER.QUARANTINE_RECORDS`
- `AUDIT.REPLAY_HISTORY`

Implementation:

- `src/quarantine/replay.py`
- `src/quarantine/replay_runner.py`

## 5. Processing 100,000 Transaction Records

I wanted to test the solution with more than just a few sample records, so I generated a larger dataset containing 100,000 transactions and 1,000 customer records.

I deliberately introduced data quality issues into the generated transactions.

| Record type | Expected count |
|---|---:|
| Total transactions | 100,000 |
| Valid transactions | 99,600 |
| Business-invalid records | 300 |
| Duplicate records | 100 |
| Total rejected records | 400 |

The dataset includes missing customer IDs, unknown customer IDs, negative amounts, and duplicate event IDs.

I used Snowflake bulk loading to load the dataset and SQL-based processing inside Snowflake for the bulk Silver transformation.

## 6. Gold Layer and Reporting

After processing, I created two main tables for analytical reporting.

**`GOLD.FACT_TRANSACTIONS`**

Contains transaction-level information such as event ID, customer ID, transaction type, amount, timestamp, source file, and batch ID.

**`GOLD.DAILY_TRANSACTION_SUMMARY`**

Provides daily transaction information, including:

- Total number of transactions.
- Total transaction amount.
- Total credit amount.
- Total debit amount.

This makes it easier to understand transaction activity without repeatedly processing the raw records.

## 7. dbt Integration

I also integrated dbt with Snowflake to manage the analytical transformations.

The dbt project contains staging models, mart models, source definitions, and data quality tests.

Some of the models include:

- `stg_valid_transactions.sql`
- `dim_fact_transactions.sql`
- `daily_transaction_summary.sql`

I used dbt to execute the models and verify the data through schema and data quality tests.

## 8. Pipeline Monitoring and Recovery

I added monitoring so that the pipeline can be checked after processing.

The health monitoring component checks the record counts across Bronze, Silver, Quarantine, and Gold. It also checks reconciliation, duplicate records, and pipeline audit status.

I also implemented a recovery controller to handle batch processing decisions.

The recovery logic avoids retrying a batch that already has a successful audit record. I tested the recovery controller using isolated unit tests covering successful batches, partial batches, retry attempts, and exhausted retries.

The recovery unit tests use mocked processing operations; actual recovery from a live Snowflake failure still requires separate failure-injection testing.

Main files:

- `src/monitoring/pipeline_health_monitor.py`
- `src/monitoring/pipeline_recovery.py`

## 9. Snowflake Database Structure

I organized the Snowflake database according to the different stages of the pipeline.

**Database:** `SMART_DATA_QUALITY_DB`

| Schema | Purpose |
|---|---|
| BRONZE | Raw customer and transaction data |
| SILVER | Validated transactions and quarantine records |
| GOLD | Final analytical tables |
| AUDIT | Pipeline runs and replay history |
| DBT_STAGING | dbt staging models |
| DBT_GOLD | dbt analytical models |

## 10. Project Structure

```text
YottaflextInOfficeHackthon/
│
├── data/
│   ├── input/
│   ├── corrected/
│   └── sample/
│
├── src/
│   ├── config/
│   ├── ingestion/
│   ├── validation/
│   ├── transformation/
│   ├── quarantine/
│   ├── snowflake/
│   ├── orchestration/
│   └── monitoring/
│
├── sql/
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   └── audit/
│
├── dbt_project/
│   ├── models/
│   │   ├── staging/
│   │   └── marts/
│   ├── tests/
│   └── dbt_project.yml
│
├── scripts/
│   ├── generate_large_dataset.py
│   ├── bulk_load_to_snowflake.py
│   ├── benchmark_100k_validation.py
│   └── test_recovery_scenarios.py
│
├── docs/
├── main.py
├── requirements.txt
├── .gitignore
└── README.md
```

## 11. How to Run the Project

### Prerequisites

- Python 3.10 or later.
- Snowflake account.
- Git.
- dbt Core with the Snowflake adapter.

### Clone the repository

```bash
git clone YOUR_GITHUB_REPOSITORY_URL
cd YottaflextInOfficeHackthon
```

### Create a virtual environment

```bash
python -m venv venv
```

Activate it on Windows:

```powershell
.\venv\Scripts\Activate.ps1
```

### Install the dependencies

```bash
pip install -r requirements.txt
```

### Configure Snowflake

Configure your Snowflake account credentials locally.

The connection requires an account, username, password, warehouse, database, and schema.

Do not upload passwords or other sensitive credentials to GitHub.

### Run the Snowflake connection test

```powershell
python -m src.snowflake.test_connection
```

### Generate the 100K dataset

```powershell
python scripts/generate_large_dataset.py
```

### Load data into Snowflake

```powershell
python scripts/bulk_load_to_snowflake.py
```

### Run the recovery tests

```powershell
python -m unittest scripts.test_recovery_scenarios -v
```

### Run the pipeline health monitor

```powershell
python -m src.monitoring.pipeline_health_monitor
```

### Run dbt

```powershell
cd dbt_project
dbt debug
dbt run
dbt test
```

Run each component according to the pipeline execution order and configured batch ID. Avoid rerunning a successfully processed production batch simply for testing.

## 12. Development and Testing

During development, I tested the project using both sample records and the generated 100K dataset.

The following executions were reported successful:

- Snowflake connection.
- Bulk ingestion of 100,000 transaction records.
- Python validation tests.
- dbt model execution.
- dbt tests (10 passed in an earlier execution).
- Recovery controller unit tests (4 passed).
- Gold daily transaction summary generation.
- Completed-batch retry protection.

The final health-monitor output and full end-to-end reconciliation should be checked before submission.

## 13. Future Improvements

There are still opportunities to extend this project.

Some improvements I would like to work on are:

- A dashboard to display data quality metrics.
- Automated notifications when a pipeline fails.
- Stronger batch-level idempotency and concurrent processing protection.
- Live failure-injection testing.
- Scheduled pipeline execution.
- Deployment to a cloud environment.

## 14. What I Learned

While building this project, I got practical experience working with different stages of a data engineering pipeline.

I learned how to load large datasets into Snowflake, apply validation rules, handle duplicates, manage rejected records, replay corrected data, build analytical models using dbt, and monitor pipeline execution.

More importantly, I understood how individual components such as Python, SQL, Snowflake, PySpark, and dbt work together in a complete data engineering solution.

---

**Project Name:** Smart Data Quality & Quarantine Pipeline

**Hackathon:** YottaFlex.ai

**Domain:** Data Engineering

**Technology Stack:** Python | PySpark | Snowflake | SQL | dbt