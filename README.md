# Smart Data Quality & Quarantine Pipeline

**YottaFlex.ai Hackathon Project**

## About the Project

I built this project to handle data quality problems that commonly occur when processing large transaction datasets.

For example, a transaction file might contain a missing customer ID, an incorrect transaction amount, an unknown customer, or the same transaction more than once. If these records are processed without checking, they can create problems in the final reports.

In this project, I have built a pipeline that checks incoming data before it reaches the reporting layer. Valid records move forward for further processing, while records with errors are stored separately so they can be investigated and corrected.

I also worked on a replay mechanism for corrected records, batch processing, audit tracking, Snowflake transformations, dbt models and pipeline monitoring.

The project uses Python, PySpark, Snowflake, SQL and dbt.

## How the Pipeline Works

The pipeline follows a Bronze, Silver and Gold structure.

```text
Customer CSV / Transaction JSON
              |
              v
         Data Ingestion
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
       Duplicate Detection
              |
          +---+---+
          |       |
          v       v
        Valid   Invalid
          |       |
          v       v
        Silver  Quarantine
          |       |
          |    Correction
          |       |
          |     Replay
          |       |
          +---+---+
              |
              v
           Gold Layer
              |
              v
          dbt Models
              |
              v
       Testing & Monitoring
```

I kept the raw, validated and analytical data separate so that each stage has its own responsibility.

## Technologies

| Technology | Where it is used |
|---|---|
| Python | Validation logic, ingestion and orchestration |
| PySpark | DataFrame processing and validation |
| Snowflake | Data storage and bulk processing |
| SQL | Data classification and transformations |
| dbt | Analytical models and data tests |
| Pandas | Supporting data operations |
| GitHub | Source code management |

## 1. Data Ingestion

I prepared two types of source data: customer information in CSV format and transaction information in JSON format.

These files are loaded into Snowflake's Bronze layer. For the larger dataset, I used Snowflake stages and `COPY INTO` rather than loading each record individually.

The main files involved are:

- `src/ingestion/bronze_ingestion.py`
- `scripts/generate_large_dataset.py`
- `scripts/bulk_load_to_snowflake.py`

## 2. Data Quality Validation

After loading the data, the pipeline checks whether the records meet the required conditions.

I separated the validation logic into three modules.

**Schema validation**

Checks the structure of the incoming data, including required fields and expected data types.

**Business validation**

Checks the transaction values against the business rules.

| Field | Validation |
|---|---|
| EVENT_ID | Cannot be null |
| CUSTOMER_ID | Cannot be null |
| CUSTOMER_ID | Must exist in customer master data |
| EVENT_TYPE | Must be CREDIT or DEBIT |
| AMOUNT | Must be greater than zero |
| EVENT_TIMESTAMP | Cannot be null |

**Duplicate validation**

Checks whether an event ID is repeated within the incoming batch or already exists in the Silver layer.

The validation code is maintained in:

- `src/validation/schema_validation.py`
- `src/validation/business_validation.py`
- `src/validation/duplicate_validation.py`

The validation modules provide the foundation for applying additional data quality rules, including completeness, uniqueness, referential integrity and range checks.

## 3. Bronze, Silver and Gold

I used three layers to organize the data.

- **Bronze:** Keeps the raw customer and transaction records.
- **Silver:** Contains validated transactions and the quarantine records.
- **Gold:** Contains the final transaction data and daily summaries used for reporting.

This separation helps keep the processing flow organized and makes it easier to trace data between stages.

## 4. Quarantine and Replay

One thing I wanted to avoid was simply deleting records that failed validation.

For that reason, I created a quarantine table in Snowflake. When a transaction fails validation, the original payload and its error information are stored there.

The quarantine table maintains:

- Quarantine ID.
- Event ID and customer ID.
- Original record payload.
- Error reason.
- Source file and run ID.
- Processing status.
- Replay count.
- Created and updated timestamps.

The main table is `SILVER.QUARANTINE_RECORDS`.

Once a rejected record is corrected, the replay process validates it again and processes it if it meets the required conditions.

Replay information is maintained in `AUDIT.REPLAY_HISTORY`.

The related Python files are:

- `src/quarantine/replay.py`
- `src/quarantine/replay_runner.py`

## 5. Processing 100,000 Transactions

I also wanted to test the pipeline with a larger dataset instead of depending only on a few sample records.

I generated 1,000 customer records and 100,000 transaction records. The transaction generator deliberately introduces different data quality issues.

| Record category | Expected count |
|---|---:|
| Total transactions | 100,000 |
| Valid transactions | 99,600 |
| Business-invalid transactions | 300 |
| Duplicate transactions | 100 |
| Total rejected transactions | 400 |

The invalid records include unknown customers, missing customer IDs and negative amounts. Duplicate event IDs are also included.

I used Snowflake bulk loading to ingest this dataset. The bulk Silver processing uses SQL inside Snowflake to classify records, merge valid transactions and insert rejected records into quarantine.

The main files are:

- `scripts/generate_large_dataset.py`
- `scripts/bulk_load_to_snowflake.py`
- `scripts/benchmark_100k_validation.py`
- `src/transformation/bulk_silver_pipeline.py`

## 6. Snowflake Database Design

The project uses the database `SMART_DATA_QUALITY_DB`.

I separated the database into schemas based on the different stages of processing.

| Schema | What it contains |
|---|---|
| BRONZE | Raw customer and transaction data |
| SILVER | Validated transactions and quarantine records |
| GOLD | Analytical tables |
| AUDIT | Pipeline runs and replay history |
| DBT_STAGING | dbt staging models |
| DBT_GOLD | dbt analytical models |

Some of the main tables are:

- `BRONZE.CUSTOMERS`
- `BRONZE.RAW_TRANSACTIONS`
- `SILVER.VALID_TRANSACTIONS`
- `SILVER.QUARANTINE_RECORDS`
- `GOLD.FACT_TRANSACTIONS`
- `GOLD.DAILY_TRANSACTION_SUMMARY`
- `AUDIT.PIPELINE_RUNS`
- `AUDIT.REPLAY_HISTORY`

## 7. Gold Layer and Daily Reports

After the Silver processing, I created two tables for analytical reporting.

**GOLD.FACT_TRANSACTIONS**

Stores individual transaction details, including event ID, customer ID, transaction type, amount, timestamp, source file and run ID.

**GOLD.DAILY_TRANSACTION_SUMMARY**

Provides daily transaction information:

- Total number of transactions.
- Total transaction amount.
- Total credit amount.
- Total debit amount.

This allows daily transaction activity to be analysed without repeatedly querying and aggregating the raw source records.

The related SQL files are:

- `sql/gold/fact_transactions.sql`
- `sql/gold/daily_transaction_summary.sql`
- `sql/gold/refresh_bulk_gold.sql`

## 8. dbt Integration

I integrated dbt with Snowflake to organize the analytical models and run tests on the transformed data.

The project contains staging and mart models.

Staging:
- `stg_valid_transactions.sql`

Marts:
- `dim_fact_transactions.sql`
- `daily_transaction_summary.sql`

I also added source definitions and schema tests.

To execute the dbt project:

```powershell
cd dbt_project
dbt debug
dbt run
dbt test
```

## 9. Pipeline Monitoring and Recovery

I added monitoring to check whether the records are moving correctly between the different layers.

The health monitor checks record counts in Bronze, Silver, Quarantine and Gold. It also checks reconciliation, duplicate records and pipeline audit status.

I created a recovery controller to check the status of a batch before deciding whether it needs to be processed again.

The recovery tests cover four situations:

1. A completed batch should not be processed again.
2. A partially processed batch should not be blindly reprocessed.
3. A failed processing attempt should be eligible for retry.
4. A failure should be recorded when retry attempts are exhausted.

The recovery controller scenarios were tested using mocked processing operations.

The main files are:

- `src/monitoring/pipeline_health_monitor.py`
- `src/monitoring/pipeline_recovery.py`
- `sql/audit/`

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
│   ├── test_pipeline_recovery.py
│   └── test_recovery_scenarios.py
│
├── docs/
├── main.py
├── requirements.txt
├── .gitignore
└── README.md
```

## 11. Installation

### Requirements

- Python 3.10 or later.
- Snowflake account.
- Git.
- dbt Core with the Snowflake adapter.

### Clone the repository

```powershell
git clone https://github.com/ra7je/smart-data-quality-quarantine-pipeline.git
cd smart-data-quality-quarantine-pipeline
```

### Create a virtual environment

```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
```

### Install dependencies

```powershell
pip install -r requirements.txt
```

### Configure Snowflake

The connection requires the Snowflake account, username, password, warehouse, database and schema.

The Python connection asks for the password locally. The dbt profile reads the `SNOWFLAKE_PASSWORD` environment variable.

In PowerShell:

```powershell
$env:SNOWFLAKE_PASSWORD = Read-Host "Enter Snowflake password"
```

Keep credentials in your local environment. Do not upload passwords to GitHub.

## 12. Running the Project

Run these commands from the project root.

**Test the Snowflake connection**

```powershell
python -m src.snowflake.test_connection
```

**Generate the 100K dataset**

```powershell
python scripts/generate_large_dataset.py
```

**Load the dataset into Snowflake**

```powershell
python scripts/bulk_load_to_snowflake.py
```

**Run the validation benchmark**

```powershell
python scripts/benchmark_100k_validation.py
```

**Run recovery tests**

```powershell
python -m unittest scripts.test_recovery_scenarios -v
```

**Run pipeline monitoring**

```powershell
python -m src.monitoring.pipeline_health_monitor
```

**Run dbt models and tests**

```powershell
cd dbt_project
dbt debug
dbt run
dbt test
```

The components should be executed in the correct pipeline order, using the configured batch ID.

## 13. Testing

During development, I tested the pipeline with sample transactions and the generated 100K dataset.

The following executions were reported successful:

| Test | Result |
|---|---|
| Snowflake connection | Successful |
| 100K bulk ingestion | Successful |
| Python validation tests | 5 passed in an earlier run |
| dbt execution | Successful |
| dbt tests | 10 passed in an earlier run |
| Recovery controller tests | 4 passed |
| Gold daily summary | Generated successfully |
| Completed-batch protection | Successful |

The dataset counts shown earlier are the expected counts from the generator. Actual processing counts should be taken from the Snowflake audit and reconciliation queries.

## AI Assistance and Development

I completed most of the project development myself, understanding the problem statement, planning the architecture, setting up the Snowflake database, developing the pipeline components, implementing validation logic, and working on the data transformations.

During development, I also used AI as a supporting tool to help with certain parts of the implementation. It helped me troubleshoot issues, improve existing code, complete some remaining components, prepare test scenarios, and review the overall pipeline.

I was responsible for understanding the requirements, making the technical decisions, integrating the components, running the code, and checking the results in my development environment.

AI assistance was mainly used during the following stages:

- Improving and troubleshooting existing Python and SQL code.
- Completing some remaining pipeline components.
- Reviewing validation and transformation logic.
- Working through quarantine, replay and recovery scenarios.
- Preparing additional test cases and project documentation.


## 15. Project Summary

The final pipeline brings together data ingestion, validation, duplicate detection, quarantine, replay, Snowflake transformations, dbt models and monitoring.

The main focus throughout development was to make rejected records traceable, keep validated data separate from raw data, and maintain information about what happened during each processing run.

**Project:** Smart Data Quality & Quarantine Pipeline

**Hackathon:** YottaFlex.ai

**Domain:** Data Engineering

**Technologies:** Python | PySpark | Snowflake | SQL | dbt
