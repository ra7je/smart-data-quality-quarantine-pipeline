# Tools and Workflow – Smart Data Quality & Quarantine Pipeline

## 1. Overview

This document explains the tools used in the project and the steps I follow to set up and execute the Smart Data Quality & Quarantine Pipeline.

## 2. Technology Stack

| Tool | Purpose |
|---|---|
| Python | Pipeline development and execution |
| PySpark | Data processing |
| Pandas | Dataset preparation and manipulation |
| Snowflake | Data storage and SQL processing |
| Snowflake SQL | Data loading and transformations |
| dbt | Data modelling and testing |
| Git | Version control |
| GitHub | Source code hosting |
| VS Code | Development environment |

## 3. Project Setup

### Step 1: Clone the repository

```bash
git clone https://github.com/ra7je/smart-data-quality-quarantine-pipeline.git
cd smart-data-quality-quarantine-pipeline
```

### Step 2: Create a Python virtual environment

```bash
python -m venv venv
```

Activate it on Windows PowerShell:

```powershell
.\venv\Scripts\Activate.ps1
```

### Step 3: Install dependencies

```bash
pip install -r requirements.txt
```

## 4. Snowflake Configuration

The project uses Snowflake as its central data platform.

The connection configuration includes:

| Parameter | Value |
|---|---|
| Account | Snowflake account identifier |
| User | Snowflake username |
| Warehouse | COMPUTE_WH |
| Database | SMART_DATA_QUALITY_DB |
| Schema | BRONZE |

The connection module is:

`src/snowflake/connection.py`

The Snowflake password is entered securely at runtime rather than stored directly in the source code.

### Test the connection

```bash
python -m src.snowflake.test_connection
```

## 5. Dataset Preparation

The project includes customer and transaction sample data.

The main source files are:

```text
data/
├── input/
│   ├── customers.csv
│   └── transactions.json
├── corrected/
│   └── transactions_corrected.json
└── sample/
    ├── customers_1000.csv
    └── transactions_100k.json
```

The synthetic dataset can be generated using:

```bash
python scripts/generate_large_dataset.py
```

## 6. Pipeline Execution

The pipeline is organized into separate components for ingestion, validation, transformation, quarantine and recovery.

### Step 1: Load source data

Use the ingestion component to load the source records into the Bronze layer.

### Step 2: Run validation and transformation

The validation modules check schema requirements, business conditions and duplicate transaction identifiers.

The transformation components classify records and publish valid and rejected data into their respective tables.

### Step 3: Run the bulk processing workflow

```bash
python -m src.transformation.bulk_silver_pipeline
```

### Step 4: Replay corrected records

```bash
python -m src.quarantine.replay_runner
```

The replay workflow processes corrected quarantined records.

### Step 5: Run monitoring

```bash
python -m src.monitoring.pipeline_health_monitor
```

### Step 6: Run recovery checks

```bash
python -m src.monitoring.pipeline_recovery
```

## 7. dbt Workflow

The dbt project contains staging and analytical SQL models.

Set the Snowflake password as an environment variable in PowerShell:

```powershell
$env:SNOWFLAKE_PASSWORD = Read-Host "Enter Snowflake password"
```

Navigate to the dbt project:

```bash
cd dbt_project
```

Check the connection:

```bash
dbt debug
```

Run models:

```bash
dbt run
```

Execute tests:

```bash
dbt test
```

Return to the project root:

```bash
cd ..
```

## 8. Testing Workflow

Run the Python test suite using:

```bash
python -m pytest
```

Run the large-dataset benchmark:

```bash
python scripts/benchmark_100k_validation.py
```

Run the recovery scenario tests:

```bash
python scripts/test_recovery_scenarios.py
```

## 9. SQL Verification

The SQL scripts are organized under:

```text
sql/
├── bronze/
├── silver/
├── gold/
└── audit/
```

They cover data loading, transformations, metrics, reconciliation, idempotency verification and pipeline health checks.

Execute the relevant SQL scripts in the Snowflake environment.

## 10. Version Control Workflow

The project uses Git for version control and GitHub for hosting.

Check changes:

```bash
git status
```

Stage changes:

```bash
git add .
```

Commit changes:

```bash
git commit -m "Update pipeline documentation and implementation"
```

Push changes:

```bash
git push origin main
```

## 11. Security Practices

- Keep passwords out of source code.
- Do not commit `.env` files or local credentials.
- Use environment variables or secure runtime prompts for connection details.
- Avoid publishing sensitive customer information.

## 12. Workflow Summary

The overall development and execution workflow is:

1. Set up the Python environment.
2. Install the required dependencies.
3. Configure Snowflake access.
4. Prepare the customer and transaction datasets.
5. Load source data into Bronze.
6. Execute validation and transformation.
7. Store rejected records in Quarantine.
8. Replay corrected records.
9. Build Gold reporting tables.
10. Run dbt models and tests.
11. Execute monitoring and reconciliation checks.
12. Commit and push project changes to GitHub.

This workflow provides a consistent way to set up, execute and verify the project.