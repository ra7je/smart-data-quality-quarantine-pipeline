# Data Dictionary – Smart Data Quality & Quarantine Pipeline

## 1. Overview

This document describes the main Snowflake database objects used in the Smart Data Quality & Quarantine Pipeline.

**Database:** `SMART_DATA_QUALITY_DB`

The database is organized into five schemas:

| Schema | Purpose |
|---|---|
| BRONZE | Raw source data |
| SILVER | Validated and quarantined records |
| GOLD | Analytical and reporting tables |
| AUDIT | Pipeline execution and replay history |
| DBT_STAGING | dbt staging models and transformations |

## 2. Bronze Schema

### 2.1 BRONZE.CUSTOMERS

**Purpose:** Stores customer master data used to validate customer references in incoming transactions.

| Column | Description |
|---|---|
| Customer identifier | Identifies an individual customer |
| Customer attributes | Stores the available customer information from the source file |

Source: `customers.csv`

### 2.2 BRONZE.RAW_TRANSACTIONS

**Purpose:** Stores transaction records received from the source system before validation.

| Data field | Description |
|---|---|
| Transaction identifier | Identifies a transaction |
| Customer identifier | Links the transaction to a customer |
| Transaction amount | Represents the transaction value |
| Transaction type | Indicates CREDIT or DEBIT |
| Transaction timestamp | Records when the transaction occurred |

Source: `transactions.json`

## 3. Silver Schema

### 3.1 SILVER.VALID_TRANSACTIONS

**Purpose:** Stores transaction records that pass the implemented validation checks.

The table supports downstream transformations, reconciliation and reporting.

Important data attributes include transaction identity, customer reference, transaction amount, transaction type and transaction timestamp.

### 3.2 SILVER.QUARANTINE_RECORDS

**Purpose:** Stores rejected transaction records for investigation and correction.

The quarantine process retains rejected records and associated validation information.

The validation information helps identify why a record was rejected and supports the replay workflow.

## 4. Gold Schema

### 4.1 GOLD.FACT_TRANSACTIONS

**Purpose:** Stores transaction-level analytical data derived from validated records.

It is used as a downstream source for transaction reporting and analytical queries.

### 4.2 GOLD.DAILY_TRANSACTION_SUMMARY

**Purpose:** Stores daily aggregated transaction metrics.

| Column | Data type | Description |
|---|---|---|
| TRANSACTION_DATE | DATE | Date of the transactions |
| TOTAL_TRANSACTIONS | NUMBER | Total transaction count |
| TOTAL_AMOUNT | NUMBER | Total transaction amount |
| TOTAL_CREDIT | NUMBER | Total credit amount |
| TOTAL_DEBIT | NUMBER | Total debit amount |

## 5. Audit Schema

### 5.1 AUDIT.PIPELINE_RUNS

**Purpose:** Stores pipeline execution information.

The audit process tracks execution status and processing results, supporting monitoring and reconciliation.

### 5.2 AUDIT.REPLAY_HISTORY

**Purpose:** Tracks the outcomes of quarantined-record replay operations.

It supports investigation of replay attempts and their processing results.

## 6. dbt Models

The dbt project organizes downstream SQL transformations.

| Model | Purpose |
|---|---|
| `stg_valid_transactions` | Staging model for validated transactions |
| `dim_fact_transactions` | Downstream transaction model |
| `daily_transaction_summary` | Daily transaction aggregation |

The models are maintained under `dbt_project/models/`.

## 7. Data Quality Attributes

The pipeline checks transaction data using the following implemented validation conditions:

| Validation | Description |
|---|---|
| Required-field validation | Checks required transaction information |
| Customer reference validation | Checks whether the customer exists |
| Amount validation | Checks that the transaction amount is positive |
| Transaction-type validation | Accepts CREDIT and DEBIT |
| Duplicate validation | Identifies duplicate transaction identifiers |

## 8. Data Movement

| Source | Destination | Processing |
|---|---|---|
| Source customer file | BRONZE.CUSTOMERS | Customer ingestion |
| Source transaction file | BRONZE.RAW_TRANSACTIONS | Transaction ingestion |
| Bronze transactions | SILVER.VALID_TRANSACTIONS | Validation and classification |
| Bronze transactions | SILVER.QUARANTINE_RECORDS | Rejected-record storage |
| Silver transactions | GOLD.FACT_TRANSACTIONS | Analytical transformation |
| Silver transactions | GOLD.DAILY_TRANSACTION_SUMMARY | Daily aggregation |

## 9. Audit and Reconciliation

The pipeline uses audit and reconciliation checks to compare record counts and processing results across data layers.

The main checks cover:

- Source and processed record counts.
- Valid and quarantined record counts.
- Duplicate transaction identifiers.
- Pipeline execution status.
- Gold and Silver record reconciliation.

SQL files for audit and reconciliation are maintained under `sql/audit/`.

## 10. Naming Conventions

| Prefix or convention | Meaning |
|---|---|
| BRONZE | Raw source data |
| SILVER | Validated or quarantined data |
| GOLD | Reporting and analytical data |
| AUDIT | Execution and replay tracking |
| STG | Staging model |
| FACT | Transaction-level analytical data |
| DIM | Dimension-oriented model |

This document provides a reference for understanding the database objects and their roles in the transaction data quality pipeline.