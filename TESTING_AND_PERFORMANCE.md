# Testing and Performance – Smart Data Quality & Quarantine Pipeline

## 1. Overview

I tested the pipeline using both a small sample dataset and a larger synthetic transaction dataset.

The purpose of testing was to check whether the pipeline could identify invalid records, separate valid and rejected transactions, process larger datasets and maintain consistent results across the data layers.

## 2. Test Environment

| Component | Technology |
|---|---|
| Programming language | Python |
| Data processing | PySpark |
| Database | Snowflake |
| SQL transformations | Snowflake SQL |
| Data modelling | dbt |
| Unit testing | Python unittest / pytest |
| Version control | Git and GitHub |

## 3. Small Dataset Testing

I started with a small dataset containing eight transactions.

The dataset included valid transactions, business validation failures and a duplicate record.

### Expected results

| Category | Records |
|---|---:|
| Total input records | 8 |
| Valid records | 4 |
| Business-invalid records | 3 |
| Duplicate records | 1 |
| Total rejected records | 4 |

### Processing result

The pipeline separated the valid records from rejected records and stored the rejected records in quarantine.

The replay workflow was also tested using a corrected transaction.

The corrected transaction passed validation and was published to Silver.

## 4. Large Dataset Testing

To test larger-volume processing, I generated a synthetic dataset with the following characteristics.

| Dataset | Records |
|---|---:|
| Customers | 1,000 |
| Transactions | 100,000 |

The transaction dataset includes deliberately introduced data quality issues.

### Expected transaction distribution

| Category | Records |
|---|---:|
| Valid transactions | 99,600 |
| Business-invalid transactions | 300 |
| Duplicate transactions | 100 |
| Total rejected transactions | 400 |
| Total input transactions | 100,000 |

These are the expected dataset counts based on the generation rules.

## 5. Data Quality Test Scenarios

| Test scenario | Expected behaviour |
|---|---|
| Missing required information | Reject the record |
| Unknown customer reference | Reject the record |
| Negative transaction amount | Reject the record |
| Invalid transaction type | Reject the record |
| Duplicate transaction identifier | Identify the duplicate |
| Corrected quarantined record | Revalidate through replay |
| Valid transaction | Publish to Silver |

## 6. dbt Testing

I used dbt to test downstream models and data quality conditions.

The dbt project contains staging and analytical models, along with schema and data tests.

**Previously recorded test result:**

<badge color="success">10/10 passed</badge>

The reported result was ten successful dbt tests.

## 7. Python Testing

Python tests were used to validate individual pipeline components and processing behaviour.

The test coverage includes:

- Validation logic.
- Transaction processing.
- Pipeline status checks.
- Recovery controller scenarios.

The recovery scenarios include successful-batch handling, partial-processing handling, retry behaviour and failure recording.

## 8. Reconciliation Testing

Reconciliation checks compare records across the different pipeline layers.

The main checks include:

1. Bronze record count.
2. Silver valid-record count.
3. Quarantine rejected-record count.
4. Bronze versus Silver plus Quarantine.
5. Silver versus Gold.
6. Duplicate transaction identifiers.
7. Audit execution status.
8. Audit counts against processing results.

The SQL verification scripts are maintained under `sql/audit/`.

## 9. Recovery Testing

The recovery component checks the batch execution status before deciding whether processing should run again.

The test scenarios cover:

- Already successful batches.
- Partially processed batches.
- Failed processing attempts.
- Retry handling.
- Failure recording after unsuccessful attempts.

The successful-batch scenario was tested to confirm that an already completed batch can be skipped instead of being unnecessarily reprocessed.

## 10. Performance Testing

The project includes a benchmark script for evaluating validation with the 100,000-record dataset.

File:

`scripts/benchmark_100k_validation.py`

The benchmark is intended to evaluate processing duration and large-volume validation behaviour.

Actual benchmark timings should be taken from the execution output for the test environment.

## 11. Test Evidence

The following project files support the testing and verification workflow:

| File | Purpose |
|---|---|
| `dbt_project/tests/test_validation_engine.py` | Python validation tests |
| `scripts/benchmark_100k_validation.py` | Large-dataset benchmark |
| `scripts/test_pipeline_recovery.py` | Recovery status test |
| `scripts/test_recovery_scenarios.py` | Recovery scenario tests |
| `sql/audit/data_quality_metrics.sql` | Data quality metrics |
| `sql/audit/reconciliation_checks.sql` | Reconciliation checks |
| `sql/audit/bulk_100k_verification.sql` | Bulk processing verification |
| `sql/audit/bulk_batch_reconciliation.sql` | Batch reconciliation |
| `sql/audit/idempotency_verification.sql` | Idempotency verification |
| `sql/audit/pipeline_health_check.sql` | Pipeline health checks |

## 12. Summary

Testing focuses on transaction validation, quarantine handling, corrected-record replay, large-volume processing, downstream dbt tests, reconciliation and recovery behaviour.

The combination of small sample testing and a 100,000-record synthetic dataset provides a way to evaluate the pipeline at both functional and larger data volumes.