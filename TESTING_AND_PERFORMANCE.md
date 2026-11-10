# Testing and Performance

## 1. Overview

I tested the Smart Data Quality & Quarantine Pipeline using automated Python tests, Snowflake verification queries, dbt tests and synthetic transaction datasets.

My testing approach covers individual validation functions, recovery scenarios, large-volume processing and data reconciliation.

## 2. Test Environment

| Component | Configuration |
|---|---|
| Language | Python |
| Test framework | pytest |
| Database | Snowflake |
| Data processing | PySpark |
| Transformation | dbt |
| Dataset | Synthetic transaction data |
| Development environment | Windows and VS Code |

## 3. Automated Test Suite

I organized the automated tests under the root-level `tests/` directory.

```text
tests/
├── __init__.py
├── test_validation_engine.py
└── test_recovery_scenarios.py
```

### 3.1 Validation Engine Tests

File: `tests/test_validation_engine.py`

The validation tests cover:

| Test case | Expected behaviour |
|---|---|
| Schema validation | Accept records with the required structure |
| Business validation | Reject records that violate business conditions |
| Duplicate event validation | Reject duplicate events |
| Empty input | Handle empty input correctly |
| Already-processed event | Reject an event that has already been processed |

**Result: 5 tests passed.**

### 3.2 Pipeline Recovery Tests

File: `tests/test_recovery_scenarios.py`

The recovery tests cover:

| Test case | Expected behaviour |
|---|---|
| Failed attempt | Allow a failed attempt to be retried |
| Failure after retries | Record the final failure |
| Partial batch | Avoid automatically reprocessing a partially processed batch |
| Successful batch | Skip an already successful batch |

**Result: 4 tests passed.**

## 4. Automated Test Execution

Run the complete root-level test suite:

```powershell
python -m pytest tests/ -v
```

### Verified test results

| Test file | Passed | Failed |
|---|---:|---:|
| test_validation_engine.py | 5 | 0 |
| test_recovery_scenarios.py | 4 | 0 |
| **Total** | **9** | **0** |

<badge color="success">9/9 passed</badge>

These results were obtained from the local test execution on November 10, 2026.

## 5. Small Dataset Testing

I used an eight-record transaction dataset to verify the initial validation and quarantine workflow.

| Category | Expected records |
|---|---:|
| Input transactions | 8 |
| Valid transactions | 4 |
| Business-invalid transactions | 3 |
| Duplicate transactions | 1 |
| Rejected transactions | 4 |

I also tested the replay workflow using a corrected transaction.

## 6. Large Dataset Testing

I generated a synthetic dataset to evaluate the pipeline with a larger transaction volume.

| Dataset | Records |
|---|---:|
| Customers | 1,000 |
| Transactions | 100,000 |

The generated transactions include deliberately introduced data quality issues.

### Expected distribution

| Category | Records |
|---|---:|
| Valid transactions | 99,600 |
| Business-invalid transactions | 300 |
| Duplicate transactions | 100 |
| Rejected transactions | 400 |

These figures describe the expected generated dataset distribution, not a separately measured benchmark result.

## 7. dbt Testing

The dbt project contains staging and analytical models with associated schema and data tests.

The previously recorded dbt test execution completed with:

**10 tests passed out of 10.**

## 8. Snowflake Reconciliation

The SQL verification scripts cover:

- Bronze, Silver and Quarantine record counts.
- Source-to-target reconciliation.
- Silver and Gold record comparisons.
- Duplicate transaction identifiers.
- Audit execution status.
- Batch processing metrics.

Relevant scripts are maintained under `sql/audit/`.

## 9. Recovery and Replay Testing

The recovery workflow is tested for successful batches, partial batches, retry scenarios and failure recording.

The corrected-record replay workflow is separately exercised using the quarantine components.

The automated recovery controller tests use controlled test scenarios to verify the expected decisions.

## 10. Performance Testing

The project includes:

`scripts/benchmark_100k_validation.py`

This script supports performance evaluation using the 100,000-record dataset.

Processing duration and throughput should be reported from the actual benchmark execution rather than estimated.

## 11. Test Commands

Run the automated tests:

```powershell
python -m pytest tests/ -v
```

Run the individual validation tests:

```powershell
python -m pytest tests/test_validation_engine.py -v
```

Run the recovery tests:

```powershell
python -m pytest tests/test_recovery_scenarios.py -v
```

Run the benchmark:

```powershell
python scripts/benchmark_100k_validation.py
```

Run dbt tests from the dbt project directory:

```powershell
cd dbt_project
dbt test
```

## 12. Conclusion

The automated Python test suite currently contains nine passing tests covering validation and recovery scenarios.

Additional verification is supported by the Snowflake SQL scripts, dbt tests and large-volume synthetic dataset.

The testing approach focuses on data correctness, rejected-record handling, recovery behaviour and consistency across the pipeline.