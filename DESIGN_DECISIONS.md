# Design Decisions – Smart Data Quality & Quarantine Pipeline

## 1. Overview

This document explains the main technical decisions I made while developing the Smart Data Quality & Quarantine Pipeline.

My main focus was to keep the pipeline modular, maintain data traceability, separate invalid records from valid records, and support recovery when corrected records become available.

## 2. Layered Data Architecture

**Decision:** Use Bronze, Silver and Gold layers.

**Reason:**

I separated the data into different layers so that raw ingestion, validation and analytical processing do not become mixed together.

- Bronze preserves incoming source data.
- Silver contains validated transaction records.
- Gold contains data prepared for reporting and analytics.
- Quarantine keeps rejected records separate from valid data.

This structure makes the data flow easier to understand and troubleshoot.

## 3. Snowflake for Data Storage and Processing

**Decision:** Use Snowflake as the central data platform.

**Reason:**

Snowflake provides centralized storage and SQL-based processing for the pipeline.

I use separate schemas for Bronze, Silver, Gold, Audit and dbt staging. This keeps the different processing responsibilities organized.

I also use Snowflake SQL for bulk transaction processing.

## 4. Python for Pipeline Components

**Decision:** Use Python to implement the pipeline logic.

**Reason:**

Python makes it easier to separate the ingestion, validation, transformation, replay and monitoring components into individual modules.

Instead of maintaining all the logic in one large script, I organized the project into smaller components.

## 5. Separate Validation Modules

**Decision:** Keep schema, business and duplicate validation in separate modules.

**Reason:**

Each validation type has a different responsibility.

- Schema validation checks required transaction information.
- Business validation checks customer references, amounts and transaction types.
- Duplicate validation identifies repeated transaction identifiers.

This separation makes the validation logic easier to maintain and test.

## 6. Quarantine Instead of Dropping Invalid Records

**Decision:** Store rejected records in a separate quarantine table.

**Reason:**

Dropping invalid records would make investigation and correction difficult.

By retaining rejected records and their validation information, I can identify the reason for rejection and process corrected records through the replay workflow.

## 7. Controlled Replay

**Decision:** Provide a separate replay workflow for corrected records.

**Reason:**

When a rejected transaction is corrected, processing the entire source dataset again is unnecessary.

The replay component allows selected records to be submitted for validation again and tracks their processing outcomes.

## 8. SQL-Based Bulk Processing

**Decision:** Use Snowflake SQL for the large-volume processing path.

**Reason:**

The project includes a 100,000-transaction test dataset.

Using SQL operations inside Snowflake reduces the need to move large datasets repeatedly between the application and the database.

The bulk pipeline uses SQL-based classification and loading operations.

## 9. dbt for Downstream Transformations

**Decision:** Use dbt to organize downstream SQL models and tests.

**Reason:**

I wanted transformation logic to be maintained separately from Python ingestion and validation code.

The dbt project contains staging and analytical models, along with schema and data tests.

## 10. Audit and Monitoring

**Decision:** Maintain separate audit and monitoring components.

**Reason:**

A pipeline should provide information about what happened during execution, not just produce final tables.

The audit and monitoring components help track execution status, processing counts and reconciliation results.

They also support investigation when processing does not complete as expected.

## 11. Synthetic Data for Testing

**Decision:** Generate synthetic customer and transaction datasets.

**Reason:**

A controlled dataset makes it possible to test both normal and invalid transaction scenarios.

The large-volume dataset contains 100,000 transactions and deliberately introduced data quality issues.

This provides a repeatable way to examine validation and bulk-processing behaviour.

## 12. Modular Project Structure

**Decision:** Organize the code into separate folders for ingestion, validation, transformation, quarantine, Snowflake, orchestration and monitoring.

**Reason:**

A modular structure makes it easier to locate individual responsibilities, understand execution flow and maintain the project as additional functionality is introduced.

## 13. Summary

The main design decisions were driven by five goals:

1. Preserve incoming source data.
2. Identify invalid records before analytical processing.
3. Retain rejected records for investigation and correction.
4. Support controlled replay and pipeline monitoring.
5. Produce reliable downstream analytical data.

These decisions form the foundation of the Smart Data Quality & Quarantine Pipeline.