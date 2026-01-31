---
description: Workflow for writing comprehensive, readable, and focused tests
---

# Write Tests

This workflow guides you through creating tests that are comprehensive yet concise, with a focus on readability and self-explanatory naming, specifically tailored for the `pydbzengine` project.

1. **Analyze the Scope**
   - Identify the Debezium handler or component to test (e.g., `IcebergChangeHandlerV2`).
   - Determine critical paths: record consumption, schema evolution, metadata enrichment.
   - **Goal:** Verify that database changes are correctly reflected in the destination (e.g., Iceberg table).

2. **Top-Down Test Design**
   - Start with integration tests using `BasePostgresqlTest` (Postgres source) and specialized containers like `S3Minio` and `CatalogRestContainer`.
   - Use `MockChangeEvent` to simulate Debezium records for controlled testing of batch handling and edge cases.
   - **Guideline:** Focus on behavior: "Does this UPDATE event result in the correct value in the Iceberg table?"

3. **Naming Convention**
   - Format: `test_<feature>_<scenario>`
   - Keep it concise and descriptive.
   - *Good:* `test_schema_evolution_adds_column`
   - *Good:* `test_null_type_sanitization`
   - *Good:* `test_metadata_enrichment`

4. **Class-Based Structure (unittest style)**
   - Define a class grouping related tests (e.g., `class TestIcebergChangeHandlerV2(BasePostgresqlTest):`).
   - Use `setUp` and `tearDown` for container lifecycle management and environment initialization.
   - Prefer helper methods like `_get_handler_and_catalog` to reduce boilerplate across tests.

5. **Implement Clean and Focused Tests**
   - **Setup:** Create a unique namespace or table for the test to ensure isolation.
   - **Action:** Call `handler.handleJsonBatch(records)` with specific `MockChangeEvent` instances.
   - **Assertion:** Use `_wait_for_condition` for asynchronous or eventually consistent checks. Verify both data and metadata (e.g., `_consumed_at`, `_dbz_event_key`).

6. **Review for Readability and Value**
   - Ensure test names clearly state the scenario (e.g., `test_multiple_tables_batch`).
   - Remove redundant assertions.
   - Use `pprint_table` or similar helpers during development to debug data states.

7. **Validate**
   - Run the tests using `pytest`.
   - Ensure failure messages from `_wait_for_condition` are helpful and descriptive.

---

## Advanced Testing Strategies

### 1. Schema Evolution & Sanitization
- **Strategy**: Test how the handler reacts to changing data structures.
  - **New Columns**: Add a field to the JSON and verify the table schema updates.
  - **Null Handling**: Ensure `null` fields are sanitized (e.g., to `string` fallback) to avoid Iceberg schema conflicts.
  - **Complex Types**: Verify nested JSON objects are correctly handled as Iceberg structs.

### 2. Metadata & Key Handling
- **Strategy**: Verify the handler enriches records with required metadata.
  - Check `_dbz_event_key` and `_dbz_event_key_hash` for correctness.
  - Verify `_consumed_at` timestamps are present.
  - Test behavior with empty or missing keys.

### 3. Integration with Infrastructure
- **Strategy**: Use the provided container helpers in `tests/` to simulate a real environment.
  - `BasePostgresqlTest`: For source database interactions.
  - `S3Minio`: For object storage.
  - `CatalogRestContainer`: For Iceberg REST catalog.
