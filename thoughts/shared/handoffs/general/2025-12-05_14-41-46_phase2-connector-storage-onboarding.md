---
date: 2025-12-05T20:41:46+0000
researcher: Claude
git_commit: b90bccc686bdc7ee4c9d22283640b7f68ea2370f
branch: feature/phase1-shared-connectors-foundation
repository: GrowthNav
topic: "Phase 2 Connector Storage & Customer Onboarding Data Sources"
tags: [implementation, shared-connectors, shared-onboarding, data-sources, snowflake]
status: complete
last_updated: 2025-12-05
last_updated_by: Claude
type: implementation_strategy
---

# Handoff: Phase 2 Connector Storage & Customer Onboarding Data Sources

## Task(s)

**Implementation Plan:** `thoughts/shared/plans/2025-12-05-shared-connectors-implementation-plan.md`

### Completed Tasks (Phase 2 - Connector Storage Architecture)

1. **DataSourceConfig dataclass** - Created in orchestrator.py to configure data sources during onboarding
2. **Extended OnboardingRequest** - Added `data_sources: list[DataSourceConfig]` field
3. **CONFIGURING_DATA_SOURCES status** - Added new status to OnboardingStatus enum
4. **ConnectorStorage class** - Created BigQuery-backed storage for connector configurations
5. **Data source configuration step** - Integrated into OnboardingOrchestrator.onboard() method
6. **Tests** - 19 tests for ConnectorStorage, 10 tests for data source onboarding features
7. **Manual verification** - Tested with Nothing Bundt Cakes Snowflake data source

**Status:** Phase 2 is COMPLETE. All automated and manual verification passed.

## Critical References

- `thoughts/shared/plans/2025-12-05-shared-connectors-implementation-plan.md` - Main implementation plan (Phase 2 complete, Phase 3 next)
- `packages/shared-connectors/growthnav/connectors/storage.py` - New ConnectorStorage class
- `packages/shared-onboarding/growthnav/onboarding/orchestrator.py` - Updated with DataSourceConfig and data source configuration step

## Recent changes

- `packages/shared-connectors/growthnav/connectors/storage.py:1-388` - NEW: ConnectorStorage class for BigQuery-backed connector config storage
- `packages/shared-connectors/growthnav/connectors/__init__.py:48,68` - Export ConnectorStorage
- `packages/shared-onboarding/growthnav/onboarding/orchestrator.py:39-66` - NEW: DataSourceConfig dataclass
- `packages/shared-onboarding/growthnav/onboarding/orchestrator.py:81` - Added data_sources field to OnboardingRequest
- `packages/shared-onboarding/growthnav/onboarding/orchestrator.py:34` - Added CONFIGURING_DATA_SOURCES status
- `packages/shared-onboarding/growthnav/onboarding/orchestrator.py:110,139-141` - Added connector_storage parameter/property
- `packages/shared-onboarding/growthnav/onboarding/orchestrator.py:297-339` - Step 6: Configure data sources during onboarding
- `packages/shared-onboarding/growthnav/onboarding/orchestrator.py:354-416` - NEW: _configure_data_sources() method
- `packages/shared-connectors/tests/test_storage.py` - NEW: 19 tests for ConnectorStorage
- `packages/shared-onboarding/tests/test_orchestrator.py:737-1002` - NEW: TestDataSourceConfig and TestOnboardingOrchestratorDataSources classes

## Learnings

1. **Snowflake timestamp column**: The THANX_PURCHASES table uses `PURCHASED_AT` not `UPDATED_AT` for timestamps. Need to specify `timestamp_column` in connection_params.

2. **fetch_records returns a generator**: The SnowflakeConnector.fetch_records() returns a generator, need to convert to list before getting length.

3. **normalize expects a list**: The connector.normalize() method expects a list of records, not a single record.

4. **BigQuery num_dml_affected_rows can be None**: Need to use `(result.num_dml_affected_rows or 0) > 0` pattern for type safety.

5. **Schema is dict not list**: The get_schema() returns `{column_name: type}` dict, not a list of column objects.

## Artifacts

- `packages/shared-connectors/growthnav/connectors/storage.py` - NEW ConnectorStorage class
- `packages/shared-connectors/tests/test_storage.py` - NEW tests for ConnectorStorage
- `packages/shared-onboarding/growthnav/onboarding/orchestrator.py` - Updated with data source features
- `packages/shared-onboarding/growthnav/onboarding/__init__.py` - Updated exports
- `packages/shared-connectors/growthnav/connectors/__init__.py` - Updated exports
- `packages/shared-onboarding/tests/test_orchestrator.py` - Updated with data source tests
- `thoughts/shared/plans/2025-12-05-shared-connectors-implementation-plan.md` - Updated with completion checkmarks

## Action Items & Next Steps

1. **Proceed to Phase 3: LLM-Assisted Schema Discovery** - The next phase involves:
   - Column profiler (`discovery/profiler.py`)
   - Schema analyzer using Claude (`discovery/analyzer.py`)
   - Field mapping generator (`discovery/mapper.py`)

2. **Consider committing Phase 2 changes** - All tests pass, ready to commit

3. **Nothing Bundt Cakes integration note**: For NBC Snowflake connection, use:
   - `timestamp_column: "PURCHASED_AT"` in connection_params
   - Table: `THANX_PURCHASES` in schema `MART_SALE`

## Other Notes

- **Test commands:**
  - `uv run pytest packages/shared-connectors/tests/ -v` (93 tests pass)
  - `uv run pytest packages/shared-onboarding/tests/ -v` (100 tests pass)
  - `uv run mypy packages/shared-connectors/` (passes)
  - `uv run mypy packages/shared-onboarding/` (passes)

- **ConnectorStorage table schema** defined at `packages/shared-connectors/growthnav/connectors/storage.py:26-50` - Creates `growthnav_registry.connectors` table

- **Onboarding flow with data sources:**
  1. Validate request
  2. Check customer doesn't exist
  3. Create BigQuery dataset
  4. Register customer
  5. Store credentials (if provided)
  6. **Configure data sources (NEW)** - Converts DataSourceConfig to ConnectorConfig and saves via ConnectorStorage
  7. Return success

- **Rollback behavior**: If data source configuration fails, the customer registry entry is marked as inactive (same pattern as credential storage failure)
