---
date: 2025-12-06T19:21:05+0000
researcher: Claude
git_commit: af69fa8fc60368cd128c38f73af04e947108239d
branch: feature/phase3-schema-discovery
repository: GrowthNav
topic: "Phase 3: Schema Discovery & LLM-Assisted Mapping"
tags: [implementation, shared-connectors, schema-discovery, llm-mapping, profiler]
status: complete
last_updated: 2025-12-06
last_updated_by: Claude
type: implementation_strategy
---

# Handoff: Phase 3 Schema Discovery & LLM-Assisted Mapping

## Task(s)

**Implementation Plan:** `thoughts/shared/plans/2025-12-05-shared-connectors-implementation-plan.md`

### Completed Tasks (Phase 3 - Schema Discovery)

1. **ColumnProfile dataclass** - Created in profiler.py with statistics and computed properties (null_percentage, unique_percentage)
2. **ColumnProfiler class** - Type inference (string, number, datetime, boolean, unknown) and pattern detection (email, phone, currency, date_iso, uuid, gclid, url)
3. **MappingSuggestion dataclass** - Stores source/target field mappings with confidence scores and reasoning
4. **LLMSchemaMapper class** - Uses Claude API for intelligent field mapping suggestions with TARGET_SCHEMA for CLV-required fields
5. **SchemaDiscovery class** - Complete pipeline combining profiler and mapper with analyze() method
6. **Tests** - 48 tests for profiler, 24 tests for mapper (72 total new tests)
7. **Manual verification** - Tested with real POS sample data, successfully mapped 11/16 fields with ≥70% confidence

**Status:** Phase 3 is COMPLETE. All automated and manual verification passed.

**Pull Request:** PR #25 created and ready for review.

## Critical References

- `thoughts/shared/plans/2025-12-05-shared-connectors-implementation-plan.md` - Main implementation plan (Phase 3 complete, Phase 4 next)
- `packages/shared-connectors/growthnav/connectors/discovery/profiler.py` - ColumnProfile and ColumnProfiler classes
- `packages/shared-connectors/growthnav/connectors/discovery/mapper.py` - MappingSuggestion, LLMSchemaMapper, and SchemaDiscovery classes

## Recent changes

- `packages/shared-connectors/growthnav/connectors/discovery/profiler.py:1-278` - NEW: ColumnProfile dataclass and ColumnProfiler class
- `packages/shared-connectors/growthnav/connectors/discovery/mapper.py:1-295` - NEW: MappingSuggestion, LLMSchemaMapper, SchemaDiscovery classes
- `packages/shared-connectors/growthnav/connectors/discovery/__init__.py` - Updated exports for all new classes
- `packages/shared-connectors/tests/test_profiler.py` - NEW: 48 tests for ColumnProfile and ColumnProfiler
- `packages/shared-connectors/tests/test_mapper.py` - NEW: 24 tests for MappingSuggestion, LLMSchemaMapper, SchemaDiscovery
- `thoughts/shared/plans/2025-12-05-shared-connectors-implementation-plan.md` - Updated with Phase 3 completion checkmarks

## Learnings

1. **ColumnProfile required fields**: The dataclass requires `total_count`, `null_count`, and `unique_count` as positional arguments - tests must always provide these.

2. **_is_numeric doesn't parse complex formats**: The implementation uses simple `float()` conversion, which doesn't handle comma-separated numbers like "1,234" or currency prefixes like "$42.50". This is intentional - keep profiler simple.

3. **Missing keys count as None**: When profiling with `row.get(column)`, rows missing a key return None, which counts toward total_count and null_count. All rows contribute to totals.

4. **SchemaDiscovery uses private attributes**: The profiler and mapper are stored as `_profiler` and `_mapper` (private), not public attributes.

5. **_parse_response raises ValueError**: Invalid JSON or malformed responses raise ValueError, not return empty list. Tests must use `pytest.raises`.

6. **Anthropic lazy initialization**: The LLMSchemaMapper uses a lazy `_client` property that only imports/instantiates anthropic when first accessed. This allows the class to be imported without anthropic installed.

7. **Manual testing setup**: Need to `uv add anthropic` and load API key from `.env` file for real LLM testing.

## Artifacts

- `packages/shared-connectors/growthnav/connectors/discovery/profiler.py` - NEW ColumnProfile and ColumnProfiler
- `packages/shared-connectors/growthnav/connectors/discovery/mapper.py` - NEW LLMSchemaMapper and SchemaDiscovery
- `packages/shared-connectors/growthnav/connectors/discovery/__init__.py` - Updated exports
- `packages/shared-connectors/tests/test_profiler.py` - NEW 48 profiler tests
- `packages/shared-connectors/tests/test_mapper.py` - NEW 24 mapper tests
- `thoughts/shared/plans/2025-12-05-shared-connectors-implementation-plan.md` - Updated with completion checkmarks

## Action Items & Next Steps

1. **Merge PR #25** - All tests pass, ready for merge

2. **Proceed to Phase 4: Field Mapping Persistence** - The next phase involves:
   - FieldMappingConfig dataclass
   - FieldMappingStorage class (BigQuery-backed)
   - API methods for CRUD operations on mappings

3. **Integration with onboarding** - Consider integrating SchemaDiscovery into the onboarding flow to automatically suggest mappings for new data sources

## Other Notes

- **Test commands:**
  - `uv run pytest packages/shared-connectors/tests/ -v` (171 tests pass)
  - `uv run mypy packages/shared-connectors/` (passes)
  - `uv run ruff check packages/shared-connectors/` (passes)

- **Target Schema for CLV analysis** defined at `packages/shared-connectors/growthnav/connectors/discovery/mapper.py:55-82`:
  - Required fields: transaction_id, timestamp, value
  - Customer fields: customer_id, email, phone, name
  - Value fields: total_amount, discount, tax, quantity
  - Marketing fields: channel, source, campaign, gclid
  - Other: location_id, product_id

- **Pattern detection thresholds**: Both type inference and pattern detection use >50% threshold - a type/pattern must match more than half of non-null values to be detected.

- **LLM prompt structure**: The mapper builds a detailed prompt including column profiles, detected patterns, and target schema documentation. Claude returns JSON with source_field, target_field, confidence (0.0-1.0), and reasoning.

- **Manual test results**: With real POS data (16 columns including order_id, paid_amount, tip, user_id, etc.), the LLM correctly identified:
  - transaction_id ← order_id (95% confidence)
  - timestamp ← created_at (95% confidence)
  - value ← paid_amount (90% confidence)
  - Plus 8 additional mappings with ≥70% confidence
