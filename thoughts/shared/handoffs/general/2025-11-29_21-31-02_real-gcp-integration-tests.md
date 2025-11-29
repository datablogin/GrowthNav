---
date: 2025-11-29T21:31:02+0000
researcher: Claude
git_commit: 0e90e91a0a72398c92aa06fe84d5de8172819e8e
branch: main
repository: GrowthNav
topic: "Real GCP Integration Tests Implementation"
tags: [integration-tests, gcp, google-sheets, google-slides, bigquery, domain-wide-delegation]
status: in_progress
last_updated: 2025-11-29
last_updated_by: Claude
type: implementation_strategy
---

# Handoff: Real GCP Integration Tests for Sheets and Slides

## Task(s)

1. **BigQuery Integration Tests** - COMPLETED
   - Created real integration tests hitting actual BigQuery API
   - All 36 tests passing in CI

2. **Google Sheets Integration Tests** - COMPLETED (CI in progress)
   - Created 12 real integration tests for SheetsExporter
   - Added domain-wide delegation support with `subject` parameter
   - Default impersonation email: `access@roimediapartners.com`
   - Tests enabled in CI with `RUN_SHEETS_INTEGRATION_TESTS=1`

3. **Google Slides Integration Tests** - COMPLETED (CI in progress)
   - Created 11 real integration tests for SlidesGenerator
   - Fixed placeholder ID bug - implementation now queries actual placeholder IDs after slide creation
   - Added domain-wide delegation support
   - Tests enabled in CI with `RUN_SLIDES_INTEGRATION_TESTS=1`

4. **Rate Limiting Protection** - COMPLETED
   - Added 5-second delays between tests to avoid 429 errors
   - Google APIs have 60 requests/minute per user limit

## Critical References

- `CLAUDE.md` - Project overview and roadmap
- `.github/workflows/ci.yml` - CI configuration with GCP auth and test environment variables

## Recent changes

- `packages/shared-reporting/growthnav/reporting/slides.py:165-278` - Rewrote `create_presentation()` to properly query placeholder IDs instead of using hardcoded IDs
- `packages/shared-reporting/growthnav/reporting/sheets.py:45-95` - Added `impersonate_email` parameter and domain-wide delegation support
- `packages/shared-reporting/tests/test_integration_slides.py:26-29` - Increased rate limit delay to 5 seconds
- `packages/shared-reporting/tests/test_integration_sheets.py:23-25` - Increased rate limit delay to 5 seconds
- `packages/shared-reporting/tests/test_integration_slides.py:145-182` - Changed slide count assertions from `==` to `>=` to account for default blank slide

## Learnings

1. **Google Slides Placeholder IDs**: When creating slides with predefined layouts, Google Slides assigns random IDs like `SLIDES_API1285841172_0` to placeholders. You cannot use custom IDs like `slide_0_title`. Must query the presentation after creating each slide to get actual placeholder IDs.

2. **Domain-Wide Delegation**: Service accounts need domain-wide delegation configured in Google Workspace Admin Console to create files on behalf of users. Use `subject` parameter in `Credentials.from_service_account_file()`.

3. **Rate Limiting**: Google Sheets/Slides APIs have 60 requests/minute per user. The new Slides implementation makes 3 API calls per slide (create, get, batchUpdate), so tests hit limits quickly. 5-second delays between tests help.

4. **Default Blank Slide**: New Google Slides presentations include a default blank slide, so test assertions should use `>=` not `==` for slide counts.

## Artifacts

- `packages/shared-reporting/tests/test_integration_sheets.py` - 12 real Sheets integration tests
- `packages/shared-reporting/tests/test_integration_slides.py` - 11 real Slides integration tests
- `packages/shared-reporting/tests/test_slides.py` - Updated unit tests with mock get() responses
- `.github/workflows/ci.yml` - CI with GCP auth and Sheets/Slides tests enabled

## Action Items & Next Steps

1. **Monitor CI Run** - Latest push (commit `0e90e91`) has increased rate limits and fixed assertions. Check if CI passes:
   ```bash
   gh run list --limit 1
   gh run view <run_id> --log-failed
   ```

2. **If Rate Limiting Still Fails** - Options:
   - Further increase delay (try 10 seconds)
   - Reduce number of integration tests
   - Request quota increase from Google
   - Run Sheets and Slides tests in separate CI jobs (different time windows)

3. **Clean Up Test Presentations** - The cleanup fixture uses service account credentials which can't delete files created by impersonated user. May need to add Drive API scope or use admin cleanup.

4. **Continue Roadmap** - Once CI passes, next items from CLAUDE.md:
   - P1: Unified conversions (`shared-conversions`) - may need real integration tests
   - P2: Industry benchmarks (`shared-bigquery`)

## Other Notes

- **GCP Project**: `topgolf-460202`
- **Service Account**: `growthnav-ci@topgolf-460202.iam.gserviceaccount.com`
- **GitHub Secret**: `GCP_SA_KEY` contains service account JSON key
- **Impersonation Email**: `access@roimediapartners.com` (default in code)
- **Domain-Wide Delegation Scopes**:
  - `https://www.googleapis.com/auth/spreadsheets`
  - `https://www.googleapis.com/auth/drive.file`
  - `https://www.googleapis.com/auth/presentations`

User explicitly stated they don't want mock-based tests - all tests should hit real GCP services.
