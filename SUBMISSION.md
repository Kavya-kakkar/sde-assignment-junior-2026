# Submission

## Candidate
- **Name:** Kavya Kakkar
- **Assignment chosen:** A (Data)
- **Time spent:** 9 hrs

## How to run

Run from the repository root in Windows PowerShell:

```powershell
cd .\solution
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -e .
python -m pip install pytest tzdata
python .\run_pipeline.py
python -m pytest -v
```

The assignment configuration must remain in `../assignment-a-data/`, and generated input data must be available in `../agentic-au-data/`. These input files are not included in the solution commit.

Reports are written to `solution/output/`.

## Design

The implementation is organized into a Python package named `interaction_ledger`. It separates configuration loading, lead ingestion and normalization, database operations, and PII protection. The `run_pipeline.py` script coordinates ingestion and report generation.

SQLite is used as the persistent ledger. Lead identity is based on `(workflow_id, phone_hmac)`. Phone numbers are normalized before matching, protected using deterministic HMAC for identity matching, and encrypted before storage.

Lead ingestion applies workflow-specific schemas, header aliases, type normalization, required-field validation, optional-field warnings, and rejection tracking. Duplicate leads are merged using non-null incoming values, while the first valid row establishes batch ownership.

Event ingestion handles accepted, malformed, duplicate, orphan, and unknown-disposition outcomes. Event IDs are deduplicated. Orphan events can be linked later when a matching lead becomes available. Supervisor disposition overrides are linked through call IDs.

The pipeline generates:
- `batch_summary.csv`
- `daily_overview.csv`
- `lead_report.csv`
- `rejections.csv`
- `data_quality.json`

## Decisions and assumptions

- SQLite was selected for simple local setup and the assignment's dataset size.
- Lead identity uses workflow ID and normalized phone HMAC.
- The first valid lead row determines batch ownership.
- Later duplicate rows update only non-null attributes.
- Naive event timestamps are interpreted as IST.
- Unknown dispositions are counted but excluded from valid disposition calculations.
- Orphan events are retained and checked for resolution after both data drops are ingested.

## Failure modes and limitations

The implementation handles invalid required fields, invalid optional values, duplicate leads, duplicate event IDs, malformed JSONL records, orphan events, and unknown dispositions.

Known limitations:
- Comprehensive automated integration tests for all report calculations are not yet present.
- Some orphan events remain unresolved and need investigation.
- Concurrent pipeline execution has not been tested.
- Crash recovery and partial-file failure scenarios need further testing.

## Testing

The current test suite contains 26 passing tests.

Coverage includes configuration loading, PII encryption and HMAC behavior, database constraints, phone normalization, date and decimal parsing, CSV header aliases, and lead validation.

Manual verification:
- Ran the pipeline against both supplied data drops.
- Successfully compiled the Python source.
- Inspected generated report row counts.
- Reran the pipeline and confirmed previously processed event files were skipped.

## Results

Latest successful run:

- Unique leads: 1,892
- Batches: 6
- Total lead rows: 2,057
- Rejected lead rows: 108
- Duplicate lead rows merged: 57
- Field warnings: 122
- Total event lines: 8,745
- Malformed events: 8
- Duplicate events: 237
- Unknown-disposition events: 19
- Orphan events remaining after late-link resolution: 461
- Orphan events resolved: 66
- Daily overview rows: 33

Generated report row counts:
- `batch_summary.csv`: 6
- `daily_overview.csv`: 33
- `lead_report.csv`: 1,892
- `rejections.csv`: 108

Execution timing was not formally benchmarked.

## If I had another day

1. Add end-to-end tests for all five reports and their calculations.
2. Investigate unresolved orphan events and strengthen event-to-lead linking.
3. Test incremental ingestion equivalence between sequential and all-at-once processing.
4. Test crash recovery, partial ingestion failures, and concurrent executions.
5. Improve logging and documentation of report calculation rules.
