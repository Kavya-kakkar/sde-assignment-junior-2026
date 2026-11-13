# AI Usage Disclosure

## Tools used
- ChatGPT
- Google Antigravity

## What I used them for
- Discussing the assignment requirements and breaking the work into implementation steps.
- Understanding data ingestion, schema design, validation, idempotency, event processing, and report generation.
- Getting implementation suggestions and debugging Python errors.
- Reviewing test failures and improving test coverage.
- Drafting documentation and setup instructions.

## What I wrote or decided myself
I made the assignment choice and worked through the implementation in the solution repository. I reviewed the proposed architecture, ran the code locally, inspected the generated reports, and made decisions about how to handle the supplied data.

The implementation uses workflow-specific lead schemas, a SQLite database, deterministic HMAC matching for phone identity, encrypted phone data, event deduplication, and report generation.

I also checked the generated output and reran the pipeline to verify that previously processed event files were skipped.

## Where the AI was wrong or unhelpful
An initial pipeline run failed on Windows because Python could not find the Asia/Kolkata timezone database. I identified this from the runtime traceback and resolved it by installing the tzdata package.

I also checked the repository instructions and retained the original assignment README rather than replacing it with implementation documentation.

## How I verified AI-generated code
- Read and reviewed the implementation while integrating it into the project.
- Ran the test suite: 26 tests passed.
- Ran Python compile checks successfully.
- Executed the pipeline against both supplied data drops.
- Inspected the generated CSV and JSON reports and verified their row counts.
- Ran the pipeline a second time and confirmed that already-processed event files were skipped.

I understand that passing unit tests and successful local execution do not guarantee correctness for every possible unseen dataset. Further integration testing and review of report edge cases would be valuable.
