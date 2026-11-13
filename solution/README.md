# Interaction Ledger

A Python-based, SQLite-backed ingestion and reporting pipeline for the Agentic Universe Junior SDE Assignment A.

## Features

- Ingests lead CSV files from multiple workflows and batches.
- Normalizes headers, phone numbers, dates, decimal values, and enum fields.
- Validates required fields and records rejected rows with reasons.
- Merges duplicate leads using workflow and canonical phone identity.
- Protects phone data at rest using encryption and deterministic HMAC matching.
- Ingests JSONL interaction events with duplicate detection.
- Handles malformed, orphan, duplicate, and unknown-disposition events.
- Supports late linking of orphan events when matching leads become available.
- Generates batch, daily, lead-level, rejection, and data-quality reports.
- Supports repeatable pipeline execution using persistent SQLite state.

## Technology

- Python 3.11+
- SQLite
- SQLAlchemy / Python database layer
- Cryptography (Fernet encryption and HMAC)
- pytest

## Project Structure

```text
solution/
├── src/
│   └── interaction_ledger/
├── tests/
├── output/
├── run_pipeline.py
├── pyproject.toml
└── README.md
