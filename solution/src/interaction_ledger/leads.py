
"""CSV lead ingestion and field normalisation."""

from __future__ import annotations

import hashlib
import json
import csv
import re
from dataclasses import dataclass
from datetime import datetime
from interaction_ledger.database import Database
from interaction_ledger.crypto import PiiProtector
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
from typing import Any

from interaction_ledger.config import WorkflowSchema, WorkflowField, normalise_header


def normalize_phone(value: str) -> str:
    """Normalize a valid Indian mobile number to +91XXXXXXXXXX."""
    digits = re.sub(r"\D", "", value.strip())

    if len(digits) == 12 and digits.startswith("91"):
        digits = digits[2:]
    elif len(digits) == 11 and digits.startswith("0"):
        digits = digits[1:]

    if len(digits) != 10 or digits[0] not in "6789":
        raise ValueError("invalid_phone")

    return "+91" + digits


def normalize_date(value: str) -> str:
    """Normalize supported date formats to ISO YYYY-MM-DD."""
    formats = (
        "%Y-%m-%d",
        "%d/%m/%Y",
        "%d-%m-%Y",
        "%d-%b-%Y",
    )

    for date_format in formats:
        try:
            return datetime.strptime(value.strip(), date_format).date().isoformat()
        except ValueError:
            continue

    raise ValueError("invalid_date")


def normalize_decimal(value: str) -> str:
    """Normalize currency values to two decimal places."""
    cleaned = re.sub(r"^(₹|INR\s*|Rs\.?\s*)", "", value.strip(), flags=re.I)
    cleaned = cleaned.replace(",", "").strip()

    try:
        amount = Decimal(cleaned)
    except InvalidOperation as error:
        raise ValueError("invalid_decimal") from error

    if not amount.is_finite() or amount < 0:
        raise ValueError("invalid_decimal")

    return str(amount.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def normalize_value(value: str, field: WorkflowField) -> str | None:
    """Validate and normalize one schema field."""
    value = value.strip()

    if not value:
        if field.required:
            raise ValueError(f"missing_required:{field.name}")
        return None

    if field.field_type == "string":
        return value

    if field.field_type == "phone":
        return normalize_phone(value)

    if field.field_type == "date":
        return normalize_date(value)

    if field.field_type == "decimal":
        return normalize_decimal(value)

    if field.field_type == "enum":
        normalized = value.casefold()
        allowed = {item.casefold() for item in field.values}

        if normalized not in allowed:
            raise ValueError(f"invalid_enum:{field.name}")

        return normalized

    raise ValueError(f"unsupported_field_type:{field.field_type}")


def map_csv_headers(
    headers: list[str],
    schema: WorkflowSchema,
) -> dict[str, str]:
    """Map CSV column names to schema field names."""
    aliases: dict[str, str] = {}

    for field in schema.fields:
        for name in (field.name, *field.aliases):
            aliases[normalise_header(name)] = field.name

    mapping: dict[str, str] = {}

    for header in headers:
        normalized = normalise_header(header)

        if normalized in aliases:
            field_name = aliases[normalized]

            if field_name in mapping.values():
                raise ValueError(f"duplicate_column:{field_name}")

            mapping[header] = field_name

    return mapping


def parse_lead_row(
    row: dict[str, Any],
    header_mapping: dict[str, str],
    schema: WorkflowSchema,
) -> tuple[dict[str, str | None], list[str]]:
    """Parse a row, rejecting the first invalid required field."""
    raw_values = {
        field_name: row.get(header, "")
        for header, field_name in header_mapping.items()
    }

    normalized: dict[str, str | None] = {}
    warnings: list[str] = []

    for field in schema.fields:
        raw = raw_values.get(field.name, "")

        try:
            normalized[field.name] = normalize_value(raw or "", field)
        except ValueError as error:
            reason = str(error)

            if field.required:
                raise ValueError(reason) from error

            normalized[field.name] = None
            warnings.append(field.name)

    return normalized, warnings


def read_lead_csv(
    csv_path: str | Path,
    schema: WorkflowSchema,
) -> list[tuple[int, dict[str, str | None], list[str]]]:
    """Read and validate CSV rows, returning row numbers and warnings."""
    results = []

    with Path(csv_path).open("r", encoding="utf-8-sig", newline="") as source:
        reader = csv.DictReader(source)

        if not reader.fieldnames:
            raise ValueError("missing_csv_header")

        mapping = map_csv_headers(reader.fieldnames, schema)

        row_number = 0
        for row in reader:
            if not any(str(value or "").strip() for value in row.values()):
                continue
            row_number += 1

            try:
                normalized, warnings = parse_lead_row(row, mapping, schema)
                results.append((row_number, normalized, warnings))
            except ValueError as error:
                results.append((row_number, {"_rejection": str(error)}, []))

    return results


@dataclass
class LeadIngestionResult:
    total_rows: int = 0
    accepted: int = 0
    rejected: int = 0
    merged_duplicates: int = 0
    field_warnings: int = 0
    skipped: bool = False


def apply_defaults(
    values: dict[str, str | None],
    schema: WorkflowSchema,
) -> dict[str, str | None]:
    """Apply optional defaults after duplicate merging."""
    result = dict(values)

    for field in schema.fields:
        if result.get(field.name) is None and field.default is not None:
            result[field.name] = str(field.default).casefold()

    return result


def ingest_lead_csv(
    database: Database,
    csv_path: str | Path,
    schema: WorkflowSchema,
    batch_id: str,
    protector: PiiProtector,
) -> LeadIngestionResult:
    """Ingest a lead CSV with encryption, deduplication and audit records."""

    path = Path(csv_path)
    content = path.read_bytes()
    content_hash = hashlib.sha256(content).hexdigest()
    source_path = str(path.resolve())

    rows = read_lead_csv(path, schema)
    result = LeadIngestionResult(total_rows=len(rows))

    workflow_id = schema.workflow_id
    phone_field = schema.phone_field

    with database.transaction() as connection:
        connection.execute(
            """
            INSERT OR IGNORE INTO ingestion_files
                (source_path, content_sha256, file_kind, workflow_id)
            VALUES (?, ?, 'leads', ?)
            """,
            (source_path, content_hash, workflow_id),
        )

        file_record = connection.execute(
            """
            SELECT id, processing_state
            FROM ingestion_files
            WHERE source_path = ? AND content_sha256 = ?
            """,
            (source_path, content_hash),
        ).fetchone()

        file_id = file_record["id"]

        if file_record["processing_state"] == "completed":
            result.skipped = True
            return result

        connection.execute(
            """
            INSERT INTO batches (workflow_id, batch_id, ingestion_file_id)
            VALUES (?, ?, ?)
            """,
            (workflow_id, batch_id, file_id),
        )

        for row_number, values, warnings in rows:
            if "_rejection" in values:
                reason = values["_rejection"]

                connection.execute(
                    """
                    INSERT INTO rejections
                        (workflow_id, batch_id, ingestion_file_id,
                         row_number, reason)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (workflow_id, batch_id, file_id, row_number, reason),
                )

                connection.execute(
                    """
                    INSERT INTO lead_lines
                        (ingestion_file_id, row_number, outcome,
                         field_warning_count, lead_id)
                    VALUES (?, ?, 'rejected', 0, NULL)
                    """,
                    (file_id, row_number),
                )

                result.rejected += 1
                continue

            result.field_warnings += len(warnings)

            canonical_phone = values[phone_field.name]
            phone_hmac = protector.phone_hmac(canonical_phone)

            existing = connection.execute(
                """
                SELECT id, pii_ciphertext, non_pii_attributes_json
                FROM leads
                WHERE workflow_id = ? AND phone_hmac = ?
                """,
                (workflow_id, phone_hmac),
            ).fetchone()

            incoming_pii = {
                field.name: values[field.name]
                for field in schema.fields
                if field.pii
            }

            incoming_attributes = {
                field.name: values[field.name]
                for field in schema.fields
                if not field.pii
            }

            if existing is None:
                merged_pii = incoming_pii
                merged_attributes = incoming_attributes
                outcome = "accepted"
            else:
                old_pii = json.loads(
                    protector.decrypt_text(existing["pii_ciphertext"])
                )
                old_attributes = json.loads(
                    existing["non_pii_attributes_json"]
                )

                merged_pii = old_pii
                merged_attributes = old_attributes

                for key, value in incoming_pii.items():
                    if value is not None:
                        merged_pii[key] = value

                for key, value in incoming_attributes.items():
                    if value is not None:
                        merged_attributes[key] = value

                outcome = "merged_duplicate"

            # Apply defaults only after merging non-null values.
            merged_all = {
                **merged_pii,
                **merged_attributes,
            }
            merged_all = apply_defaults(merged_all, schema)

            merged_pii = {
                field.name: merged_all[field.name]
                for field in schema.fields
                if field.pii
            }
            merged_attributes = {
                field.name: merged_all[field.name]
                for field in schema.fields
                if not field.pii
            }

            ciphertext = protector.encrypt_text(
                json.dumps(merged_pii, ensure_ascii=False)
            )
            attributes_json = json.dumps(
                merged_attributes,
                ensure_ascii=False,
            )

            if existing is None:
                cursor = connection.execute(
                    """
                    INSERT INTO leads (
                        workflow_id, batch_id, phone_hmac,
                        pii_ciphertext, non_pii_attributes_json
                    )
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        workflow_id,
                        batch_id,
                        phone_hmac,
                        ciphertext,
                        attributes_json,
                    ),
                )
                lead_id = cursor.lastrowid
                result.accepted += 1
            else:
                lead_id = existing["id"]

                connection.execute(
                    """
                    UPDATE leads
                    SET pii_ciphertext = ?,
                        non_pii_attributes_json = ?,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                    """,
                    (ciphertext, attributes_json, lead_id),
                )

                result.merged_duplicates += 1

            connection.execute(
                """
                INSERT INTO lead_lines (
                    ingestion_file_id, row_number, outcome,
                    field_warning_count, lead_id
                )
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    file_id,
                    row_number,
                    outcome,
                    len(warnings),
                    lead_id,
                ),
            )

        connection.execute(
            """
            UPDATE ingestion_files
            SET processing_state = 'completed',
                completed_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (file_id,),
        )

    return result
