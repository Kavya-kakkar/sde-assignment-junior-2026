from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from interaction_ledger.database import Database


def _database(tmp_path: Path) -> Database:
    database = Database(tmp_path / "ledger.sqlite3")
    database.initialize()
    return database


def _ingestion_file(
    connection: sqlite3.Connection,
    *,
    source_path: str,
    file_kind: str,
    workflow_id: str | None = "motor_renewal",
) -> int:
    cursor = connection.execute(
        """
        INSERT INTO ingestion_files (source_path, content_sha256, file_kind, workflow_id)
        VALUES (?, ?, ?, ?)
        """,
        (source_path, f"sha256:{source_path}", file_kind, workflow_id),
    )
    assert cursor.lastrowid is not None
    return cursor.lastrowid


def _batch(connection: sqlite3.Connection, ingestion_file_id: int) -> None:
    connection.execute(
        """
        INSERT INTO batches (workflow_id, batch_id, ingestion_file_id)
        VALUES ('motor_renewal', 'batch_001', ?)
        """,
        (ingestion_file_id,),
    )


def _lead(connection: sqlite3.Connection, *, phone_hmac: str = "phone-hmac") -> int:
    cursor = connection.execute(
        """
        INSERT INTO leads (
            workflow_id, batch_id, phone_hmac, pii_ciphertext, non_pii_attributes_json
        )
        VALUES ('motor_renewal', 'batch_001', ?, ?, '{}')
        """,
        (phone_hmac, b"test-fernet-ciphertext"),
    )
    assert cursor.lastrowid is not None
    return cursor.lastrowid


def test_initialization_is_idempotent(tmp_path: Path) -> None:
    database = _database(tmp_path)
    database.initialize()

    with database.connect() as connection:
        table_names = {
            row["name"]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }

    assert {
        "leads",
        "batches",
        "rejections",
        "events",
        "ingestion_files",
        "event_lines",
        "disposition_catalogue",
    }.issubset(table_names)


def test_connections_enforce_foreign_keys(tmp_path: Path) -> None:
    database = _database(tmp_path)

    with database.connect() as connection:
        assert connection.execute("PRAGMA foreign_keys").fetchone()[0] == 1

    with pytest.raises(sqlite3.IntegrityError):
        with database.transaction() as connection:
            connection.execute(
                """
                INSERT INTO batches (workflow_id, batch_id, ingestion_file_id)
                VALUES ('motor_renewal', 'batch_001', 999)
                """
            )


def test_lead_identity_and_event_ids_are_unique(tmp_path: Path) -> None:
    database = _database(tmp_path)

    with database.transaction() as connection:
        lead_file_id = _ingestion_file(
            connection, source_path="drop_1/leads/motor_renewal/batch_001.csv", file_kind="leads"
        )
        _batch(connection, lead_file_id)
        _lead(connection)

        with pytest.raises(sqlite3.IntegrityError):
            _lead(connection)

        event_file_id = _ingestion_file(
            connection, source_path="drop_1/events/events_0001.jsonl", file_kind="events", workflow_id=None
        )
        connection.execute(
            """
            INSERT INTO events (
                event_id, workflow_id, lead_id, call_id, event_type, occurred_at,
                phone_hmac, phone_ciphertext, payload_json, ingestion_file_id, source_line_number
            )
            VALUES (?, 'motor_renewal', NULL, 'call_001', 'call.initiated',
                    '2026-10-01T10:00:00Z', ?, ?, '{}', ?, 1)
            """,
            ("evt_001", "phone-hmac", b"encrypted-phone", event_file_id),
        )
        stored_event = connection.execute(
            "SELECT lead_id FROM events WHERE event_id = 'evt_001'"
        ).fetchone()
        assert stored_event is not None
        assert stored_event["lead_id"] is None

        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                """
                INSERT INTO events (
                    event_id, workflow_id, event_type, occurred_at, payload_json,
                    ingestion_file_id, source_line_number
                )
                VALUES ('evt_001', 'policy_feedback', 'call.initiated',
                        '2026-10-01T10:00:00Z', '{}', ?, 2)
                """,
                (event_file_id,),
            )


def test_event_payload_is_append_only_but_late_lead_linking_is_allowed(tmp_path: Path) -> None:
    database = _database(tmp_path)

    with database.transaction() as connection:
        lead_file_id = _ingestion_file(
            connection, source_path="drop_1/leads/motor_renewal/batch_001.csv", file_kind="leads"
        )
        _batch(connection, lead_file_id)
        lead_id = _lead(connection)
        event_file_id = _ingestion_file(
            connection, source_path="drop_1/events/events_0001.jsonl", file_kind="events", workflow_id=None
        )
        connection.execute(
            """
            INSERT INTO events (
                event_id, workflow_id, event_type, occurred_at, payload_json,
                ingestion_file_id, source_line_number
            )
            VALUES ('evt_001', 'motor_renewal', 'call.initiated',
                    '2026-10-01T10:00:00Z', '{}', ?, 1)
            """,
            (event_file_id,),
        )

        connection.execute("UPDATE events SET lead_id = ? WHERE event_id = 'evt_001'", (lead_id,))
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            connection.execute(
                "UPDATE events SET event_type = 'call.connected' WHERE event_id = 'evt_001'"
            )
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            connection.execute("DELETE FROM events WHERE event_id = 'evt_001'")


def test_ingestion_line_records_cannot_be_inserted_twice(tmp_path: Path) -> None:
    database = _database(tmp_path)

    with database.transaction() as connection:
        event_file_id = _ingestion_file(
            connection, source_path="drop_1/events/events_0001.jsonl", file_kind="events", workflow_id=None
        )
        connection.execute(
            """
            INSERT INTO event_lines (ingestion_file_id, line_number, event_id, processing_status)
            VALUES (?, 1, NULL, 'malformed')
            """,
            (event_file_id,),
        )
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                """
                INSERT INTO event_lines (ingestion_file_id, line_number, event_id, processing_status)
                VALUES (?, 1, NULL, 'malformed')
                """,
                (event_file_id,),
            )
