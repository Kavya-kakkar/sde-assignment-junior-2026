"""SQLite database creation and transaction helpers for the interaction ledger."""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path


# DDL is deliberately explicit rather than hidden behind an ORM. The table and
# index names make the key reporting and idempotency invariants easy to inspect.
SCHEMA_STATEMENTS: tuple[str, ...] = (
    """
    CREATE TABLE IF NOT EXISTS ingestion_files (
        id INTEGER PRIMARY KEY,
        source_path TEXT NOT NULL,
        content_sha256 TEXT NOT NULL,
        file_kind TEXT NOT NULL CHECK (file_kind IN ('leads', 'events')),
        workflow_id TEXT,
        processing_state TEXT NOT NULL DEFAULT 'pending'
            CHECK (processing_state IN ('pending', 'completed', 'failed')),
        discovered_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        completed_at TEXT,
        UNIQUE (source_path, content_sha256)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS batches (
        id INTEGER PRIMARY KEY,
        workflow_id TEXT NOT NULL,
        batch_id TEXT NOT NULL CHECK (length(trim(batch_id)) > 0),
        ingestion_file_id INTEGER NOT NULL UNIQUE,
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        UNIQUE (workflow_id, batch_id),
        FOREIGN KEY (ingestion_file_id) REFERENCES ingestion_files(id)
            ON DELETE RESTRICT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS disposition_catalogue (
        workflow_id TEXT NOT NULL,
        code TEXT NOT NULL,
        label TEXT NOT NULL,
        is_terminal INTEGER NOT NULL CHECK (is_terminal IN (0, 1)),
        is_success INTEGER NOT NULL CHECK (is_success IN (0, 1)),
        loaded_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (workflow_id, code)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS leads (
        id INTEGER PRIMARY KEY,
        workflow_id TEXT NOT NULL,
        batch_id TEXT NOT NULL,
        phone_hmac TEXT NOT NULL CHECK (length(phone_hmac) > 0),
        pii_ciphertext BLOB NOT NULL CHECK (length(pii_ciphertext) > 0),
        non_pii_attributes_json TEXT NOT NULL DEFAULT '{}',
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        UNIQUE (workflow_id, phone_hmac),
        UNIQUE (workflow_id, id),
        FOREIGN KEY (workflow_id, batch_id) REFERENCES batches(workflow_id, batch_id)
            ON DELETE RESTRICT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS rejections (
        id INTEGER PRIMARY KEY,
        workflow_id TEXT NOT NULL,
        batch_id TEXT NOT NULL,
        ingestion_file_id INTEGER NOT NULL,
        row_number INTEGER NOT NULL CHECK (row_number > 0),
        reason TEXT NOT NULL CHECK (length(trim(reason)) > 0),
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        UNIQUE (ingestion_file_id, row_number),
        FOREIGN KEY (ingestion_file_id) REFERENCES ingestion_files(id)
            ON DELETE RESTRICT,
        FOREIGN KEY (workflow_id, batch_id) REFERENCES batches(workflow_id, batch_id)
            ON DELETE RESTRICT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS lead_lines (
        id INTEGER PRIMARY KEY,
        ingestion_file_id INTEGER NOT NULL,
        row_number INTEGER NOT NULL CHECK (row_number > 0),
        outcome TEXT NOT NULL
            CHECK (outcome IN ('accepted', 'rejected', 'merged_duplicate')),
        field_warning_count INTEGER NOT NULL DEFAULT 0 CHECK (field_warning_count >= 0),
        lead_id INTEGER,
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        UNIQUE (ingestion_file_id, row_number),
        CHECK (
            (outcome = 'rejected' AND lead_id IS NULL)
            OR (outcome <> 'rejected' AND lead_id IS NOT NULL)
        ),
        FOREIGN KEY (ingestion_file_id) REFERENCES ingestion_files(id)
            ON DELETE RESTRICT,
        FOREIGN KEY (lead_id) REFERENCES leads(id) ON DELETE RESTRICT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS events (
        event_id TEXT PRIMARY KEY NOT NULL,
        workflow_id TEXT NOT NULL,
        lead_id INTEGER,
        call_id TEXT,
        event_type TEXT NOT NULL,
        occurred_at TEXT NOT NULL,
        phone_hmac TEXT,
        phone_ciphertext BLOB,
        payload_json TEXT NOT NULL DEFAULT '{}',
        is_reportable INTEGER NOT NULL DEFAULT 1 CHECK (is_reportable IN (0, 1)),
        ingestion_file_id INTEGER NOT NULL,
        source_line_number INTEGER NOT NULL CHECK (source_line_number > 0),
        recorded_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        UNIQUE (ingestion_file_id, source_line_number),
        CHECK (
            (phone_hmac IS NULL AND phone_ciphertext IS NULL)
            OR (phone_hmac IS NOT NULL AND phone_ciphertext IS NOT NULL)
        ),
        FOREIGN KEY (ingestion_file_id) REFERENCES ingestion_files(id)
            ON DELETE RESTRICT,
        FOREIGN KEY (workflow_id, lead_id) REFERENCES leads(workflow_id, id)
            ON DELETE RESTRICT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS event_lines (
        id INTEGER PRIMARY KEY,
        ingestion_file_id INTEGER NOT NULL,
        line_number INTEGER NOT NULL CHECK (line_number > 0),
        event_id TEXT,
        processing_status TEXT NOT NULL CHECK (
            processing_status IN (
                'accepted', 'malformed', 'duplicate', 'orphan', 'unknown_disposition'
            )
        ),
        detail TEXT,
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        UNIQUE (ingestion_file_id, line_number),
        CHECK (
            (processing_status = 'malformed' AND event_id IS NULL)
            OR (processing_status <> 'malformed' AND event_id IS NOT NULL)
        ),
        FOREIGN KEY (ingestion_file_id) REFERENCES ingestion_files(id)
            ON DELETE RESTRICT,
        FOREIGN KEY (event_id) REFERENCES events(event_id) ON DELETE RESTRICT
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_ingestion_files_kind_state ON ingestion_files(file_kind, processing_state)",
    "CREATE INDEX IF NOT EXISTS idx_leads_workflow_batch ON leads(workflow_id, batch_id)",
    "CREATE INDEX IF NOT EXISTS idx_rejections_workflow_batch ON rejections(workflow_id, batch_id, row_number)",
    "CREATE INDEX IF NOT EXISTS idx_lead_lines_outcome ON lead_lines(outcome)",
    "CREATE INDEX IF NOT EXISTS idx_events_workflow_phone ON events(workflow_id, phone_hmac)",
    "CREATE INDEX IF NOT EXISTS idx_events_lead_occurred ON events(lead_id, occurred_at)",
    "CREATE INDEX IF NOT EXISTS idx_events_call_id ON events(call_id)",
    "CREATE INDEX IF NOT EXISTS idx_event_lines_status ON event_lines(processing_status)",
    """
    CREATE TRIGGER IF NOT EXISTS leads_preserve_identity
    BEFORE UPDATE ON leads
    WHEN OLD.workflow_id IS NOT NEW.workflow_id
      OR OLD.batch_id IS NOT NEW.batch_id
      OR OLD.phone_hmac IS NOT NEW.phone_hmac
    BEGIN
        SELECT RAISE(ABORT, 'lead workflow_id, batch_id, and phone_hmac are immutable');
    END
    """,
    """
    CREATE TRIGGER IF NOT EXISTS events_preserve_ledger_fields
    BEFORE UPDATE ON events
    WHEN OLD.event_id IS NOT NEW.event_id
      OR OLD.workflow_id IS NOT NEW.workflow_id
      OR OLD.call_id IS NOT NEW.call_id
      OR OLD.event_type IS NOT NEW.event_type
      OR OLD.occurred_at IS NOT NEW.occurred_at
      OR OLD.phone_hmac IS NOT NEW.phone_hmac
      OR OLD.phone_ciphertext IS NOT NEW.phone_ciphertext
      OR OLD.payload_json IS NOT NEW.payload_json
      OR OLD.is_reportable IS NOT NEW.is_reportable
      OR OLD.ingestion_file_id IS NOT NEW.ingestion_file_id
      OR OLD.source_line_number IS NOT NEW.source_line_number
      OR OLD.recorded_at IS NOT NEW.recorded_at
    BEGIN
        SELECT RAISE(ABORT, 'event ledger fields are immutable; only lead_id may change');
    END
    """,
    """
    CREATE TRIGGER IF NOT EXISTS events_prevent_deletion
    BEFORE DELETE ON events
    BEGIN
        SELECT RAISE(ABORT, 'events are append-only and cannot be deleted');
    END
    """,
)


class Database:
    """A small SQLite database wrapper with foreign keys and transactions enabled."""

    def __init__(self, database_path: str | Path) -> None:
        self._database_path = str(database_path)

    @property
    def database_path(self) -> str:
        """Return the SQLite database location passed to the constructor."""

        return self._database_path

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        """Yield a connection with SQLite foreign-key enforcement enabled."""

        connection = sqlite3.connect(self._database_path)
        try:
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA foreign_keys = ON")
            yield connection
        finally:
            connection.close()

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        """Yield one connection in a transaction, committing only on success."""

        with self.connect() as connection:
            try:
                connection.execute("BEGIN")
                yield connection
            except BaseException:
                connection.rollback()
                raise
            else:
                connection.commit()

    def initialize(self) -> None:
        """Create the initial schema safely; calling this repeatedly is harmless."""

        self._create_parent_directory()
        with self.transaction() as connection:
            for statement in SCHEMA_STATEMENTS:
                connection.execute(statement)

    def _create_parent_directory(self) -> None:
        if self._database_path == ":memory:" or self._database_path.startswith("file:"):
            return
        Path(self._database_path).parent.mkdir(parents=True, exist_ok=True)


def initialize_database(database_path: str | Path) -> Database:
    """Initialize a database and return its ready-to-use wrapper."""

    database = Database(database_path)
    database.initialize()
    return database
