"""Interaction Ledger backend foundation.

This package currently provides configuration loading, SQLite schema setup, and
PII protection primitives. Ingestion and reporting are intentionally deferred.
"""

__all__ = ["config", "crypto", "database"]
