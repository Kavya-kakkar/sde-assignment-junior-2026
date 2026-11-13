from __future__ import annotations

from pathlib import Path

import pytest

from interaction_ledger.config import (
    ConfigurationError,
    load_assignment_configuration,
    load_configuration,
)


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SCHEMA_DIRECTORY = PROJECT_ROOT / "assignment-a-data" / "schemas"


def test_loads_both_assignment_workflows() -> None:
    configuration = load_assignment_configuration(PROJECT_ROOT)

    assert set(configuration.workflows) == {"motor_renewal", "policy_feedback"}
    assert configuration.workflows["motor_renewal"].reference_field.name == "policy_number"
    assert configuration.workflows["policy_feedback"].reference_field.name == "order_id"
    assert configuration.workflows["motor_renewal"].phone_field.pii is True
    assert configuration.workflows["policy_feedback"].phone_field.required is True


def test_loads_workflow_scoped_disposition_catalogue() -> None:
    configuration = load_assignment_configuration(PROJECT_ROOT)

    renewed = configuration.disposition_for("motor_renewal", "renewed")
    feedback = configuration.disposition_for("policy_feedback", "feedback_captured")

    assert renewed is not None
    assert renewed.is_terminal is True
    assert renewed.is_success is True
    assert feedback is not None
    assert feedback.is_terminal is True
    assert feedback.is_success is True
    assert configuration.disposition_for("policy_feedback", "renewed") is None


def test_missing_workflow_schema_has_a_clear_error(tmp_path: Path) -> None:
    with pytest.raises(ConfigurationError, match="does not exist"):
        load_configuration(
            [tmp_path / "missing.json"],
            SCHEMA_DIRECTORY / "dispositions.csv",
        )


def test_malformed_workflow_schema_has_a_clear_error(tmp_path: Path) -> None:
    malformed_schema = tmp_path / "invalid.json"
    malformed_schema.write_text("{not json", encoding="utf-8")

    with pytest.raises(ConfigurationError, match="not valid JSON"):
        load_configuration(
            [malformed_schema],
            SCHEMA_DIRECTORY / "dispositions.csv",
        )
