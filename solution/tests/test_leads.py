from pathlib import Path

import pytest

from interaction_ledger.config import load_assignment_configuration
from interaction_ledger.leads import (
    map_csv_headers,
    normalize_date,
    normalize_decimal,
    normalize_phone,
    normalize_value,
    parse_lead_row,
)


ROOT = Path(__file__).resolve().parents[2]
CONFIG = load_assignment_configuration(ROOT)


def test_indian_phone_normalization():
    assert normalize_phone("9932302346") == "+919932302346"
    assert normalize_phone("+91 99323 02346") == "+919932302346"
    assert normalize_phone("09932302346") == "+919932302346"


def test_invalid_phone_is_rejected():
    with pytest.raises(ValueError, match="invalid_phone"):
        normalize_phone("123456")


@pytest.mark.parametrize(
    "value, expected",
    [
        ("2026-11-23", "2026-11-23"),
        ("21/11/2026", "2026-11-21"),
        ("21-11-2026", "2026-11-21"),
        ("21-Nov-2026", "2026-11-21"),
    ],
)
def test_date_normalization(value, expected):
    assert normalize_date(value) == expected


def test_invalid_date_is_rejected():
    with pytest.raises(ValueError, match="invalid_date"):
        normalize_date("31-02-2026")


@pytest.mark.parametrize(
    "value, expected",
    [
        ("₹27,554", "27554.00"),
        ("INR 25301.4", "25301.40"),
        ("Rs. 6850", "6850.00"),
    ],
)
def test_decimal_normalization(value, expected):
    assert normalize_decimal(value) == expected


def test_negative_decimal_is_rejected():
    with pytest.raises(ValueError, match="invalid_decimal"):
        normalize_decimal("-100")


def test_csv_alias_mapping():
    schema = CONFIG.workflows["motor_renewal"]
    mapping = map_csv_headers(
        ["Insured Name", "Mobile No", "Policy No", "Premium"],
        schema,
    )

    assert mapping == {
        "Insured Name": "customer_name",
        "Mobile No": "phone",
        "Policy No": "policy_number",
        "Premium": "premium_amount",
    }


def test_optional_invalid_enum_becomes_warning():
    schema = CONFIG.workflows["motor_renewal"]
    mapping = map_csv_headers(
        [
            "customer_name",
            "phone",
            "policy_number",
            "preferred_language",
        ],
        schema,
    )
    row = {
        "customer_name": "Rohan Sharma",
        "phone": "8880395512",
        "policy_number": "MOT/2026/100387",
        "preferred_language": "Hindi",
    }

    normalized, warnings = parse_lead_row(row, mapping, schema)

    assert normalized["preferred_language"] is None
    assert warnings == ["preferred_language"]


def test_required_phone_missing_is_rejected():
    schema = CONFIG.workflows["motor_renewal"]
    field = schema.phone_field

    with pytest.raises(ValueError, match="missing_required:phone"):
        normalize_value("", field)