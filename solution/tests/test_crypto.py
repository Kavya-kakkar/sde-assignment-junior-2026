from __future__ import annotations

import base64
import secrets

import pytest
from cryptography.fernet import Fernet

from interaction_ledger.crypto import (
    FERNET_KEY_ENV,
    HMAC_KEY_ENV,
    PiiProtector,
    SecretConfigurationError,
)


def _hmac_key() -> str:
    return base64.urlsafe_b64encode(secrets.token_bytes(32)).decode("ascii")


def _environment(*, hmac_key: str | None = None) -> dict[str, str]:
    return {
        FERNET_KEY_ENV: Fernet.generate_key().decode("ascii"),
        HMAC_KEY_ENV: hmac_key or _hmac_key(),
    }


def test_encrypt_decrypt_round_trip_and_no_plaintext_in_ciphertext() -> None:
    protector = PiiProtector.from_environment(_environment())
    plaintext = "Asha Menon, +919876543210"

    ciphertext = protector.encrypt_text(plaintext)

    assert protector.decrypt_text(ciphertext) == plaintext
    assert plaintext.encode("utf-8") not in ciphertext
    assert b"Asha" not in ciphertext
    assert b"9876543210" not in ciphertext


def test_phone_hmac_is_deterministic_and_uses_a_separate_key() -> None:
    first_hmac_key = _hmac_key()
    first = PiiProtector.from_environment(_environment(hmac_key=first_hmac_key))
    second = PiiProtector.from_environment(_environment(hmac_key=first_hmac_key))
    third = PiiProtector.from_environment(_environment(hmac_key=_hmac_key()))

    phone = "+919876543210"
    assert first.phone_hmac(phone) == first.phone_hmac(phone)
    assert first.phone_hmac(phone) == second.phone_hmac(phone)
    assert first.phone_hmac(phone) != third.phone_hmac(phone)


def test_missing_or_reused_environment_keys_fail_clearly() -> None:
    with pytest.raises(SecretConfigurationError, match=FERNET_KEY_ENV):
        PiiProtector.from_environment({})

    reused_key = Fernet.generate_key().decode("ascii")
    with pytest.raises(SecretConfigurationError, match="must be distinct"):
        PiiProtector.from_environment(
            {FERNET_KEY_ENV: reused_key, HMAC_KEY_ENV: reused_key}
        )
