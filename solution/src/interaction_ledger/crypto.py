"""Authenticated encryption and deterministic matching for PII."""

from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import os
from collections.abc import Mapping
from dataclasses import dataclass, field

from cryptography.fernet import Fernet, InvalidToken


FERNET_KEY_ENV = "INTERACTION_LEDGER_FERNET_KEY"
HMAC_KEY_ENV = "INTERACTION_LEDGER_HMAC_KEY"
_MINIMUM_HMAC_KEY_BYTES = 32


class SecretConfigurationError(RuntimeError):
    """Raised when the PII-protection keys are missing or invalid."""


class PiiProtectionError(ValueError):
    """Raised when a PII value cannot be encrypted or decrypted safely."""


@dataclass(frozen=True, slots=True)
class PiiProtector:
    """Encrypt recoverable PII and derive deterministic phone identifiers.

    Callers should normalise phones before creating an HMAC. The encryption and
    HMAC keys are deliberately independent environment variables so a lookup
    key cannot decrypt stored PII.
    """

    _fernet: Fernet = field(repr=False)
    _hmac_key: bytes = field(repr=False)

    @classmethod
    def from_environment(
        cls, environ: Mapping[str, str] | None = None
    ) -> "PiiProtector":
        """Create a protector from the required environment variables.

        ``INTERACTION_LEDGER_HMAC_KEY`` must be URL-safe base64 text encoding
        at least 32 random bytes. It must not reuse the Fernet key value.
        """

        environment = os.environ if environ is None else environ
        fernet_key_text = _required_secret(environment, FERNET_KEY_ENV)
        hmac_key_text = _required_secret(environment, HMAC_KEY_ENV)

        if hmac_key_text == fernet_key_text:
            raise SecretConfigurationError(
                f"{HMAC_KEY_ENV} must be distinct from {FERNET_KEY_ENV}."
            )

        try:
            fernet_key = fernet_key_text.encode("ascii")
        except UnicodeEncodeError as error:
            raise SecretConfigurationError(
                f"{FERNET_KEY_ENV} must contain an ASCII Fernet key."
            ) from error

        try:
            fernet = Fernet(fernet_key)
        except (TypeError, ValueError) as error:
            raise SecretConfigurationError(
                f"{FERNET_KEY_ENV} is not a valid Fernet key."
            ) from error

        hmac_key = _decode_hmac_key(hmac_key_text)
        return cls(_fernet=fernet, _hmac_key=hmac_key)

    def encrypt_text(self, plaintext: str) -> bytes:
        """Return a Fernet token for a UTF-8 PII value."""

        if not isinstance(plaintext, str):
            raise PiiProtectionError("PII plaintext must be a string.")
        return self._fernet.encrypt(plaintext.encode("utf-8"))

    def decrypt_text(self, ciphertext: bytes | bytearray) -> str:
        """Recover a UTF-8 PII value and reject invalid authenticated tokens."""

        if not isinstance(ciphertext, (bytes, bytearray)):
            raise PiiProtectionError("PII ciphertext must be bytes.")
        try:
            return self._fernet.decrypt(bytes(ciphertext)).decode("utf-8")
        except InvalidToken as error:
            raise PiiProtectionError(
                "Unable to decrypt PII ciphertext; it is invalid or uses another key."
            ) from error
        except UnicodeDecodeError as error:
            raise PiiProtectionError("Decrypted PII is not valid UTF-8 text.") from error

    def phone_hmac(self, canonical_phone: str) -> str:
        """Return the SHA-256 HMAC used for deterministic phone matching."""

        if not isinstance(canonical_phone, str) or not canonical_phone:
            raise PiiProtectionError("A non-empty canonical phone string is required.")
        return hmac.new(
            self._hmac_key,
            canonical_phone.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()


def _required_secret(environ: Mapping[str, str], name: str) -> str:
    value = environ.get(name)
    if not isinstance(value, str) or not value:
        raise SecretConfigurationError(
            f"Missing required environment variable: {name}."
        )
    return value


def _decode_hmac_key(value: str) -> bytes:
    try:
        encoded = value.encode("ascii")
        decoded = base64.b64decode(encoded, altchars=b"-_", validate=True)
    except (UnicodeEncodeError, binascii.Error) as error:
        raise SecretConfigurationError(
            f"{HMAC_KEY_ENV} must be URL-safe base64-encoded random key material."
        ) from error
    if len(decoded) < _MINIMUM_HMAC_KEY_BYTES:
        raise SecretConfigurationError(
            f"{HMAC_KEY_ENV} must decode to at least {_MINIMUM_HMAC_KEY_BYTES} bytes."
        )
    return decoded
