"""PII匿名化・仮名化の純粋関数（structured_common.pii）のユニットテスト。

pyspark非依存。実際の Silver 変換（silver_customers.py）では同じロジックを
Sparkの関数（sha2 / regexp_replace 等）で実装し直しているが、
hashlib.sha256(...).hexdigest() と Spark の sha2(col, 256) は同一の
SHA-256 16進ダイジェストを返すため値は完全に一致する。
"""
import hashlib

from structured_common.pii import (
    generalize_address_to_region,
    generalize_birth_date_to_year,
    hash_value,
    is_valid_email_format,
    mask_phone,
)


def test_hash_value_is_deterministic():
    assert hash_value("aiko.tanaka@example.com", "salt123") == hash_value(
        "aiko.tanaka@example.com", "salt123"
    )


def test_hash_value_differs_with_different_salt():
    assert hash_value("aiko.tanaka@example.com", "salt123") != hash_value(
        "aiko.tanaka@example.com", "salt456"
    )


def test_hash_value_matches_known_sha256_hexdigest():
    expected = hashlib.sha256("valuesalt".encode("utf-8")).hexdigest()
    assert hash_value("value", "salt") == expected


def test_mask_phone_keeps_first_and_last_block():
    assert mask_phone("090-1111-2222") == "090-****-2222"


def test_mask_phone_handles_missing_value():
    assert mask_phone(None) is None
    assert mask_phone("") == ""


def test_mask_phone_falls_back_for_unexpected_format():
    assert mask_phone("0901112222") == "****"


def test_generalize_birth_date_to_year():
    assert generalize_birth_date_to_year("1988-04-12") == "1988"


def test_generalize_birth_date_to_year_handles_missing_value():
    assert generalize_birth_date_to_year(None) is None


def test_generalize_address_to_region_is_passthrough_of_region():
    assert generalize_address_to_region("tokyo") == "tokyo"


def test_is_valid_email_format_accepts_well_formed_address():
    assert is_valid_email_format("aiko.tanaka@example.com") is True


def test_is_valid_email_format_rejects_missing_at_sign():
    # sample_data/structured/customers/customers_seed.csv の CUST003 と同じ壊れ方
    assert is_valid_email_format("yuki.yamamoto.example.com") is False


def test_is_valid_email_format_rejects_empty_or_none():
    assert is_valid_email_format(None) is False
    assert is_valid_email_format("") is False
