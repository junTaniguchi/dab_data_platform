"""検疫の是正判定ロジック（structured_common.reprocessing_rules）のユニットテスト。

reprocess_quarantine.py が実際に使うロジックそのものをテストする
（pyspark非依存の純粋関数のため、実ワークスペースなしで検証できる）。
"""
import datetime

from structured_common.reprocessing_rules import (
    apply_correction,
    correction_note,
    decide_resolution,
)


def test_decide_resolution_single_correctable_rule():
    assert decide_resolution(["positive_amount"]) == "CORRECTED"


def test_decide_resolution_multiple_correctable_rules():
    assert decide_resolution(["positive_amount", "order_date_not_future"]) == "CORRECTED"


def test_decide_resolution_uncorrectable_rule():
    assert decide_resolution(["customer_id_not_null"]) == "UNCORRECTABLE"


def test_decide_resolution_mixed_correctable_and_uncorrectable_is_uncorrectable():
    # 一部が補正可能でも、1つでも補正不能なルールがあれば全体としてUNCORRECTABLE
    # （中途半端な補正はしない設計）
    assert decide_resolution(["positive_amount", "customer_id_not_null"]) == "UNCORRECTABLE"


def test_decide_resolution_unknown_rule_defaults_to_uncorrectable():
    # 未知のルール名は安全側に倒す
    assert decide_resolution(["some_future_rule_not_yet_classified"]) == "UNCORRECTABLE"


def test_apply_correction_fixes_negative_amount():
    order = {"order_id": "ORD1003", "amount": -500.0}
    corrected = apply_correction(order, ["positive_amount"])
    assert corrected["amount"] == 500.0
    # 元のdictは変更しない
    assert order["amount"] == -500.0


def test_apply_correction_clips_future_order_date_to_today():
    order = {"order_id": "ORD1005", "order_date": "2099-01-01"}
    corrected = apply_correction(order, ["order_date_not_future"])
    assert corrected["order_date"] == datetime.date.today().isoformat()


def test_apply_correction_leaves_unrelated_fields_untouched():
    order = {"order_id": "ORD1003", "amount": -500.0, "currency": "JPY"}
    corrected = apply_correction(order, ["positive_amount"])
    assert corrected["currency"] == "JPY"
    assert corrected["order_id"] == "ORD1003"


def test_correction_note_for_correctable_rule_describes_the_fix():
    note = correction_note(["positive_amount"], "CORRECTED")
    assert "絶対値" in note


def test_correction_note_for_uncorrectable_rule_mentions_manual_followup():
    note = correction_note(["customer_id_not_null"], "UNCORRECTABLE")
    assert "顧客ID" in note
