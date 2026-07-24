"""Silver層の品質ルール（ORDER_RULES）が、サンプルデータに対して意図通り
「検疫対象」を判定できることを確認する回帰テスト。

Sparkは使わず、ORDER_RULESの各キーに対応するPython版の判定ロジックをこの
テスト内に直接実装し、実際のサンプルJSONへ適用する。Spark SQL式の構文自体は
ここでは検証しない（実際にワークスペースへデプロイして確認すること）。

このテストの目的は「検疫（silver_orders_quarantine）＋正常系（silver_orders）
を合わせれば bronze_orders の全行になる」という、検疫パターンの中核的な
性質（データの取りこぼしが無いこと）をサンプルデータ単位で固定しておくこと。
"""
import datetime
import json
from pathlib import Path

from structured_common.quality_rules import ORDER_RULES

SAMPLE_ORDERS_PATH = (
    Path(__file__).resolve().parents[2]
    / "sample_data"
    / "structured"
    / "orders"
    / "orders_seed.json"
)

EXPECTED_QUARANTINE_IDS = {"ORD1003", "ORD1004", "ORD1005"}
EXPECTED_VALID_IDS = {"ORD1001", "ORD1002", "ORD1006", "ORD1007", "ORD1008", "ORD1009", "ORD1010"}


def _load_sample_orders() -> list[dict]:
    with SAMPLE_ORDERS_PATH.open(encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


def _passes_all_rules(order: dict) -> bool:
    """ORDER_RULES と同じ意図を Python で再現した判定（Sparkの F.expr(...) の代わり）。

    ORDER_RULES を変更した場合は、このチェック内容も対応するキーを
    追加/削除して更新すること（下の assert がキー名の乖離を検知する）。
    """
    checks = {
        "customer_id_not_null": order.get("customer_id") is not None,
        "positive_amount": (order.get("amount") or 0) > 0,
        "order_date_not_future": order["order_date"] <= datetime.date.today().isoformat(),
        "valid_currency": order.get("currency") in ("JPY", "USD"),
    }
    assert set(checks.keys()) == set(ORDER_RULES.keys()), (
        "このテストのチェック実装が structured_common.quality_rules.ORDER_RULES "
        "と乖離しています。ORDER_RULES を変更したら _passes_all_rules も更新すること。"
    )
    return all(checks.values())


def test_order_rules_keys_are_stable():
    assert set(ORDER_RULES.keys()) == {
        "customer_id_not_null",
        "positive_amount",
        "order_date_not_future",
        "valid_currency",
    }


def test_sample_orders_quarantine_set_matches_expected_ids():
    orders = _load_sample_orders()

    quarantined_ids = {order["order_id"] for order in orders if not _passes_all_rules(order)}

    assert quarantined_ids == EXPECTED_QUARANTINE_IDS


def test_sample_orders_valid_set_matches_expected_ids():
    orders = _load_sample_orders()

    valid_ids = {order["order_id"] for order in orders if _passes_all_rules(order)}

    assert valid_ids == EXPECTED_VALID_IDS


def test_valid_and_quarantine_sets_partition_all_sample_orders_without_loss():
    """検疫パターンの中核性質: 正常系 ∪ 検疫系 == 全件、正常系 ∩ 検疫系 == 空集合。"""
    orders = _load_sample_orders()
    all_ids = {order["order_id"] for order in orders}

    quarantined_ids = {order["order_id"] for order in orders if not _passes_all_rules(order)}
    valid_ids = {order["order_id"] for order in orders if _passes_all_rules(order)}

    assert quarantined_ids | valid_ids == all_ids
    assert quarantined_ids & valid_ids == set()
