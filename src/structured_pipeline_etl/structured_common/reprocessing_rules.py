"""検疫データの是正判定ロジック（pyspark非依存の純粋関数）。

reprocessing/reprocess_quarantine.py から呼ばれる、「どのルール違反なら
自動補正できるか」の唯一の判断基準。ここを変更すれば是正ジョブの挙動が変わる
（is_valid_email_format 等と同じく、単体テストで固定できる小さな純粋関数として
切り出している）。
"""
from __future__ import annotations

import datetime

# 自動補正できるルール（機械的に妥当な値へ書き換えられるもの）とその補正内容の説明。
CORRECTABLE_RULES: dict[str, str] = {
    "positive_amount": "amount の符号を反転して正の値にする（絶対値を採用）",
    "order_date_not_future": "order_date を実行日(today)にクリップする",
}

# 自動補正できないルール（値を機械的に推測できないため、ソース側の確認が必要）。
UNCORRECTABLE_RULES: dict[str, str] = {
    "customer_id_not_null": "顧客IDが欠落しており自動補正不可。ソースシステム側の確認が必要。",
    "valid_currency": "通貨コードが不正であり自動補正不可。ソースシステム側の確認が必要。",
}


def decide_resolution(violated_rules: list[str]) -> str:
    """違反ルールの一覧から、是正結果ステータス（CORRECTED / UNCORRECTABLE）を決める。

    1つでも UNCORRECTABLE_RULES に含まれる違反があれば、他が補正可能でも
    全体として UNCORRECTABLE 扱いにする（部分的な補正はしない。1行につき
    "全部直すか、直さず人手に回すか" の二択にすることで、中途半端に補正された
    行が紛れ込むことを防ぐ）。
    未知のルール名（CORRECTABLE_RULES にも UNCORRECTABLE_RULES にも無い）は
    安全側に倒して UNCORRECTABLE とする。
    """
    for rule in violated_rules:
        if rule in UNCORRECTABLE_RULES:
            return "UNCORRECTABLE"
    for rule in violated_rules:
        if rule not in CORRECTABLE_RULES:
            return "UNCORRECTABLE"
    return "CORRECTED"


def correction_note(violated_rules: list[str], resolution: str) -> str:
    """是正結果の理由コメントを組み立てる（quarantine_resolution_log.correction_note用）。"""
    if resolution == "CORRECTED":
        return "; ".join(CORRECTABLE_RULES[rule] for rule in violated_rules)
    notes = [UNCORRECTABLE_RULES[rule] for rule in violated_rules if rule in UNCORRECTABLE_RULES]
    if not notes:
        notes = [f"未知のルール違反のため自動補正不可: {', '.join(violated_rules)}"]
    return "; ".join(notes)


def apply_correction(order: dict, violated_rules: list[str]) -> dict:
    """1件の受注レコード（dict）へ、判明している補正ロジックを適用した新しいdictを返す。

    decide_resolution() が "CORRECTED" と判定した行にのみ使うこと。
    UNCORRECTABLE な行にこの関数を呼んでも安全に倒すため補正は一切行わない
    （呼び出し側は decide_resolution の結果に従い、UNCORRECTABLEなら
    そもそもこの関数を呼ばない設計にすること）。
    """
    corrected = dict(order)
    if "positive_amount" in violated_rules:
        corrected["amount"] = abs(float(corrected["amount"]))
    if "order_date_not_future" in violated_rules:
        corrected["order_date"] = datetime.date.today().isoformat()
    return corrected
