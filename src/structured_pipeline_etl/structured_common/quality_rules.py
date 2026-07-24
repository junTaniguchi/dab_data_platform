"""Silver層の品質ルール定義（pyspark非依存の純粋な文字列定数）。

このモジュールが「検疫（Quarantine）」実装全体の要である。Lakeflow の Expectations
（`@dp.expect_or_drop` 等）は、違反した行を silently に drop する／pipeline を fail
させることはできるが、**drop された行そのものを後から取得できる queryable な場所を
提供しない**（イベントログには件数・集計は残るが、行データ自体は残らない）。

そのため「検疫」を実現するには、以下の2つの独立した flow を自分で用意する必要がある。

    1. silver_orders            : ORDER_RULES を満たす行だけを残す「正常系」
    2. silver_orders_quarantine : ORDER_RULES を満たさない行を明示的に拾う「検疫系」

この2つが同じ ORDER_RULES 辞書を参照することで、
    正常系に残る行 ∪ 検疫系に残る行 == Bronze の全行
が保証される（=サイレントなデータ消失が起きない）。ルールを追加・変更する際は
**この辞書だけ**を編集すればよく、正常系・検疫系のロジックが乖離する心配がない。

各ルールの値は Spark SQL のブール式（文字列）。
    - `@dp.expect_all_or_drop(ORDER_RULES)` にそのまま渡せる。
    - 検疫系では `~F.expr(predicate)` として同じ文字列を否定形で再利用する
      （transformations/silver/silver_orders_quarantine.py 参照）。
"""

# Bronze層: Warn（警告のみ、行は落とさない）。再処理不能なほど壊れたデータでも
# とりあえず保持し、傾向観測のためだけに使う。ここでは「観測用の緩いチェック」に
# とどめ、Drop/Failロジックはここには置かない。
BRONZE_ORDER_WARN_RULES: dict[str, str] = {
    "order_id_present": "order_id IS NOT NULL",
    "amount_present": "amount IS NOT NULL",
}

BRONZE_CUSTOMER_WARN_RULES: dict[str, str] = {
    "customer_id_present": "customer_id IS NOT NULL",
    "email_looks_like_email": "email IS NULL OR email RLIKE '.+@.+'",
}

# Silver層（orders）: Drop + 検疫。ここに書いたキーが違反したルール名として
# silver_orders_quarantine.violated_rules 配列にそのまま現れる。
#
# 【実機で遭遇した落とし穴: NULL評価の行は正常系・検疫系のどちらにも入らず消える】
# 当初 "amount > 0" のようにNULL非対応のルール式にしていたところ、
# amount=NULL の行が silver_orders にも silver_orders_quarantine にも
# 一切現れず、サイレントに消失することを実機で確認した
# （このコメントは当初「NULLはLakeflow Expectationsにより合格扱いされ
# silver_ordersへ通る」と誤って記載していたが、実際には
# @dp.expect_all_or_drop も silver_orders_quarantine.py の
# `bronze.filter(~all_rules_pass)` も、述語がNULLに評価される行を
# 「保持する」ではなく「除外する」という、SQLのWHERE句・Sparkの.filter()に
# 共通する3値論理の挙動を取る。つまり NULL は silver_orders 側からは
# 「合格ではない」として、quarantine側からは `~NULL` もNULLのため
# 「違反行ではない」として、**両方から除外される**。「正常系+検疫系
# ＝Bronzeの全行」という本パイプラインが前提とする不変条件は、
# ルール対象列がNULLになり得る限り保証されない）。
# そのため、NULLを見逃したくない列を参照するルールは必ず
# "amount IS NOT NULL AND amount > 0" のように明示的にNULLを弾く形で書く
# （CUSTOMER_RULES の valid_email_format と同じ書き方）。
# order_id_not_null / positive_amount はこの理由でNULL-safeにしている。
ORDER_RULES: dict[str, str] = {
    "order_id_not_null": "order_id IS NOT NULL",
    "customer_id_not_null": "customer_id IS NOT NULL",
    "positive_amount": "amount IS NOT NULL AND amount > 0",
    "order_date_not_future": "order_date <= current_date()",
    "valid_currency": "currency IN ('JPY', 'USD')",
}

# Silver層（customers）: Drop のみ（検疫テーブルは用意していない）。
# あえて検疫を作らない理由は README / silver_customers.py のコメントを参照。
# サンプルデータの CUST003（不正なメール形式）はこのルールにより Drop され、
# 「検疫を用意しないと、この顧客レコードは跡形もなく消える」ことを意図的に示す。
CUSTOMER_RULES: dict[str, str] = {
    "customer_id_not_null": "customer_id IS NOT NULL",
    "valid_operation": "operation IN ('INSERT', 'UPDATE', 'DELETE')",
    "updated_at_not_null": "updated_at IS NOT NULL",
    "valid_email_format": "email IS NOT NULL AND email RLIKE '^[^@\\\\s]+@[^@\\\\s]+\\\\.[^@\\\\s]+$'",
}

# Gold層: Fail（ゲート）。Silverの Drop とは意味が違う。
# ここに違反があるということは「個別レコードの品質問題」ではなく
# 「パイプライン／上流契約そのものが壊れている」ことを意味するため、
# 黙って除外するのではなく pipeline update 自体を失敗させ、on-call に気づかせる。
GOLD_ORDER_GATE_RULES: dict[str, str] = {
    "no_duplicate_order_id": "dup_count = 1",
    "amount_still_positive": "amount > 0",
    "customer_id_still_present": "customer_id IS NOT NULL",
}


def rule_names(rules: dict[str, str]) -> list[str]:
    """ルール名一覧を安定した順序で返す（テスト・ドキュメント生成用）。"""
    return list(rules.keys())
