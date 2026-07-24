# Gold: 受注データの重大品質ゲート（Fail）。
#
# Silver の Drop と Gold の Fail は意味がまったく異なる。
#   - Silver Drop : 個々の行の品質問題。壊れた行だけを検疫へ回し、パイプラインは
#                  正常に完了する（silver_orders_quarantine.py参照）。
#   - Gold Fail   : 集計・重大な不変条件（invariant）の違反。ここで検知される
#                  問題は「1行がおかしい」のではなく「パイプラインや上流の契約
#                  そのものが壊れている」ことを意味するため、黙って除外せず
#                  pipeline update 自体を失敗させ、on-call に気づかせる。
#
# 代表例が「重複キー」。silver_orders の時点では1行1行は ORDER_RULES を
# すべて満たしていても、同じ order_id が2件存在すれば、それは個別の行の品質問題
# ではなく「上流でイベントが重複配信された」というシステム上の異常である。
#
# デフォルトのサンプルデータには重複は含まれていない（初回デプロイでこのゲートが
# 失敗して驚かないように、という設計）。意図的に Fail 挙動を確認したい場合は、
# sample_data/structured/orders_incident/orders_incident_duplicate.json を
# raw_structured_data/orders/ へ手動でアップロードしてから再実行すると、
# ORD1002 が重複し、このテーブルの更新が意図的に失敗する
# （README「Gold Fail挙動を確認する」参照）。
# NOTE: @dp.expect_all_or_fail(GOLD_ORDER_GATE_RULES) はデコレータのため、
# GOLD_ORDER_GATE_RULES はモジュールロード時点で束縛済みである必要があり、
# sys.path 追加・import を関数の中まで遅延できない（silver_orders.py と同じ
# 制約。詳細はそちらのコメント参照）。実際にデプロイして本ファイルのロードで
# `NameError: name 'spark' is not defined` が出た場合は、GOLD_ORDER_GATE_RULES の
# 内容をこのファイルへ直接インライン化すること。
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

import sys

sys.path.append(spark.conf.get("structured_src_root"))
from structured_common.quality_rules import GOLD_ORDER_GATE_RULES


@dp.materialized_view(
    name="gold_order_quality_gate",
    comment="order_id 単位の重大品質ゲート。違反時は pipeline update を Fail させる。",
    table_properties={"quality": "gold"},
)
@dp.expect_all_or_fail(GOLD_ORDER_GATE_RULES)
def gold_order_quality_gate():
    orders = dp.read("silver_orders")
    dup_window = Window.partitionBy("order_id")

    return orders.withColumn("dup_count", F.count("*").over(dup_window)).select(
        "order_id", "customer_id", "amount", "dup_count"
    )
