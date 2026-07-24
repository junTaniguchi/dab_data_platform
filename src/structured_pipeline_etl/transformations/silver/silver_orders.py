# Silver: bronze_orders のうち ORDER_RULES を満たす行だけを残す「正常系」。
#
# これは検疫パターンの片翼にすぎない。もう片翼（違反行を捕捉する「検疫系」）は
# silver_orders_quarantine.py にある。両者は structured_common.quality_rules.ORDER_RULES
# という同じ辞書を参照しているため、
#     silver_orders (このテーブル) の行数 + silver_orders_quarantine の行数
#     == bronze_orders の行数
# が常に成り立つ（=Dropされた行が「消えた」のではなく「隔離された」ことを保証する）。
#
# レイヤー設計:
#   - Expectation      : Drop（品質管理の主戦場。NULL/範囲外/フォーマット不正を除外）
#   - Deletion Vectors : 有効化
#   - 保持期間          : 90〜180日
# NOTE: @dp.expect_all_or_drop(ORDER_RULES) はデコレータなので、ORDER_RULES は
# このモジュールがロードされる時点（decoratorの引数評価時点）で束縛済みで
# なければならない。そのため sys.path 追加・import を関数の中まで遅延できず、
# モジュールのトップレベルで spark.conf.get(...) を呼んでいる。
# bronze_documents.py 等の既存実装は常に「関数の中」でのみ spark を参照して
# おり、モジュールのトップレベル（関数の外）で spark が利用可能かは実機で
# 未検証である。実際にデプロイして本ファイルのロードで
# `NameError: name 'spark' is not defined` が出た場合は、silver_customers.py の
# CUSTOMER_RULES と同様に ORDER_RULES の内容をこのファイルへ直接インライン化
# すること（その場合は silver_orders_quarantine.py 側のコピーと手動で同期を
# 保つ必要がある。structured_common/quality_rules.py の docstring 参照）。
from pyspark import pipelines as dp

import sys

sys.path.append(spark.conf.get("structured_src_root"))
from structured_common.quality_rules import ORDER_RULES


@dp.table(
    name="silver_orders",
    comment="ORDER_RULES を満たす受注データ（検疫済み・正常系）。silver_orders_quarantine と対になる。",
    table_properties={
        "quality": "silver",
        "delta.enableDeletionVectors": "true",
        "delta.deletedFileRetentionDuration": "interval 180 days",
        "delta.logRetentionDuration": "interval 180 days",
    },
)
@dp.expect_all_or_drop(ORDER_RULES)
def silver_orders():
    return dp.read_stream("bronze_orders")
