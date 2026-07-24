# Gold: 地域・日付・ステータス別の売上サマリ（Materialized View）。
#
# レイヤー設計:
#   - table/view : Materialized View（Goldは基本MV。Enzyme増分更新エンジンが
#                  Silverの Deletion Vectors + Change Data Feed を前提に安く
#                  差分再計算するため、silver_orders 側で DV を有効化済み）
#   - Expectation : ここでは行レベルの厳格な Fail は設けない
#                  （集計後のため「重複キー」等はgold_order_quality_gateで
#                  別途チェックする。詳細は12.6参照）
#   - Row Filter  : governance/structured_governance.sql が
#                  「region列に基づく地域別アクセス制御」「DRAFT状態(承認前)取引の
#                  非表示」をこのテーブルに適用する（クエリ時の動的フィルタであり、
#                  ここでは何も行わない。ストレージ上の値は素のまま）。
#   - 保持期間     : 1年以上
from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="gold_daily_sales_by_region",
    comment="日付・地域・ステータス別の売上集計（Row Filterで地域/承認状態に応じたアクセス制御を適用）",
    table_properties={
        "quality": "gold",
        "delta.enableChangeDataFeed": "true",
        "delta.deletedFileRetentionDuration": "interval 365 days",
        "delta.logRetentionDuration": "interval 365 days",
    },
)
def gold_daily_sales_by_region():
    orders = dp.read("silver_orders")

    return orders.groupBy("order_date", "region", "status").agg(
        F.count("order_id").alias("order_count"),
        F.sum("amount").alias("total_amount"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.round(F.avg("discount_rate"), 4).alias("avg_discount_rate"),
        F.max("ingestion_time").alias("last_ingested_at"),
    )
