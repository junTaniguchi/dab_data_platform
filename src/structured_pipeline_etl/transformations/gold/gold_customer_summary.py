# Gold: 顧客ごとの購買サマリ（Materialized View）。
#
# レイヤー設計:
#   - table/view : Materialized View
#   - 入力        : silver_customers の「現在状態」のみ（SCD Type 2 の __END_AT IS NULL）
#                  + silver_orders の顧客別集計
#   - Row Filter  : governance/structured_governance.sql が「退職者・解約済み顧客
#                  (status = 'CHURNED') のデータ」を retention-team 以外から隠す。
#   - Column Mask : governance/structured_governance.sql が discount_rate
#                  （取引先ごとの値引き率）を pricing-team 以外には NULL として返す。
#   - 保持期間     : 1年以上
#
# NOTE: __START_AT / __END_AT は AUTO CDC（SCD Type 2）が自動付与するシステム列。
# channel: PREVIEW を使用しているため列名が変わる可能性がある。実際にデプロイして
# `DESCRIBE silver_customers` で列名を確認すること。
from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="gold_customer_summary",
    comment="顧客別の購買サマリ（Row Filterで解約済み顧客を、Column Maskで値引き率を保護）",
    table_properties={
        "quality": "gold",
        "delta.enableChangeDataFeed": "true",
        "delta.deletedFileRetentionDuration": "interval 365 days",
        "delta.logRetentionDuration": "interval 365 days",
    },
)
def gold_customer_summary():
    current_customers = dp.read("silver_customers").filter(F.col("__END_AT").isNull())

    order_stats = dp.read("silver_orders").groupBy("customer_id").agg(
        F.count("order_id").alias("order_count"),
        F.sum("amount").alias("total_spend"),
        F.round(F.avg("discount_rate"), 4).alias("discount_rate"),
        F.max("order_date").alias("last_order_date"),
    )

    return current_customers.join(order_stats, on="customer_id", how="left").select(
        current_customers["customer_id"],
        current_customers["name"],
        current_customers["email_hash"],
        current_customers["phone_masked"],
        current_customers["address_region"],
        current_customers["birth_year"],
        current_customers["status"],
        F.coalesce(order_stats["order_count"], F.lit(0)).alias("order_count"),
        F.coalesce(order_stats["total_spend"], F.lit(0.0)).alias("total_spend"),
        order_stats["discount_rate"],
        order_stats["last_order_date"],
    )
