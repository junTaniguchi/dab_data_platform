# Gold: 検疫状況の観測用サマリ（Materialized View）。
#
# 「今の品質はどうか」だけでなく「悪化し始めているか」を継続観測できるようにする
# ためのビュー。ルール別の違反件数（Violation Rate）と、正常系/検疫系を合わせた
# 全体の Pass Rate を、行ごとに繰り返し持たせたフラットなレポート用テーブルにする
# （BIツールからそのままグラフ化しやすいよう、あえて非正規化している）。
#
# quarantine_resolution_log（reprocessing/reprocess_quarantine.py が管理する、
# Lakeflow管理外の素のDeltaテーブル）はここでは意図的に結合しない。
# 理由: このテーブルは再処理ジョブを一度も実行していない状態（初回デプロイ直後）
# には存在せず、存在しないテーブルへの依存を宣言的パイプラインに持ち込むと
# 初回実行が失敗してしまう。「検疫件数のうち何件が是正済みか」を見たい場合は、
# README に記載した一時的なSQLクエリ（このMVと quarantine_resolution_log を
# 手動でJOINするクエリ）を使うこと。
#
# 既知の制約: 検疫が0件の場合、per_rule が0行になりこのテーブル自体も0行になる
# （pass_rate_pct=100%という行すら出ない）。運用で「0件でも必ず1行出したい」場合は
# per_rule が空のときのフォールバック行を追加すること。
from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="gold_data_quality_summary",
    comment="受注データの検疫状況サマリ（ルール別違反件数、全体Pass Rate）",
    table_properties={"quality": "gold"},
)
def gold_data_quality_summary():
    valid_count = dp.read("silver_orders").count()

    quarantine = dp.read("silver_orders_quarantine")
    quarantine_count = quarantine.count()
    total_count = valid_count + quarantine_count
    pass_rate_pct = round((valid_count / total_count) * 100, 2) if total_count > 0 else None

    per_rule = (
        quarantine.select(F.explode("violated_rules").alias("violated_rule"))
        .groupBy("violated_rule")
        .agg(F.count("*").alias("violation_count"))
    )

    return (
        per_rule.withColumn("valid_count", F.lit(valid_count))
        .withColumn("quarantine_count", F.lit(quarantine_count))
        .withColumn("total_count", F.lit(total_count))
        .withColumn("pass_rate_pct", F.lit(pass_rate_pct))
        .withColumn("summary_generated_at", F.current_timestamp())
    )
