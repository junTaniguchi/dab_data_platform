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
# 【実機で遭遇した落とし穴: パイプライン関数内で .count() のような即時アクションを
# 呼んではいけない】
# 当初は `valid_count = dp.read("silver_orders").count()` のように、関数の中で
# Python側の即時アクション（.count()）を呼び、その結果を F.lit(...) でカラムに
# 埋め込んでいた。実際にデプロイして中身を確認すると、valid_count/quarantine_count/
# total_count が常に 0、pass_rate_pct が常に NULL になっていた（実際の件数は
# silver_orders=7件、silver_orders_quarantine=3件だったにもかかわらず）。
# Lakeflow のパイプライン関数はグラフ構築・解析の過程で複数回呼ばれることがあり、
# その時点でまだ上流テーブルにデータが無い（あるいは実行順序が期待通りでない）
# 段階で `.count()` が評価されてしまうと考えられる。
# 教訓: パイプライン関数の中では、Python側の即時アクション（.count() 等）で
# 値を確定させてから `F.lit(...)` で埋め込むのではなく、**最後まで遅延評価の
# DataFrame操作（集計・crossJoin等）として組み立てる**こと。そうすれば
# Lakeflowが実際にテーブルを再計算するタイミングで正しい値が計算される。
#
# 既知の制約: 検疫が0件の場合、per_rule が0行になり crossJoin の結果も0行になる
# （pass_rate_pct=100%という行すら出ない）。運用で「0件でも必ず1行出したい」場合は
# per_rule が空のときのフォールバック行（例: LEFT側にダミーの1行DataFrameを置き
# `per_rule` をLEFT JOINする）を追加すること。
from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="gold_data_quality_summary",
    comment="受注データの検疫状況サマリ（ルール別違反件数、全体Pass Rate）",
    table_properties={"quality": "gold"},
)
def gold_data_quality_summary():
    valid_counts = dp.read("silver_orders").agg(F.count(F.lit(1)).alias("valid_count"))

    quarantine = dp.read("silver_orders_quarantine")
    quarantine_counts = quarantine.agg(F.count(F.lit(1)).alias("quarantine_count"))

    per_rule = (
        quarantine.select(F.explode("violated_rules").alias("violated_rule"))
        .groupBy("violated_rule")
        .agg(F.count(F.lit(1)).alias("violation_count"))
    )

    # 1行×1行の定数DataFrame同士の crossJoin なので、per_rule の行数を増やさない。
    totals = (
        valid_counts.crossJoin(quarantine_counts)
        .withColumn("total_count", F.col("valid_count") + F.col("quarantine_count"))
        .withColumn(
            "pass_rate_pct",
            F.when(
                F.col("total_count") > 0,
                F.round(F.col("valid_count") / F.col("total_count") * 100, 2),
            ),
        )
    )

    return per_rule.crossJoin(totals).withColumn(
        "summary_generated_at", F.current_timestamp()
    )
