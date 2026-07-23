# Gold: Genie Space から参照する集計ビュー（STEP 05）
#
# 「部署ごとに何件チャンクがあるか」「手法A/Bでチャンク数・平均長はどう違うか」といった
# 自然言語での質問に Genie Space が答えられるよう、あらかじめビジネス集計を用意しておく。
from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(
    name="gold_chunk_metrics_by_department",
    comment="部署・機密レベル別のチャンク集計（Genie Space 用）",
    table_properties={"quality": "gold"},
)
def gold_chunk_metrics_by_department():
    gold = dp.read("gold_document_chunks_for_search")

    return gold.groupBy("department", "classification").agg(
        F.countDistinct("doc_path").alias("document_count"),
        F.count("chunk_id").alias("chunk_count"),
        F.round(F.avg("chunk_length"), 1).alias("avg_chunk_length"),
        F.max("ingestion_time").alias("last_ingested_at"),
    )


@dp.table(
    name="gold_chunk_metrics_by_method",
    comment="チャンキング手法（ai_prep_search vs fixed_overlap）別の集計（Genie Space 用）",
    table_properties={"quality": "gold"},
)
def gold_chunk_metrics_by_method():
    gold = dp.read("gold_document_chunks_for_search")

    return gold.groupBy("chunk_method").agg(
        F.countDistinct("doc_path").alias("document_count"),
        F.count("chunk_id").alias("chunk_count"),
        F.round(F.avg("chunk_length"), 1).alias("avg_chunk_length"),
        F.round(F.stddev("chunk_length"), 1).alias("stddev_chunk_length"),
    )
