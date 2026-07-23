# Gold（中間ビュー）手法A: ai_prep_search による検索用チャンキング
#
# ai_prep_search は Databricks AI Functions が提供する検索前処理関数で、本文テキストを
# 検索/埋め込みに適したチャンク列（chunk_text, chunk_index 等）へ変換する。
# プレビュー機能のため、戻り値の struct フィールド名は自分のワークスペースの
# `DESCRIBE FUNCTION EXTENDED ai_prep_search` 等で確認し、必要に応じて調整すること。
from pyspark import pipelines as dp
from pyspark.sql import functions as F

AI_PREP_SEARCH_EXPR = "ai_prep_search(parsed_text)"


@dp.view(
    name="stg_chunks_ai_prep_search",
    comment="手法A: ai_prep_search によるチャンキング結果（中間ビュー）",
)
def stg_chunks_ai_prep_search():
    silver = dp.read("silver_parsed_documents")

    return (
        silver.withColumn("chunks", F.expr(AI_PREP_SEARCH_EXPR))
        .withColumn("chunk", F.explode("chunks"))
        .select(
            "doc_path",
            "department",
            "classification",
            "file_name",
            "ingestion_time",
            F.col("chunk.chunk_text").alias("chunk_text"),
            F.col("chunk.chunk_index").cast("int").alias("chunk_index"),
            F.lit("ai_prep_search").alias("chunk_method"),
        )
    )
