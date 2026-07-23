# Gold（中間ビュー）手法A: ai_prep_search による検索用チャンキング
#
# ai_prep_search は Databricks AI Functions が提供する検索前処理関数で、本文テキストを
# 検索/埋め込みに適したチャンクへ変換する。SQLウェアハウスで実際に確認した戻り値は
# STRUCT等ではなく VARIANT で、成功時は概ね
#   {"document": {"contents": [{"content": "...", ...}, ...], "pages": [...]}, "error_status": null}
# という形（フィールド名はプレビュー機能のため変更されうる。実データ・実ドキュメントで
# `src/rag_pipeline_etl/explorations/sample_exploration.ipynb` を使って確認・調整すること）。
# `ai_prep_search(<空/非対応テキスト>)` はエラー時 `{"error_message": "...", "response": null}`
# のような別形状を返すが、その場合 variant_get は単に NULL を返し、transform/explode も
# 安全に「チャンク0件」になる（型不整合やクラッシュにはならないことを確認済み）。
from pyspark import pipelines as dp
from pyspark.sql import functions as F

AI_PREP_SEARCH_EXPR = (
    "transform("
    "  variant_get(ai_prep_search(parsed_text), '$.document.contents', 'ARRAY<VARIANT>'),"
    "  c -> variant_get(c, '$.content', 'STRING')"
    ")"
)


@dp.view(
    name="stg_chunks_ai_prep_search",
    comment="手法A: ai_prep_search によるチャンキング結果（中間ビュー）",
)
def stg_chunks_ai_prep_search():
    # dp.read（バッチ）ではなく dp.read_stream にする理由は
    # gold_document_chunks_for_search.py 側のコメントを参照。
    silver = dp.read_stream("silver_parsed_documents")

    return silver.withColumn("chunks", F.expr(AI_PREP_SEARCH_EXPR)).select(
        "doc_path",
        "department",
        "classification",
        "file_name",
        "ingestion_time",
        F.posexplode("chunks").alias("chunk_index", "chunk_text"),
        F.lit("ai_prep_search").alias("chunk_method"),
    )
