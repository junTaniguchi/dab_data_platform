# Silver: Bronze の生ファイルからテキストを抽出する
#
# - PDF / 画像等のバイナリ文書は Databricks AI Functions の ai_parse_document() でテキスト化する。
#   ai_parse_document は VARIANT を返し、`document.pages` はページのARRAY。
#   実際に SQL ウェアハウスで確認したところ、`ai_parse_document(content):document:pages` の
#   ような `:` パス記法だと `transform()` が要求する ARRAY 型ではなく VARIANT 型のまま
#   返ってきてしまい `[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE]` になった。
#   `variant_get(expr, path, targetType)` で明示的に `ARRAY<VARIANT>` / `STRING` に
#   キャストする必要がある。ページの本文が入るフィールド名（ここでは `$.content` と仮定）は
#   プレビュー機能のため変更されうるので、サポート対象のファイル形式（PDF等）を実際に
#   処理させて `src/rag_pipeline_etl/explorations/sample_exploration.ipynb` で
#   出力を確認し、必要なら調整すること。
# - サンプルデータの .txt / .md はプレーンテキストなので UTF-8 デコードのみで済ませる
#   （ai_parse_document は主に PDF / 画像 / Office 文書向けの機能のため。実際に .txt を
#   渡すと `"error_status":[{"error_message":"Unsupported file format: unknown"}]` になる）。
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# ai_parse_document の戻り値から本文相当のテキストを取り出す想定パス。
# 実際のワークスペースのスキーマに合わせて調整すること。
PARSED_TEXT_EXPR = (
    "array_join("
    "  transform("
    "    variant_get(ai_parse_document(raw_content), '$.document.pages', 'ARRAY<VARIANT>'),"
    "    page -> variant_get(page, '$.content', 'STRING')"
    "  ), "
    "  '\\n\\n'"
    ")"
)


@dp.table(
    name="silver_parsed_documents",
    comment="Bronze から本文テキストを抽出した Silver テーブル",
    table_properties={"quality": "silver"},
)
@dp.expect_or_drop("has_parsed_text", "parsed_text IS NOT NULL AND length(parsed_text) > 0")
def silver_parsed_documents():
    # bronze_documents.py と同じ理由（Lakeflow Direct Publishing 実行環境では __file__ が
    # 未定義）で、common への sys.path 追加とimportを関数内で行う。
    import sys

    sys.path.append(spark.conf.get("rag_src_root"))
    from common.text_extraction import TEXT_LIKE_EXTENSIONS

    bronze = dp.read_stream("bronze_documents")

    is_text_like = F.lower(F.col("file_extension")).isin(*TEXT_LIKE_EXTENSIONS)

    return bronze.select(
        "doc_path",
        "department",
        "classification",
        "file_name",
        "file_extension",
        "ingestion_time",
        F.when(is_text_like, F.decode(F.col("raw_content"), "UTF-8").cast(StringType()))
        .otherwise(F.expr(PARSED_TEXT_EXPR))
        .alias("parsed_text"),
        F.when(is_text_like, F.lit("utf8_decode"))
        .otherwise(F.lit("ai_parse_document"))
        .alias("parse_method"),
    )
