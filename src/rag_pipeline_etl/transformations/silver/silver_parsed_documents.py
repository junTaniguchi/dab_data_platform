# Silver: Bronze の生ファイルからテキストを抽出する
#
# - PDF / 画像等のバイナリ文書は Databricks AI Functions の ai_parse_document() でテキスト化する。
#   ai_parse_document の戻り値スキーマはプレビュー機能のため変更されうる。ここでは
#   `parsed:document:pages` 配下の各ページ本文を改行区切りで連結する想定で実装しているので、
#   実行前に自分のワークスペースの ai_parse_document 出力スキーマ（DESCRIBE等）で
#   フィールド名を確認し、PARSED_TEXT_PATH を調整すること。
# - サンプルデータの .txt / .md はプレーンテキストなので UTF-8 デコードのみで済ませる
#   （ai_parse_document は主に PDF / 画像 / Office 文書向けの機能のため）。
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from common.text_extraction import TEXT_LIKE_EXTENSIONS

# ai_parse_document の戻り値から本文相当のテキストを取り出す想定パス。
# 実際のワークスペースのスキーマに合わせて調整すること。
PARSED_TEXT_EXPR = (
    "array_join("
    "  transform(ai_parse_document(raw_content):document:pages, page -> page:content), "
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
