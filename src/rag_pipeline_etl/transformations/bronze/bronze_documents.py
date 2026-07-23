# Bronze: UC Volume に配置された生ドキュメントファイルをそのまま取り込む
#
# 想定するボリュームレイアウト（sample_data/documents 参照）:
#   /Volumes/<catalog>/<schema>/raw_documents/<department>/<classification>/<file_name>
#
# department / classification はファイルパスから抽出し、そのまま Silver/Gold まで
# 引き継いで ABAC（行フィルタ）の判定属性列として使用する。
import sys
from pathlib import Path

# Lakeflow のソースファイルは独立したトップレベルモジュールとして実行されるため、
# 相対importではなく sys.path 経由で src/rag_pipeline_etl/common を明示的に参照する。
sys.path.append(str(Path(__file__).resolve().parents[2]))

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from common.path_parsing import RAW_PATH_PATTERN


@dp.table(
    name="bronze_documents",
    comment="raw_documents Volume 配下の生ドキュメントを取り込む Bronze テーブル",
    table_properties={"quality": "bronze"},
)
def bronze_documents():
    raw_volume_path = spark.conf.get("raw_volume_path")

    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "binaryFile")
        .option("pathGlobFilter", "*.*")
        .option("recursiveFileLookup", "true")
        .load(raw_volume_path)
        .select(
            F.col("path").alias("doc_path"),
            F.regexp_extract(F.input_file_name(), RAW_PATH_PATTERN, 1).alias("department"),
            F.regexp_extract(F.input_file_name(), RAW_PATH_PATTERN, 2).alias("classification"),
            F.element_at(F.split(F.col("path"), "/"), -1).alias("file_name"),
            F.element_at(F.split(F.col("path"), "\\."), -1).alias("file_extension"),
            F.col("content").alias("raw_content"),
            F.col("length").alias("file_size_bytes"),
            F.col("modificationTime").alias("source_modification_time"),
            F.current_timestamp().alias("ingestion_time"),
        )
    )
