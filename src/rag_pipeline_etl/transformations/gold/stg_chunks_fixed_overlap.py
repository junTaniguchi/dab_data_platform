# Gold（中間ビュー）手法B: 固定長 + オーバーラップによるチャンキング
#
# ai_prep_search（手法A）と比較するためのベースラインとして、単純な固定長スライディング
# ウィンドウでチャンク分割する。fixed_overlap_chunks は common/chunking.py の純粋関数で、
# tests/unit から直接 import してテストできる。ここでは UDF でラップして利用する。
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType

from common.chunking import fixed_overlap_chunks

fixed_overlap_chunks_udf = F.udf(fixed_overlap_chunks, ArrayType(StringType()))


@dp.view(
    name="stg_chunks_fixed_overlap",
    comment="手法B: 固定長+オーバーラップによるチャンキング結果（中間ビュー）",
)
def stg_chunks_fixed_overlap():
    silver = dp.read("silver_parsed_documents")

    return silver.withColumn(
        "chunks", fixed_overlap_chunks_udf(F.col("parsed_text"))
    ).select(
        "doc_path",
        "department",
        "classification",
        "file_name",
        "ingestion_time",
        F.posexplode("chunks").alias("chunk_index", "chunk_text"),
        F.lit("fixed_overlap").alias("chunk_method"),
    )
