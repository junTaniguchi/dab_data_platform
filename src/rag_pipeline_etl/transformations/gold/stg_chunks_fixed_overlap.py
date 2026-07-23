# Gold（中間ビュー）手法B: 固定長 + オーバーラップによるチャンキング
#
# ai_prep_search（手法A）と比較するためのベースラインとして、単純な固定長スライディング
# ウィンドウでチャンク分割する。
#
# 当初 Python UDF（common/chunking.py の fixed_overlap_chunks を F.udf でラップ）で実装して
# いたが、実際にパイプラインを実行すると executor 側で UDF をデシリアライズする際に
# `ModuleNotFoundError: No module named 'common.chunking'` で失敗した。
# ドライバ側で `sys.path.append(...)` しても、それは同じ Python プロセス内でしか有効でなく、
# UDF のクロージャは cloudpickle で executor 側に転送されて別プロセスで実行されるため、
# そちらの sys.path には `common` パッケージが存在せず import できない。
# そのため Python UDF をやめ、`sequence` + `substring` による純粋な Spark SQL 式に書き換えた
# （sha2ベースの chunk_id 生成を UDF からネイティブ式に変えたのと同じ理由。
# gold_document_chunks_for_search.py 参照）。
# common/chunking.py の fixed_overlap_chunks はロジックの参照実装・単体テスト用として残しており、
# 下記の CHUNK_SIZE / OVERLAP と同じ値である限り出力は完全に一致する
# （tests/unit/test_gold_chunking_union.py で SQL 版と同一の境界条件を検証済み）。
from pyspark import pipelines as dp
from pyspark.sql import functions as F

CHUNK_SIZE = 800
OVERLAP = 200
STRIDE = CHUNK_SIZE - OVERLAP

FIXED_OVERLAP_CHUNKS_EXPR = (
    "CASE WHEN length(parsed_text) = 0 THEN array() ELSE "
    f"  transform("
    f"    sequence(0, length(parsed_text) - 1, {STRIDE}),"
    f"    start -> substring(parsed_text, start + 1, {CHUNK_SIZE})"
    f"  ) "
    "END"
)


@dp.view(
    name="stg_chunks_fixed_overlap",
    comment="手法B: 固定長+オーバーラップによるチャンキング結果（中間ビュー）",
)
def stg_chunks_fixed_overlap():
    # dp.read（バッチ）ではなく dp.read_stream にする理由は
    # gold_document_chunks_for_search.py 側のコメントを参照。
    silver = dp.read_stream("silver_parsed_documents")

    return silver.withColumn("chunks", F.expr(FIXED_OVERLAP_CHUNKS_EXPR)).select(
        "doc_path",
        "department",
        "classification",
        "file_name",
        "ingestion_time",
        F.posexplode("chunks").alias("chunk_index", "chunk_text"),
        F.lit("fixed_overlap").alias("chunk_method"),
    )
