# Gold: 手法A(ai_prep_search) + 手法B(fixed_overlap) の統合テーブル
#
# chunk_id は doc_path + chunk_method + chunk_index の sha256 ハッシュで生成する。
# chunk_method を必ずキーに含めることで、2つのチャンキング手法が同じ chunk_index を
# 独立に払い出しても chunk_id が衝突しない（重複バグ再発防止。
# tests/unit/test_gold_chunking_union.py で回帰テストしている）。
#
# 当初 common/chunk_id.py の compute_chunk_id を F.udf でラップして使っていたが、
# 実際にパイプラインを実行すると executor 側で UDF をデシリアライズする際に
# `ModuleNotFoundError: No module named 'common.chunk_id'` で失敗した。
# ドライバ側の sys.path.append は同一プロセス内でしか有効でなく、UDFのクロージャは
# cloudpickle で別プロセス（executor）に転送されるため、そちらには common パッケージが
# 存在せず import できない。そのため Python UDF をやめ、F.sha2 によるネイティブ Spark SQL
# 式に書き換えた（stg_chunks_fixed_overlap.py の chunking ロジックも同じ理由で書き換え済み）。
# common/chunk_id.py の compute_chunk_id は同じロジックの参照実装・単体テスト用として残しており、
# hashlib.sha256(...).hexdigest() と Spark の sha2(..., 256) は同じ SHA-256 hexダイジェストを
# 返すため、実際の値は完全に一致する。
#
# dp.read_stream を使う理由: 中間ビュー（stg_chunks_ai_prep_search / stg_chunks_fixed_overlap）を
# dp.read（バッチ）で読むと、Lakeflow はこのテーブルを実体を持つ Delta テーブルではなく
# MATERIALIZED_VIEW として作成する。Vector Search の delta_sync index は
# `DESCRIBE HISTORY` が使える実テーブル（Change Data Feed対応）を要求するため、
# MATERIALIZED_VIEW だと `[EXPECT_TABLE_NOT_VIEW.NO_ALTERNATIVE] ... expects a table but
# ... is a view` で index の同期が失敗する。upstream をすべて dp.read_stream にして
# ストリーミング経路にすることで、本テーブルは STREAMING_TABLE（実テーブル）として作成される。
from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(
    name="gold_document_chunks_for_search",
    comment="Vector Search index の source となる、検索用チャンク統合 Gold テーブル",
    # Vector Search の delta_sync index は Change Data Feed を要求する。未設定だと
    # `Source table ... is not a valid Vector Search source. Please retry after
    # enabling change data feed (delta.enableChangeDataFeed = true)` でindex作成に失敗する。
    table_properties={"quality": "gold", "delta.enableChangeDataFeed": "true"},
)
@dp.expect_or_drop("has_chunk_text", "chunk_text IS NOT NULL AND length(chunk_text) > 0")
@dp.expect_or_fail("unique_chunk_id_inputs", "doc_path IS NOT NULL AND chunk_method IS NOT NULL AND chunk_index IS NOT NULL")
def gold_document_chunks_for_search():
    chunks_a = dp.read_stream("stg_chunks_ai_prep_search")
    chunks_b = dp.read_stream("stg_chunks_fixed_overlap")

    unioned = chunks_a.unionByName(chunks_b)

    return unioned.select(
        F.sha2(
            F.concat_ws(
                "-", F.col("doc_path"), F.col("chunk_method"), F.col("chunk_index").cast("string")
            ),
            256,
        ).alias("chunk_id"),
        "doc_path",
        "file_name",
        "department",
        "classification",
        "chunk_method",
        "chunk_index",
        "chunk_text",
        F.length("chunk_text").alias("chunk_length"),
        "ingestion_time",
    )
