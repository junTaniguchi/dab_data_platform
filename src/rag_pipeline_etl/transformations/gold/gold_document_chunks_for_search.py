# Gold: 手法A(ai_prep_search) + 手法B(fixed_overlap) の統合テーブル
#
# chunk_id は common/chunk_id.py の compute_chunk_id（doc_path + chunk_method + chunk_index の
# sha256 ハッシュ）で生成する。chunk_method を必ずキーに含めることで、2つのチャンキング手法が
# 同じ chunk_index を独立に払い出しても chunk_id が衝突しない（重複バグ再発防止。
# tests/unit/test_gold_chunking_union.py で回帰テストしている）。
#
# classification / department は governance/abac_policies.sql の
# ABAC 行フィルタポリシーが参照する判定属性列としてそのまま保持する。
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from common.chunk_id import compute_chunk_id

compute_chunk_id_udf = F.udf(compute_chunk_id, StringType())


@dp.table(
    name="gold_document_chunks_for_search",
    comment="Vector Search index の source となる、検索用チャンク統合 Gold テーブル",
    table_properties={"quality": "gold"},
)
@dp.expect_or_drop("has_chunk_text", "chunk_text IS NOT NULL AND length(chunk_text) > 0")
@dp.expect_or_fail("unique_chunk_id_inputs", "doc_path IS NOT NULL AND chunk_method IS NOT NULL AND chunk_index IS NOT NULL")
def gold_document_chunks_for_search():
    chunks_a = dp.read("stg_chunks_ai_prep_search")
    chunks_b = dp.read("stg_chunks_fixed_overlap")

    unioned = chunks_a.unionByName(chunks_b)

    return unioned.select(
        compute_chunk_id_udf(
            F.col("doc_path"), F.col("chunk_method"), F.col("chunk_index")
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
