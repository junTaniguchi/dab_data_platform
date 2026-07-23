"""chunk_id 生成ロジック（純粋関数、pyspark非依存）。

doc_path + chunk_method + chunk_index から sha256 ハッシュを生成する。
chunk_method を必ずキーに含めることで、手法A(ai_prep_search)と手法B(fixed_overlap)が
同じ chunk_index を独立に払い出しても chunk_id が衝突しないようにする
（Gold統合時の重複バグ再発防止。tests/unit/test_gold_chunking_union.py で回帰テスト）。
"""
import hashlib


def compute_chunk_id(doc_path: str, chunk_method: str, chunk_index: int) -> str:
    key = f"{doc_path}-{chunk_method}-{chunk_index}"
    return hashlib.sha256(key.encode("utf-8")).hexdigest()
