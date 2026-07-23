"""Gold統合（手法A: ai_prep_search + 手法B: fixed_overlap）の chunk_id 一意性テスト。

過去に、chunk_id を doc_path + chunk_index のみから生成していたため、2つのチャンキング
手法が同じ chunk_index を独立に払い出すと chunk_id が衝突する（=行が上書き/欠落する）
バグが発生した。chunk_method をハッシュキーに含めることで再発しないことを保証する。
"""
import pytest

from common.chunk_id import compute_chunk_id
from common.chunking import fixed_overlap_chunks


def test_same_doc_and_index_but_different_method_produce_distinct_ids():
    doc_path = "/Volumes/cat/schema/raw_documents/hr/internal/onboarding_guide.txt"

    id_from_method_a = compute_chunk_id(doc_path, "ai_prep_search", 0)
    id_from_method_b = compute_chunk_id(doc_path, "fixed_overlap", 0)

    assert id_from_method_a != id_from_method_b


def test_chunk_ids_are_unique_across_unioned_methods():
    doc_path = "/Volumes/cat/schema/raw_documents/finance/internal/expense_policy.txt"
    method_a_indexes = range(5)
    method_b_indexes = range(5)

    ids = [compute_chunk_id(doc_path, "ai_prep_search", i) for i in method_a_indexes]
    ids += [compute_chunk_id(doc_path, "fixed_overlap", i) for i in method_b_indexes]

    assert len(ids) == len(set(ids)), "chunk_id collision detected across chunking methods"


def test_chunk_id_is_deterministic():
    doc_path = "/Volumes/cat/schema/raw_documents/general/public/company_faq.txt"

    first = compute_chunk_id(doc_path, "fixed_overlap", 2)
    second = compute_chunk_id(doc_path, "fixed_overlap", 2)

    assert first == second


def test_fixed_overlap_chunks_covers_full_text_with_overlap():
    text = "x" * 1000
    chunks = fixed_overlap_chunks(text, chunk_size=300, overlap=50)

    assert all(len(c) <= 300 for c in chunks)
    # stride = chunk_size - overlap = 250 -> ceil(1000/250) = 4 chunks
    assert len(chunks) == 4


def test_fixed_overlap_chunks_empty_text_returns_no_chunks():
    assert fixed_overlap_chunks("") == []


def test_fixed_overlap_chunks_rejects_overlap_gte_chunk_size():
    with pytest.raises(ValueError):
        fixed_overlap_chunks("some text", chunk_size=100, overlap=100)


def test_fixed_overlap_chunks_produce_unique_indexes_when_zipped():
    text = "y" * 500
    chunks = fixed_overlap_chunks(text, chunk_size=200, overlap=50)
    indexed = list(enumerate(chunks))

    assert len(indexed) == len(chunks)
    assert len({i for i, _ in indexed}) == len(indexed)
