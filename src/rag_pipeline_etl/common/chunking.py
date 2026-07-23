"""チャンキング関連の純粋関数（pyspark非依存）。"""

DEFAULT_CHUNK_SIZE = 800
DEFAULT_OVERLAP = 200


def fixed_overlap_chunks(
    text: str,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    overlap: int = DEFAULT_OVERLAP,
) -> list[str]:
    """本文テキストを固定長・オーバーラップ付きでチャンク分割する（手法B）。

    overlap は chunk_size 未満である必要がある（無限ループ防止）。
    """
    if not text:
        return []
    if overlap >= chunk_size:
        raise ValueError("overlap must be smaller than chunk_size")

    stride = chunk_size - overlap
    chunks = []
    start = 0
    text_length = len(text)
    while start < text_length:
        chunk = text[start : start + chunk_size]
        if chunk:
            chunks.append(chunk)
        start += stride
    return chunks
