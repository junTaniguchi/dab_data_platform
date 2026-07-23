"""Silver層のテキスト抽出方式の判定ロジック（純粋関数、pyspark非依存）。"""

TEXT_LIKE_EXTENSIONS = ("txt", "md")


def is_text_like_extension(file_extension: str) -> bool:
    return file_extension.lower() in TEXT_LIKE_EXTENSIONS


def parse_method_for(file_extension: str) -> str:
    return "utf8_decode" if is_text_like_extension(file_extension) else "ai_parse_document"
