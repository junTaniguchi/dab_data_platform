"""Silver層のテキスト抽出方式判定ロジックのユニットテスト。

common.text_extraction は pyspark.pipelines に依存しない純粋関数なので、
Databricks Runtime 外の通常の pytest 環境でそのままテストできる。
"""
from common.text_extraction import is_text_like_extension, parse_method_for


def test_txt_and_md_are_text_like():
    assert is_text_like_extension("txt") is True
    assert is_text_like_extension("md") is True


def test_txt_extension_is_case_insensitive():
    assert is_text_like_extension("TXT") is True
    assert is_text_like_extension("Md") is True


def test_pdf_and_image_are_not_text_like():
    assert is_text_like_extension("pdf") is False
    assert is_text_like_extension("png") is False
    assert is_text_like_extension("docx") is False


def test_parse_method_for_text_like_uses_utf8_decode():
    assert parse_method_for("txt") == "utf8_decode"
    assert parse_method_for("md") == "utf8_decode"


def test_parse_method_for_binary_uses_ai_parse_document():
    assert parse_method_for("pdf") == "ai_parse_document"
    assert parse_method_for("jpg") == "ai_parse_document"
