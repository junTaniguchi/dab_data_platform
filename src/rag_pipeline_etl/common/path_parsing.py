"""Bronze層のファイルパス解析ロジック（純粋関数、pyspark非依存）。

Lakeflow Declarative Pipelines のソースファイルはトップレベルモジュールとして
実行されるため相対importが安定して機能するとは限らない。そのため共有ロジックは
`common/` パッケージへ切り出し、各変換ファイル側で sys.path 経由で明示的に import する。
これにより通常の pytest（Databricks Runtime 外）からも直接importしてテストできる。
"""
import re

# /Volumes/<catalog>/<schema>/raw_documents/<department>/<classification>/<file_name>
RAW_PATH_PATTERN = r".*/raw_documents/([^/]+)/([^/]+)/[^/]+$"

_PATTERN = re.compile(RAW_PATH_PATTERN)


def parse_department_classification(file_path: str) -> tuple[str, str]:
    """ファイルパスから (department, classification) を抽出する。

    パターンに一致しない場合は空文字のタプルを返す（Spark の regexp_extract と同じ挙動）。
    """
    match = _PATTERN.match(file_path)
    if not match:
        return "", ""
    return match.group(1), match.group(2)
