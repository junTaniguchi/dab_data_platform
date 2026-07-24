"""PII（個人情報）匿名化・仮名化の純粋関数（pyspark非依存）。

重要な区別（README「PIIハンドリングの2つの仕組み」も参照）:

  1. Silver層の匿名化・仮名化（このモジュールが対象）
     ETL変換の中で一度だけ計算し、変換後の値をテーブルに永続化する。
     不可逆（ハッシュ化）または情報量を落とす（一般化・抑制）ため、
     元の値へ戻すことはできない。

  2. Gold層の Row Filter / Column Mask（governance/structured_governance.sql が対象）
     クエリ時に、呼び出したユーザーのグループ member ship に応じて動的に
     見せる／隠す仕組み。ストレージ上の値そのものは変えない。

このファイルは (1) のみを扱う。実際の Lakeflow 変換コード
（transformations/silver/silver_customers.py）では、ここに書いた純粋関数と
"同じロジック" を Spark の関数（sha2 / regexp_replace / substring 等）で
実装し直す。hashlib.sha256(...).hexdigest() と Spark の sha2(col, 256) は
同一の SHA-256 16進ダイジェストを返すため、値は完全に一致する
（common/chunk_id.py の compute_chunk_id と同じ考え方）。
"""
import hashlib
import re

_EMAIL_LIKE_PATTERN = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


def is_valid_email_format(email: str | None) -> bool:
    """メールアドレスの形式が妥当かどうかを判定する。

    CUSTOMER_RULES["valid_email_format"] の Spark SQL 式と同じ意図の
    Pythonでの参照実装（tests/unit/test_structured_quality_rules.py で使用）。
    """
    if not email:
        return False
    return bool(_EMAIL_LIKE_PATTERN.match(email))


def hash_value(value: str, salt: str) -> str:
    """値をソルト付きSHA-256でハッシュ化する（不可逆・仮名化）。

    email のような一意識別子を、統計的な突合（同一人物の継続利用判定等）には
    使えるが元の値には戻せない形に変換する。ソルトは Databricks Secrets
    （Key Vault等と連携したSecret Scope）から取得すること。ソルトをコード
    リテラルやバンドル変数に書いてはいけない（辞書攻撃で原文が推測できてしまう）。
    """
    return hashlib.sha256(f"{value}{salt}".encode("utf-8")).hexdigest()


def mask_phone(phone: str | None) -> str | None:
    """電話番号を部分マスクする（抑制: Suppression）。

    "090-1111-2222" -> "090-****-2222" のように、先頭のブロックと末尾4桁だけ
    残し、中間ブロックを固定文字列で置き換える。完全な削除ではなく、
    BIやカスタマーサポートの部分照合に必要な最小限の情報だけを残す設計。
    """
    if not phone:
        return phone
    parts = phone.split("-")
    if len(parts) != 3:
        # 想定外のフォーマットは安全側に倒して全体をマスクする
        return "****"
    return f"{parts[0]}-****-{parts[2]}"


def generalize_birth_date_to_year(birth_date: str | None) -> str | None:
    """生年月日を年のみに一般化する（一般化: Generalization）。

    "1988-04-12" -> "1988"。月日を落とすことで、年代分析には使えるが
    個人を一意に特定するには情報が不足する状態にする。
    """
    if not birth_date:
        return birth_date
    return birth_date[:4]


def generalize_address_to_region(region: str | None) -> str | None:
    """住所を番地レベルから地域（region）レベルへ一般化する。

    生の番地情報（address列）はSilver以降には一切引き継がず、
    Bronzeで取得済みの region 列をそのまま採用する。
    """
    return region
