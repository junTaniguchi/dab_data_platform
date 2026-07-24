"""sample_data/structured 配下のサンプル構造化データを UC Volume へ登録するシードスクリプト。

rag の seed_sample_data.py と同じ設計方針（`__file__` が使えない spark_python_task
実行環境のため、呼び出し側からサンプルデータの絶対パスを引数で渡す／冪等に既存ファイルは
スキップする）を踏襲している。

customers/ と orders/ のサブディレクトリ構造を維持したまま Volume へアップロードする。
Bronze側（bronze_customers.py / bronze_orders.py）は、この Volume 配下の
`customers/` `orders/` サブパスをそれぞれ個別に Auto Loader で読み込む。

意図的に sample_data/structured/orders_incident/ 配下のファイルはアップロードしない
（Gold の Fail 挙動を確認したい場合にのみ手動で投入する検証用データのため。
詳細は README「Gold Fail挙動を確認する」セクションを参照）。
"""
import argparse
import io
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound

UPLOAD_SUBDIRS = ("customers", "orders")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Seed sample structured data into a UC Volume")
    parser.add_argument(
        "--sample_data_dir",
        required=True,
        help="e.g. /Workspace/Users/<user>/.bundle/<bundle>/<target>/files/sample_data/structured",
    )
    parser.add_argument(
        "--volume_path", required=True, help="e.g. /Volumes/<catalog>/<schema>/raw_structured_data"
    )
    return parser.parse_args()


def upload_sample_structured_data(w: WorkspaceClient, sample_data_dir: str, volume_path: str) -> None:
    sample_data_path = Path(sample_data_dir)
    if not sample_data_path.exists():
        raise FileNotFoundError(
            f"sample data directory not found at {sample_data_path}. "
            "Was the bundle deployed with sample_data/ included?"
        )

    uploaded, skipped = 0, 0
    for subdir in UPLOAD_SUBDIRS:
        local_dir = sample_data_path / subdir
        if not local_dir.exists():
            print(f"[seed] skip missing subdir: {local_dir}")
            continue

        for local_file in sorted(local_dir.rglob("*")):
            if not local_file.is_file():
                continue

            relative_path = local_file.relative_to(sample_data_path)
            target_path = f"{volume_path.rstrip('/')}/{relative_path.as_posix()}"

            if _file_exists(w, target_path):
                skipped += 1
                continue

            content = local_file.read_bytes()
            w.files.upload(target_path, io.BytesIO(content), overwrite=False)
            uploaded += 1
            print(f"[seed] uploaded {relative_path} -> {target_path}")

    print(f"[seed] done. uploaded={uploaded} skipped_existing={skipped}")


def _file_exists(w: WorkspaceClient, target_path: str) -> bool:
    try:
        w.files.get_metadata(target_path)
        return True
    except NotFound:
        return False


def main() -> None:
    args = parse_args()

    w = WorkspaceClient()
    upload_sample_structured_data(w, args.sample_data_dir, args.volume_path)


if __name__ == "__main__":
    main()
