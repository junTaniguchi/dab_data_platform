"""sample_data/documents 配下のサンプルドキュメントを UC Volume へ登録するシードスクリプト。

rag_pipeline_job.job.yml の spark_python_task から実行される。
サンプルデータのディレクトリは `--sample_data_dir` 引数で受け取る。当初は
`Path(__file__).resolve().parents[3]` でスクリプト自身の場所から逆算していたが、
spark_python_task のサーバーレス実行環境ではスクリプトが
`exec(compile(f.read(), filename, 'exec'))` という形で実行され `__file__` が
グローバル変数として定義されないため `NameError: name '__file__' is not defined` で
失敗した。そのため、バンドルの組み込み変数 `${workspace.file_path}`
（デプロイ先のファイル同期ルート、例: /Workspace/Users/<user>/.bundle/<name>/<target>/files）
を使って呼び出し側（rag_pipeline_job.job.yml）から絶対パスを明示的に渡す方式にした。

Volume自体は resources/rag_unity_catalog.yml の resources.volumes で
バンドルが宣言的に作成する（このスクリプトでは作成しない。作成主体を1箇所に絞るため）。
このスクリプトはあくまでファイルのアップロードのみを担当する。

冪等性: 既にアップロード済みのファイルはスキップする（overwrite しない）ので、
何度実行しても安全。
"""
import argparse
import io
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Seed sample RAG documents into a UC Volume")
    parser.add_argument(
        "--sample_data_dir",
        required=True,
        help="e.g. /Workspace/Users/<user>/.bundle/<bundle>/<target>/files/sample_data/documents",
    )
    parser.add_argument("--volume_path", required=True, help="e.g. /Volumes/<catalog>/<schema>/raw_documents")
    return parser.parse_args()


def upload_sample_documents(w: WorkspaceClient, sample_data_dir: str, volume_path: str) -> None:
    sample_data_path = Path(sample_data_dir)
    if not sample_data_path.exists():
        raise FileNotFoundError(
            f"sample data directory not found at {sample_data_path}. "
            "Was the bundle deployed with sample_data/ included?"
        )

    local_files = sorted(sample_data_path.rglob("*.txt"))
    if not local_files:
        print(f"[seed] no sample files found under {sample_data_path}")
        return

    uploaded, skipped = 0, 0
    for local_file in local_files:
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
    upload_sample_documents(w, args.sample_data_dir, args.volume_path)


if __name__ == "__main__":
    main()
