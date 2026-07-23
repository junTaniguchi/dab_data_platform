"""sample_data/documents 配下のサンプルドキュメントを UC Volume へ登録するシードスクリプト。

rag_pipeline_job.job.yml の spark_python_task から実行される。
`databricks bundle deploy` により sample_data/ もワークスペースファイルとして同期されるため、
このスクリプト自身の場所からの相対パスでローカルのサンプルファイルを解決する。

冪等性: 既にアップロード済みのファイルはスキップする（overwrite しない）ので、
何度実行しても安全。
"""
import argparse
import io
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service.catalog import VolumeType

# このファイル: dab_data_platform/src/rag_pipeline_etl/seed/seed_sample_data.py
# -> parents[3] == dab_data_platform（プロジェクトルート）
PROJECT_ROOT = Path(__file__).resolve().parents[3]
SAMPLE_DATA_DIR = PROJECT_ROOT / "sample_data" / "documents"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Seed sample RAG documents into a UC Volume")
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--volume_path", required=True, help="e.g. /Volumes/<catalog>/<schema>/raw_documents")
    return parser.parse_args()


def ensure_volume(w: WorkspaceClient, catalog: str, schema: str, volume_name: str) -> None:
    full_name = f"{catalog}.{schema}.{volume_name}"
    try:
        w.volumes.read(full_name)
        print(f"[seed] volume already exists: {full_name}")
    except NotFound:
        print(f"[seed] creating volume: {full_name}")
        w.volumes.create(
            catalog_name=catalog,
            schema_name=schema,
            name=volume_name,
            volume_type=VolumeType.MANAGED,
        )


def upload_sample_documents(w: WorkspaceClient, volume_path: str) -> None:
    if not SAMPLE_DATA_DIR.exists():
        raise FileNotFoundError(
            f"sample data directory not found at {SAMPLE_DATA_DIR}. "
            "Was the bundle deployed with sample_data/ included?"
        )

    local_files = sorted(SAMPLE_DATA_DIR.rglob("*.txt"))
    if not local_files:
        print(f"[seed] no sample files found under {SAMPLE_DATA_DIR}")
        return

    uploaded, skipped = 0, 0
    for local_file in local_files:
        relative_path = local_file.relative_to(SAMPLE_DATA_DIR)
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
    volume_name = args.volume_path.rstrip("/").split("/")[-1]

    w = WorkspaceClient()
    ensure_volume(w, args.catalog, args.schema, volume_name)
    upload_sample_documents(w, args.volume_path)


if __name__ == "__main__":
    main()
