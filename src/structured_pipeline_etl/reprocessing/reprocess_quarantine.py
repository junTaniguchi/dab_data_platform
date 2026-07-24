"""受注データの検疫（quarantine）是正・再投入ジョブ。

silver_orders_quarantine の行を読み取り、以下のサイクルを回す。

    1. 判定    : structured_common.reprocessing_rules.decide_resolution() で
                自動是正可能（CORRECTED）か、人手対応が必要（UNCORRECTABLE）かを判定する。
    2. 是正    : CORRECTED な行は apply_correction() で値を補正する。
    3. 再投入  : 補正済みレコードを、Auto Loader が読み込む Bronze 取り込み用 Volume の
                `orders/reprocessed/` サブパスへ**生のJSONファイルとして**書き出す。
                Silver/Bronzeの Lakeflow 管理テーブルへ外部から直接 MERGE/UPDATE する
                ことは意図的に避けている（Lakeflowパイプラインが所有するテーブルへ
                パイプライン外から書き込む行為はサポートが曖昧なため）。次回の
                パイプライン実行で Bronze -> Silver の Expectations 検証を
                "もう一度正面から" 通すことで、安全に再評価させる。
    4. 監査    : 是正結果（CORRECTED/UNCORRECTABLE、理由、時刻）は
                quarantine_resolution_log という、このジョブが単独で所有する
                素のDeltaテーブル（Lakeflow管理外）に追記する。
                silver_orders_quarantine 自体は一切 UPDATE/DELETE しない
                （検疫時点のスナップショットを監査証跡として不変のまま残すため）。

冪等性: 同じ order_id が既に quarantine_resolution_log に記録済みであれば
スキップする。何度実行しても、同じ検疫行から再投入ファイルが重複生成されない。

前提: `--src_root` には Databricks へ同期された
`.../files/src/structured_pipeline_etl` の絶対パスを渡すこと
（spark_python_task 実行環境では `__file__` が定義されないため、
seed_structured_sample_data.py と同じ理由で、パスは呼び出し側から明示的に渡す）。
"""
import argparse
import io
import json
import sys
import uuid
from datetime import datetime

from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession

LOG_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS {log_table} (
  order_id STRING,
  violated_rules ARRAY<STRING>,
  resolution_status STRING,
  correction_note STRING,
  resolved_at TIMESTAMP,
  reprocess_count INT
)
USING DELTA
COMMENT 'reprocess_quarantine.py が単独で所有する検疫是正の監査ログ。Lakeflowパイプラインの管理対象ではない。'
"""

LOG_SCHEMA = (
    "order_id STRING, violated_rules ARRAY<STRING>, resolution_status STRING, "
    "correction_note STRING, resolved_at TIMESTAMP, reprocess_count INT"
)

REPROCESSED_ORDER_FIELDS = [
    "order_id",
    "customer_id",
    "order_date",
    "amount",
    "currency",
    "status",
    "region",
    "unit_price",
    "discount_rate",
    "source_system",
    "ingestion_batch",
    "payment_card_last4",
    "payment_card_hash",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Reprocess quarantined structured records")
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument(
        "--volume_path", required=True, help="e.g. /Volumes/<catalog>/<schema>/raw_structured_data"
    )
    parser.add_argument(
        "--src_root", required=True, help="e.g. /Workspace/.../files/src/structured_pipeline_etl"
    )
    return parser.parse_args()


def run(catalog: str, schema: str, volume_path: str, src_root: str) -> None:
    sys.path.append(src_root)
    from structured_common.reprocessing_rules import (
        apply_correction,
        correction_note,
        decide_resolution,
    )

    spark = SparkSession.builder.getOrCreate()

    quarantine_table = f"{catalog}.{schema}.silver_orders_quarantine"
    log_table = f"{catalog}.{schema}.quarantine_resolution_log"

    spark.sql(LOG_TABLE_DDL.format(log_table=log_table))

    already_logged_ids = {
        row["order_id"]
        for row in spark.table(log_table).select("order_id").distinct().collect()
    }

    quarantine_rows = [
        row.asDict(recursive=True) for row in spark.table(quarantine_table).collect()
    ]
    pending_rows = [row for row in quarantine_rows if row["order_id"] not in already_logged_ids]

    if not pending_rows:
        print("[reprocess] no new quarantined rows to process.")
        return

    now = datetime.utcnow()
    log_records: list[dict] = []
    reprocessed_records: list[dict] = []

    for row in pending_rows:
        violated_rules = list(row.get("violated_rules") or [])
        resolution = decide_resolution(violated_rules)
        note = correction_note(violated_rules, resolution)

        log_records.append(
            {
                "order_id": row["order_id"],
                "violated_rules": violated_rules,
                "resolution_status": resolution,
                "correction_note": note,
                "resolved_at": now,
                "reprocess_count": 1,
            }
        )

        if resolution == "CORRECTED":
            corrected = apply_correction(row, violated_rules)
            record = {field: corrected.get(field) for field in REPROCESSED_ORDER_FIELDS}
            record["reprocessed_from_quarantine"] = True
            reprocessed_records.append(record)

    # 1. 是正結果を監査ログへ追記する（silver_orders_quarantine 自体はUPDATEしない）。
    log_df = spark.createDataFrame(log_records, schema=LOG_SCHEMA)
    log_df.write.format("delta").mode("append").saveAsTable(log_table)
    print(
        f"[reprocess] logged {len(log_records)} resolutions "
        f"({sum(1 for r in log_records if r['resolution_status'] == 'CORRECTED')} corrected, "
        f"{sum(1 for r in log_records if r['resolution_status'] == 'UNCORRECTABLE')} uncorrectable)"
    )

    # 2. 是正できた行だけを、Bronzeの取り込みVolumeへ"生JSON"として再投入する。
    if reprocessed_records:
        w = WorkspaceClient()
        file_name = f"reprocessed_{now.strftime('%Y%m%dT%H%M%S')}_{uuid.uuid4().hex[:8]}.json"
        target_path = f"{volume_path.rstrip('/')}/orders/reprocessed/{file_name}"
        content = "\n".join(json.dumps(record, default=str) for record in reprocessed_records)
        w.files.upload(target_path, io.BytesIO(content.encode("utf-8")), overwrite=False)
        print(f"[reprocess] requeued {len(reprocessed_records)} corrected record(s) -> {target_path}")
    else:
        print("[reprocess] no correctable rows in this batch.")


def main() -> None:
    args = parse_args()
    run(args.catalog, args.schema, args.volume_path, args.src_root)


if __name__ == "__main__":
    main()
