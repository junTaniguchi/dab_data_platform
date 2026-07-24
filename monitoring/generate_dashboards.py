"""monitoring/dashboards/*.lvdash.json を生成するスクリプト。

【なぜダッシュボードJSON自体をDABの変数展開（${var.catalog}等）に頼らず、
このスクリプトで catalog/schema/pipeline_id を直接埋め込んで生成しているか】
実際に `resources.dashboards` の `file_path` が指すJSONファイル内に
`${var.catalog}` を書いてデプロイしたところ、
`Error: invalid dependency "${var.catalog}", no such node ""` で失敗した。
YAML側のフィールド（例: warehouse_id）では同じ `${var.xxx}` 構文が正常に
展開されるのに対し、file_path経由で読み込む外部JSONファイルの中身に対しては
現行のDatabricks CLI v1.9.0では変数展開が正しく機能しないことを実機で確認した
（既知の制約としてREADMEにも記載）。
そのため、このリポジトリでは「JSONファイルに変数を書く」のではなく、
「このスクリプトが catalog/schema/pipeline_id を実際の値に解決してからJSONを
書き出す」という一段階前の処理として値を確定させる方式にしている。
別のワークスペース/カタログ/スキーマへ展開する場合は、下記の CATALOG / SCHEMA /
RAG_PIPELINE_ID / STRUCTURED_PIPELINE_ID を実際の値に書き換えてから
`python monitoring/generate_dashboards.py` を再実行し、生成された
monitoring/dashboards/*.lvdash.json を再デプロイすること
（`databricks pipelines list-pipelines` でpipeline_idは確認できる）。

用意していないダッシュボード:
    No.22「承認済みメンテナンス一覧との突合」は Jira 等の変更管理ツールとの
    連携（approved_changes テーブル）が本リポジトリの範囲外のため、実装していない
    （README「監視・アラート」セクションに理由を記載）。
"""

import json
import os

# --- このワークスペース(dev target)向けに解決済みの値。他環境へ展開する場合はここを書き換える ---
CATALOG = "workspace"
SCHEMA = "dev_ultia0602_sales_dev"
RAG_PIPELINE_ID = "b17663d1-bdcf-4423-9415-6d30ad190318"
STRUCTURED_PIPELINE_ID = "c0fda4ff-60aa-4cb0-aba6-ab00c2517e18"

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "dashboards")


def make_dashboard(display_name, dataset_name, query_lines, columns):
    """1データセット・1テーブルウィジェットだけのシンプルなLakeviewダッシュボードを組み立てる。
    columns: [(fieldName, displayName), ...]（SELECTの列順と一致させること）
    """
    fields = [{"name": c[0], "expression": f"`{c[0]}`"} for c in columns]
    encodings_columns = [{"fieldName": c[0], "displayName": c[1]} for c in columns]
    return {
        "datasets": [
            {"name": dataset_name, "displayName": dataset_name, "queryLines": query_lines}
        ],
        "pages": [
            {
                "name": "main",
                "displayName": display_name,
                "layout": [
                    {
                        "widget": {
                            "name": "widget1",
                            "queries": [
                                {
                                    "name": "main_query",
                                    "query": {
                                        "datasetName": dataset_name,
                                        "fields": fields,
                                        "disaggregated": True,
                                    },
                                }
                            ],
                            "spec": {
                                "version": 3,
                                "widgetType": "table",
                                "encodings": {"columns": encodings_columns},
                                "frame": {"showTitle": True, "title": display_name},
                            },
                        },
                        "position": {"x": 0, "y": 0, "width": 6, "height": 6},
                    }
                ],
            }
        ],
    }


DASHBOARDS = [
    dict(
        file="query_health.lvdash.json",
        display_name="クエリの健康診断（中央値付き）",
        dataset_name="query_health_ds",
        query_lines=[
            "-- 平均だけだと極端に重いクエリに引っ張られるため中央値も併記する。",
            "-- 【要求仕様からの補正】元のSQLはGROUP BYしつつread_bytesを非集計のまま",
            "-- 参照しておりSQLとして不正だったため、SUM(read_bytes)に修正した。",
            "-- あわせて『失敗率の把握』という運用目的に対しfailed_countが元案に",
            "-- 無かったため追加した。",
            "SELECT",
            "  executed_by,",
            "  ROUND(AVG(total_duration_ms)/1000, 2) AS avg_sec,",
            "  ROUND(PERCENTILE(total_duration_ms, 0.5)/1000, 2) AS median_sec,",
            "  ROUND(SUM(read_bytes)/POWER(1024,3), 3) AS gb_scanned,",
            "  COUNT_IF(execution_status = 'FAILED') AS failed_count,",
            "  COUNT(*) AS query_count",
            "FROM system.query.history",
            "WHERE start_time >= current_timestamp() - INTERVAL 1 DAY",
            "GROUP BY executed_by",
            "ORDER BY avg_sec DESC",
        ],
        columns=[
            ("executed_by", "実行者"),
            ("avg_sec", "平均秒"),
            ("median_sec", "中央値秒"),
            ("gb_scanned", "スキャンGB"),
            ("failed_count", "失敗数"),
            ("query_count", "実行数"),
        ],
    ),
    dict(
        file="finops_cost_allocation.lvdash.json",
        display_name="FinOps（コスト配賦）",
        dataset_name="finops_ds",
        query_lines=[
            "-- project_nameがNULLの行が大きい場合、コスト以前にタグ付けの徹底が課題",
            "-- （このワークスペースで実際に確認したところ custom_tags['project'] を",
            "-- 設定していないため現状は全行NULLになる。運用開始前にタグ付けルールの",
            "-- 整備が必要、という実際の発見）。",
            "SELECT",
            "  custom_tags['project'] AS project_name,",
            "  SUM(usage_quantity) AS total_usage",
            "FROM system.billing.usage",
            "WHERE usage_date >= current_date() - 30",
            "GROUP BY custom_tags['project']",
            "ORDER BY total_usage DESC",
        ],
        columns=[("project_name", "プロジェクト名"), ("total_usage", "利用量")],
    ),
    dict(
        file="cost_per_user.lvdash.json",
        display_name="Cost/User（概算）",
        dataset_name="cost_per_user_ds",
        query_lines=[
            "-- system.billing.usage はコンピュート単位の集計であり個人には直接紐づかないため、",
            "-- 実行時間ベースの近似値として見る。",
            "SELECT",
            "  executed_by,",
            "  SUM(total_duration_ms) AS total_ms",
            "FROM system.query.history",
            "WHERE start_time >= current_date() - 7",
            "GROUP BY executed_by",
            "ORDER BY total_ms DESC",
        ],
        columns=[("executed_by", "実行者"), ("total_ms", "累計実行時間(ms)")],
    ),
    dict(
        file="warehouse_cost_autostop.lvdash.json",
        display_name="Warehouse別コスト＋Auto Stop設定",
        dataset_name="warehouse_cost_ds",
        query_lines=[
            "-- 【要求仕様からの補正】system.billing.usage に compute_resource_name という列は",
            "-- 実在しない。usage_metadata.warehouse_id を system.compute.warehouses（実在）",
            "-- へJOINしてWarehouse名を取得する。判断基準の「Auto Stopのタイムアウト設定を",
            "-- 確認する」を別画面に頼らずこのクエリ自体にauto_stop_minutes列として含めた。",
            "SELECT",
            "  w.warehouse_name,",
            "  w.auto_stop_minutes,",
            "  SUM(u.usage_quantity) AS total_usage_7d",
            "FROM system.billing.usage u",
            "LEFT JOIN system.compute.warehouses w",
            "  ON u.usage_metadata.warehouse_id = w.warehouse_id",
            "WHERE u.usage_date >= current_date() - 7",
            "GROUP BY w.warehouse_name, w.auto_stop_minutes",
            "ORDER BY total_usage_7d DESC",
        ],
        columns=[
            ("warehouse_name", "Warehouse名"),
            ("auto_stop_minutes", "AutoStop(分)"),
            ("total_usage_7d", "直近7日利用量"),
        ],
    ),
    dict(
        file="cost_by_hour_dow.lvdash.json",
        display_name="時間帯別・曜日別コスト",
        dataset_name="cost_by_hour_dow_ds",
        query_lines=[
            "-- 深夜・早朝帯の消費は曜日別平均と比較し、特定曜日だけの異常か",
            "-- 常態化した無駄かを見分ける。",
            "SELECT",
            "  HOUR(usage_start_time) AS hour,",
            "  DAYOFWEEK(usage_date) AS dow,",
            "  SUM(usage_quantity) AS total_usage",
            "FROM system.billing.usage",
            "WHERE usage_date >= current_date() - 28",
            "GROUP BY hour, dow",
            "ORDER BY dow, hour",
        ],
        columns=[("hour", "時"), ("dow", "曜日(1=日)"), ("total_usage", "利用量")],
    ),
    dict(
        file="sla_dashboard.lvdash.json",
        display_name="SLAダッシュボード",
        dataset_name="sla_ds",
        query_lines=[
            "-- 【要求仕様からの補正】system.job_run は実在しないため",
            "-- system.lakeflow.job_run_timeline を使う。",
            "-- 単日の低下より複数日の下降トレンドを優先する。",
            "-- 【既知の制約】「SLA未達日をクリックすると#1のJob一覧へ遷移」のような",
            "-- ドリルダウンは、Lakeview UI上でこのダッシュボードにフィルタ/パラメータを",
            "-- 手動で追加する必要がある（JSON定義だけでは表現しきれないインタラクティブ",
            "-- 機能のため、UI側での追加設定が必要という制約として記載する）。",
            "SELECT",
            "  date_trunc('day', period_start_time) AS run_date,",
            "  COUNT(*) AS total_runs,",
            "  SUM(CASE WHEN TIMESTAMPDIFF(MINUTE, period_start_time, period_end_time) <= 15 THEN 1 ELSE 0 END) AS sla_met,",
            "  ROUND(SUM(CASE WHEN TIMESTAMPDIFF(MINUTE, period_start_time, period_end_time) <= 15 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS sla_met_pct",
            "FROM system.lakeflow.job_run_timeline",
            "GROUP BY 1",
            "ORDER BY 1 DESC",
        ],
        columns=[
            ("run_date", "日付"),
            ("total_runs", "総実行数"),
            ("sla_met", "SLA達成数"),
            ("sla_met_pct", "SLA達成率%"),
        ],
    ),
    dict(
        file="pipeline_stability_error_category.lvdash.json",
        display_name="パイプライン安定性＋エラー分類",
        dataset_name="pipeline_stability_ds",
        query_lines=[
            "-- 【要求仕様からの補正】system.pipelines.events は実在しない。",
            "-- message列（エラー分類に必須）を持つのは event_log('<pipeline-id>') のみ",
            "-- なので、本リポジトリの2本のパイプラインを対象にUNION ALLしている。",
            "-- capacityはインフラ担当、governanceはガバナンス担当、dataはデータソース",
            "-- 担当へエスカレーションする。",
            "SELECT",
            "  origin.pipeline_name,",
            "  CASE",
            "    WHEN message LIKE '%RESOURCE_EXHAUSTED%' THEN 'capacity'",
            "    WHEN message LIKE '%permission%' THEN 'governance'",
            "    WHEN message LIKE '%Schema%' THEN 'data'",
            "    ELSE 'other'",
            "  END AS error_category,",
            "  COUNT(*) AS occurrences",
            "FROM (",
            f"  SELECT * FROM event_log('{RAG_PIPELINE_ID}')",
            "  UNION ALL",
            f"  SELECT * FROM event_log('{STRUCTURED_PIPELINE_ID}')",
            ")",
            "WHERE level = 'ERROR'",
            "GROUP BY origin.pipeline_name, error_category",
            "ORDER BY occurrences DESC",
        ],
        columns=[
            ("pipeline_name", "パイプライン名"),
            ("error_category", "エラー分類"),
            ("occurrences", "発生件数"),
        ],
    ),
    dict(
        file="mttr.lvdash.json",
        display_name="MTTR（平均復旧時間）",
        dataset_name="mttr_ds",
        query_lines=[
            "-- 【要求仕様からの補正】monitoring.mttr という管理テーブルは実在しない。",
            "-- 『最後のFAILEDから次のSUCCESSまでの時間』を",
            "-- system.lakeflow.job_run_timeline から直接算出する",
            "-- （＝自動復旧時間の近似値。人手対応を含めたMTTRを測るにはAck記録が別途必要、",
            "-- という判断基準は元の要求仕様のまま変わらない）。",
            "WITH runs AS (",
            "  SELECT",
            "    job_id, result_state, period_end_time,",
            "    LEAD(result_state) OVER (PARTITION BY job_id ORDER BY period_start_time) AS next_result_state,",
            "    LEAD(period_start_time) OVER (PARTITION BY job_id ORDER BY period_start_time) AS next_start_time",
            "  FROM system.lakeflow.job_run_timeline",
            ")",
            "SELECT",
            "  job_id,",
            "  ROUND(AVG(TIMESTAMPDIFF(MINUTE, period_end_time, next_start_time)), 2) AS avg_mttr_minutes",
            "FROM runs",
            "WHERE result_state = 'FAILED' AND next_result_state = 'SUCCESS'",
            "GROUP BY job_id",
        ],
        columns=[("job_id", "ジョブID"), ("avg_mttr_minutes", "平均MTTR(分)")],
    ),
    dict(
        file="pii_access_new_users.lvdash.json",
        display_name="監査ログ（PIIアクセス＋新規ユーザー）",
        dataset_name="pii_access_ds",
        query_lines=[
            "-- 【要求仕様からの補正】system.access.audit はUnity Catalogのメタデータ操作",
            "-- （createTable/getTable等）は記録するが、行レベルのSELECT実行そのものは",
            "-- 記録しない（実機のaction_name一覧で確認済み）。実際にテーブルへSELECTした",
            "-- 記録は system.query.history の方に残るため、こちらを使う。",
            "-- 普段そのテーブルに触れないユーザーや、直近90日に登場しなかった新規",
            "-- アクセスユーザーを優先的に確認する。",
            "SELECT",
            "  executed_by,",
            "  start_time,",
            "  statement_text",
            "FROM system.query.history",
            "WHERE statement_type = 'SELECT'",
            "  AND lower(statement_text) LIKE '%customer%'",
            "  AND executed_by NOT IN (",
            "    SELECT DISTINCT executed_by",
            "    FROM system.query.history",
            "    WHERE start_time BETWEEN current_date() - 90 AND current_date() - 1",
            "  )",
            "ORDER BY start_time DESC",
        ],
        columns=[
            ("executed_by", "実行者"),
            ("start_time", "実行時刻"),
            ("statement_text", "SQL文"),
        ],
    ),
    dict(
        file="table_mutation_audit.lvdash.json",
        display_name="監査ログ（本番テーブル更新・削除）",
        dataset_name="table_mutation_ds",
        query_lines=[
            "-- 【要求仕様からの補正】system.access.audit にはDELETE/UPDATE/DROP TABLE/",
            "-- ALTER TABLEという文字列そのもののaction_nameは存在しない",
            "-- （実在するのはcreateTable/deleteTable/updateTables等のUC操作名で、",
            "-- 行レベルDML(DELETE/UPDATE文)は記録されない）。実行されたSQL文の種別が",
            "-- そのまま残る system.query.history.statement_type を使う方が、",
            "-- 元の運用目的（DELETE/UPDATE/DROP/ALTERの追跡）に対して正確。",
            "-- スケジュール済みメンテナンスかどうかをまず確認し、予定外の操作は",
            "-- 即座に本人確認する。",
            "SELECT",
            "  executed_by,",
            "  start_time,",
            "  statement_type,",
            "  statement_text",
            "FROM system.query.history",
            "WHERE statement_type IN ('DELETE', 'UPDATE', 'DROP', 'ALTER')",
            "ORDER BY start_time DESC",
        ],
        columns=[
            ("executed_by", "実行者"),
            ("start_time", "実行時刻"),
            ("statement_type", "種別"),
            ("statement_text", "SQL文"),
        ],
    ),
    dict(
        file="uc_metadata_quality.lvdash.json",
        display_name="Unity Catalog品質（コメント・タグ・Owner）",
        dataset_name="uc_metadata_quality_ds",
        query_lines=[
            "-- 【要求仕様からの補正】system.information_schema.tables は実在しない",
            "-- （information_schemaは system カタログ配下ではなく、各カタログ配下に",
            "-- 存在する。実機の DESCRIBE で確認済み）。",
            f"-- 対象カタログを {CATALOG} に固定して {CATALOG}.information_schema.tables を使う。",
            "-- 絶対値より週ごとの推移を見る。低下傾向はテーブル追加にドキュメント整備が",
            "-- 追いついていないサイン。",
            "SELECT",
            "  ROUND(COUNT_IF(comment IS NOT NULL) * 100.0 / COUNT(*), 2) AS comment_rate_pct,",
            "  ROUND(COUNT_IF(table_owner IS NOT NULL) * 100.0 / COUNT(*), 2) AS owner_rate_pct,",
            "  COUNT(*) AS total_tables",
            f"FROM {CATALOG}.information_schema.tables",
            "WHERE table_schema != 'information_schema'",
        ],
        columns=[
            ("comment_rate_pct", "コメント充足率%"),
            ("owner_rate_pct", "Owner充足率%"),
            ("total_tables", "対象テーブル数"),
        ],
    ),
    dict(
        file="unused_table_inventory.lvdash.json",
        display_name="未使用データ・テーブルサイズの棚卸し",
        dataset_name="unused_table_ds",
        query_lines=[
            "-- 【要求仕様からの補正】2点補正している。",
            "-- (1) information_schema.tables に size_gb にあたる列（バイトサイズ）は",
            "--     存在しない。テーブルごとのサイズはDESCRIBE DETAILを個別に叩く必要が",
            "--     あり、1本のSQLでは取得できない（本リポジトリでは未実装。将来的に",
            "--     全テーブルを列挙してDESCRIBE DETAILする別ジョブが必要）。",
            "-- (2) 最終アクセス時刻は system.access.audit ではなく",
            "--     system.access.table_lineage（実在。UCのテーブル間リネージを",
            "--     追跡するsystem table）の event_time を使う方が正確。",
            "-- size_gbが取得できるようになった際は、大きいテーブルほど優先度を上げる。",
            "-- 削除前には利用部門への確認が必須。",
            "SELECT",
            "  t.table_catalog,",
            "  t.table_schema,",
            "  t.table_name,",
            "  a.last_access",
            f"FROM {CATALOG}.information_schema.tables t",
            "LEFT JOIN (",
            "  SELECT target_table_full_name, MAX(event_time) AS last_access",
            "  FROM system.access.table_lineage",
            "  GROUP BY target_table_full_name",
            ") a",
            "  ON CONCAT(t.table_catalog, '.', t.table_schema, '.', t.table_name) = a.target_table_full_name",
            "WHERE a.last_access < current_date() - INTERVAL 90 DAYS OR a.last_access IS NULL",
        ],
        columns=[
            ("table_catalog", "カタログ"),
            ("table_schema", "スキーマ"),
            ("table_name", "テーブル名"),
            ("last_access", "最終アクセス"),
        ],
    ),
    dict(
        file="lineage_downstream_count.lvdash.json",
        display_name="依存関係・リネージ・下流依存数",
        dataset_name="lineage_ds",
        query_lines=[
            "-- downstream_countが多いテーブルほど変更時の影響範囲が広く、",
            "-- 事前周知や慎重なテストが必要。",
            "-- 【要求仕様からの補正】単純なCOUNT(*)だと同一テーブルペアへの",
            "-- 複数回の書き込みイベントを重複カウントしてしまうため、",
            "-- COUNT(DISTINCT target_table_full_name)（重複排除した下流テーブル数）に",
            "-- した方が『依存関係の広さ』という運用目的に対して正確。",
            "SELECT",
            "  source_table_full_name,",
            "  COUNT(DISTINCT target_table_full_name) AS downstream_count",
            "FROM system.access.table_lineage",
            "GROUP BY source_table_full_name",
            "ORDER BY downstream_count DESC",
        ],
        columns=[("source_table_full_name", "ソーステーブル"), ("downstream_count", "下流テーブル数")],
    ),
]


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    for spec in DASHBOARDS:
        dashboard = make_dashboard(
            spec["display_name"], spec["dataset_name"], spec["query_lines"], spec["columns"]
        )
        path = os.path.join(OUTPUT_DIR, spec["file"])
        with open(path, "w", encoding="utf-8") as f:
            json.dump(dashboard, f, ensure_ascii=False, indent=2)
        print(f"wrote {path}")


if __name__ == "__main__":
    main()
