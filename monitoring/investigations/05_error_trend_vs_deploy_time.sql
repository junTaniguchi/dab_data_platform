-- 調査目的: デプロイ日時とエラー増加時期の比較。
-- <pipeline-id> を実際のパイプラインIDに置き換えて実行する。
SELECT
  DATE(timestamp) AS run_date,
  COUNT(*) AS error_count
FROM event_log('<pipeline-id>')
WHERE level = 'ERROR'
GROUP BY run_date
ORDER BY run_date;

-- 判断基準・見方: 上記の結果と、CI/CDのbundle deploy実行ログを突合し、
-- リリース直後の増加ならコード起因、それ以外の時期の増加なら外部要因を疑う。
--
-- 【デプロイ実行ログの実際の取得方法】
-- cd.yml はサービスプリンシパル（DATABRICKS_CLIENT_ID）の環境変数で認証してデプロイする
-- ため、system.access.audit を identity_metadata.run_by（または
-- user_identity.email）でそのサービスプリンシパルのアプリケーションIDに絞り込むと
-- デプロイ関連操作のタイムスタンプが追える（例: service_name='workspaceFiles' の
-- ファイルアップロード操作等）。ただし「このデプロイがbundle deployによるものか」を
-- 一意に区別するaction_nameは無いため、GitHub Actions側のワークフロー実行履歴
-- （`gh run list`）と突き合わせるのが最も確実。
SELECT
  event_time,
  service_name,
  action_name
FROM system.access.audit
WHERE user_identity.email = '<デプロイに使っているサービスプリンシパルのアプリケーションID>'
ORDER BY event_time DESC;
