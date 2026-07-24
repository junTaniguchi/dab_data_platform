-- 調査目的: 失敗イベントの詳細確認。
-- <pipeline-id> を実際のパイプラインID（`databricks pipelines list-pipelines` で確認、
-- または databricks.yml デプロイ後は resources.pipelines.<name>.id）に置き換えて実行する。
--
-- 判断基準・見方: message列のスタックトレース・エラー文言を最優先で読む。
-- event_type で障害カテゴリを見分ける。
SELECT
  timestamp,
  event_type,
  message
FROM event_log('<pipeline-id>')
WHERE level = 'ERROR'
ORDER BY timestamp DESC;
