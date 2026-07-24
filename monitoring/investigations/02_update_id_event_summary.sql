-- 調査目的: update_id単位でのイベント横断集計。
-- 1回のUpdateで何が起きたかをevent_type横断でまとめて把握できる。
-- 障害調査の最初の1クエリとして利用する。
--
-- <pipeline-id> / <失敗したupdate_id> を実際の値に置き換えて実行する
-- （update_idは system.lakeflow.pipeline_update_timeline や、失敗直後の
-- `databricks pipelines get-update <pipeline-id> <update-id>` の出力から確認できる）。
SELECT
  origin.update_id,
  event_type,
  COUNT(*) AS event_count
FROM event_log('<pipeline-id>')
WHERE origin.update_id = '<失敗したupdate_id>'
GROUP BY origin.update_id, event_type;
