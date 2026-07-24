-- 調査目的: 失敗率の推移確認（単一パイプライン内）。
-- <pipeline-id> を実際のパイプラインIDに置き換えて実行する。
--
-- 判断基準・見方: 単日のスパイクより、複数日にわたる増加傾向が続いているかを確認する。
SELECT
  DATE(timestamp) AS run_date,
  COUNT(*) AS error_count
FROM event_log('<pipeline-id>')
WHERE level = 'ERROR'
GROUP BY run_date
ORDER BY run_date DESC;
