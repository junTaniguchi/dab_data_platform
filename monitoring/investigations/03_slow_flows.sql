-- 調査目的: 遅いFlowと処理データ量の特定。
-- <pipeline-id> を実際のパイプラインIDに置き換えて実行する。
--
-- 判断基準・見方: 普段のavg_secと比較して何倍になっているかを見る。
-- output_rowsの増加率を大きく上回るduration増加のみ問題視する
-- （フィールド名は num_output_rows。processed_rows という列は存在しない）。
SELECT
  origin.flow_name,
  ROUND(AVG(duration_ms) / 1000, 2) AS avg_sec,
  AVG(TRY_CAST(details:flow_progress.metrics.num_output_rows AS BIGINT)) AS avg_output_rows
FROM event_log('<pipeline-id>')
WHERE event_type = 'flow_progress'
GROUP BY origin.flow_name
ORDER BY avg_sec DESC;
