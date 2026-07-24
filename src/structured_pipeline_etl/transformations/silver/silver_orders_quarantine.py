# Silver: 「検疫（Quarantine）」テーブル本体。
#
# ============================================================================
# なぜこのテーブルが必要か（Lakeflow Expectations だけでは足りない理由）
# ============================================================================
# `@dp.expect_or_drop` / `@dp.expect_all_or_drop` は、違反した行を pipeline の
# 出力から除外してくれる。しかし Databricks は「除外された行そのもの」を
# 参照できる queryable な場所を標準では提供しない。Lakeflow のイベントログ
# （event_log() / system.lakeflow.* system tables）に残るのは
#   - どのルールが
#   - 何件
#   - いつ
# 違反したか、という「集計値」のみであり、「どの行が」「どの値で」違反したのかを
# SQL で SELECT することはできない。
#
# つまり「検疫」は Lakeflow の組み込み機能ではなく、**自分で作るアーキテクチャ
# パターン**である。本パイプラインでは以下の構成でそれを実現している。
#
#   bronze_orders （Auto Loaderで取り込んだ生イベント、streaming）
#        │
#        ├─→ silver_orders            : ORDER_RULES を満たす行だけを残す
#        │                              （@dp.expect_all_or_drop、正常系）
#        │
#        └─→ silver_orders_quarantine : ORDER_RULES のいずれかに違反した行を
#             （このファイル）           明示的に捕捉する（検疫系）
#
# 2つの flow は同じ bronze_orders を独立した streaming read として消費するため
# （Lakeflow は同一テーブルへの複数 flow によるファンアウトをサポートしている）、
# 「正常系に残る行」と「検疫系に残る行」を合わせれば bronze_orders の全行に
# 一致する。これにより「検疫したつもりが実は静かに消えていた」という事故を防ぐ。
#
# 両方の flow が structured_common.quality_rules.ORDER_RULES という**同じ辞書**
# を参照している点が最重要。ルールを追加・変更する際は必ずこの辞書だけを編集し、
# silver_orders.py 側だけ直して silver_orders_quarantine.py 側を直し忘れる、
# という乖離を構造的に起こせないようにしてある。
#
# ============================================================================
# 検疫後のライフサイクル（是正 → 再投入 → 再評価）
# ============================================================================
# このテーブルの行は「削除して終わり」ではない。以下のサイクルを回す。
#   1. Quarantine  : このテーブルへ捕捉される（本ファイル）
#   2. Validation  : gold_data_quality_summary でルール別の違反件数・傾向を確認
#   3. Correction  : reprocessing/reprocess_quarantine.py が是正可能な行を補正
#   4. Reprocessing: 補正済みレコードを Bronze の取り込みVolumeへ"正面から"
#                    再投入する（Silver/Bronzeへ外部から直接 MERGE しない。
#                    Lakeflow が所有するテーブルへパイプライン外から書き込むのは
#                    避け、Auto Loader 経由でもう一度 Bronze→Silver の検証を
#                    通す設計にしている。詳細は reprocess_quarantine.py 冒頭コメント）
#   5. Expectation再評価: 次回のパイプライン更新で silver_orders / このテーブルへ
#                    再度振り分けられる
#
# 是正結果（補正できたか／できなかったか）の記録は、このテーブル自体を UPDATE
# するのではなく、reprocess_quarantine.py が単独で所有する
# quarantine_resolution_log テーブル（Lakeflow管理外の素のDeltaテーブル）に
# 追記する。これにより「Lakeflowが所有するテーブルへパイプライン外から書き込む」
# という曖昧な操作を完全に避けつつ、監査証跡（いつ・誰が・どう是正したか）を
# 失わずに残せる。
#
# 保持期間・VACUUM に関する注意: 検疫テーブルは調査・監査のための唯一の記録なので、
# 通常のSilverより長め、あるいは VACUUM を意図的に緩めることも検討する
# （最低でも90〜180日、規制対象データなら要件に応じてさらに長く）。
from functools import reduce

from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(
    name="silver_orders_quarantine",
    comment="ORDER_RULES のいずれかに違反した受注データ。silver_orders と対になる検疫テーブル。",
    table_properties={
        "quality": "silver",
        "delta.enableDeletionVectors": "true",
        "delta.deletedFileRetentionDuration": "interval 180 days",
        "delta.logRetentionDuration": "interval 180 days",
    },
)
def silver_orders_quarantine():
    # NOTE: この関数はデコレータの引数としてではなく、関数本体の中で ORDER_RULES を
    # 使う（silver_orders.py の @dp.expect_all_or_drop(ORDER_RULES) とは違い、
    # デコレータ引数としては使わない）。そのため sys.path 追加・import を
    # モジュールのトップレベルではなく、ここ（関数の中、Lakeflowが実際に
    # 呼び出す時点）まで遅延できる。bronze_documents.py 等の既存実装と同じ
    # 「関数内で spark.conf.get(...) する」パターンに厳密に合わせてあるため、
    # 動作実績のある形と完全に一致し安全性が高い。
    import sys

    sys.path.append(spark.conf.get("structured_src_root"))
    from structured_common.quality_rules import ORDER_RULES

    bronze = dp.read_stream("bronze_orders")

    rule_conditions = {name: F.expr(predicate) for name, predicate in ORDER_RULES.items()}
    all_rules_pass = reduce(lambda a, b: a & b, rule_conditions.values())

    # 行ごとに「どのルール名で違反したか」を配列にする。
    # 違反していないルールは NULL を挟んで、後段の filter で取り除く。
    violated_rules_with_nulls = F.array(
        *[F.when(~condition, F.lit(rule_name)) for rule_name, condition in rule_conditions.items()]
    )
    violated_rules = F.filter(violated_rules_with_nulls, lambda x: x.isNotNull())

    return (
        bronze.filter(~all_rules_pass)
        .withColumn("violated_rules", violated_rules)
        .withColumn("quarantined_at", F.current_timestamp())
        # PENDING -> REPROCESSED / UNCORRECTABLE は quarantine_resolution_log 側で
        # 追跡する。この列は「最後に検疫された時点でのスナップショット」であり、
        # 是正済みかどうかの正とはしない（正は quarantine_resolution_log）。
        .withColumn("resolution_status_at_quarantine_time", F.lit("PENDING"))
    )
