# Bronze: raw_structured_data/orders 配下の受注イベント(JSON)を取り込む。
#
# 通常、Bronzeは「生データを一切加工せず、失われる前に保管する」層である。
# ただし本テーブルには1つだけ例外がある: payment_card_number（クレジットカード番号）。
#
# reference table の「匿名化・仮名化」行が明示している通り、クレジットカード番号・
# 銀行口座番号・生体認証情報は "Bronzeの anonymization 対象" ＝ Bronzeであっても
# 平文で一度も永続化してはならない項目である。これは「Bronze=無加工」という原則より
# 優先される、コンプライアンス上の絶対要件（PCI-DSS等）。
#
# そのため本ファイルだけは、Auto Loader が読み込んだ直後・Deltaへ書き込む前に
#   1. カード番号の下4桁（payment_card_last4）のみを保持（不正利用時の突合用）
#   2. ソルト付きSHA-256ハッシュ（payment_card_hash）を計算
#   3. 生のカード番号列は select から除外し、テーブルに一切書き込まない
# を行う。ソルトはコード中に書かず、Databricks Secrets（Key Vault等と連携した
# Secret Scope）から取得する。scope/key名はパイプラインの configuration
# （structured_pipeline_etl.pipeline.yml の pii_hash_salt_secret_scope/key）経由で
# 渡す。事前に `databricks secrets create-scope` / `put-secret` でスコープを
# 作成しておく必要がある（README「Secretsの事前準備」参照）。
#
# email/phone/address のような Silver層匿名化対象の項目は orders には存在しない
# （顧客の連絡先情報は customers 側で扱う）ため、ここでは追加の匿名化は行わない。
#
# 【再処理（reprocessing）で入ってくるレコードとの互換性】
# reprocessing/reprocess_quarantine.py は、検疫から是正したレコードをこの
# Bronzeの取り込みVolumeへ"生のJSONとして"再投入する（正面から Bronze -> Silver
# の検証をもう一度通すため）。しかし検疫テーブル（silver_orders_quarantine）は
# Bronzeより後段なので、生の payment_card_number はそもそも保持していない
# （＝再構築不可能。これはBronzeで即座に破棄する設計として正しい）。
# そのため再投入されるJSONは payment_card_number の代わりに、
# 検疫テーブルに残っている payment_card_hash / payment_card_last4 を
# そのまま引き継いで持つ（既にハッシュ化済みの値を運ぶだけなので、これによって
# 生カード番号が復元されることは無い）。
# 本関数は、入力JSONに payment_card_number（初回取り込み）があればその場で
# ハッシュ化し、無ければ payment_card_hash/payment_card_last4（再処理由来）を
# そのまま採用する、という coalesce ロジックで両方のケースに対応する。
#
# 注意: 再処理由来のJSONに初めて payment_card_hash 等の新しい列が出現すると、
# Auto Loader のスキーマ推論がそれを新規列として検知し、schemaEvolutionMode=
# addNewColumns により一度ストリームが再起動する（`UnknownFieldException` で
# 一時的にジョブが失敗したように見えるが、これはAuto Loaderの正常な仕様であり、
# 自動で再起動して継続する）。詳細はREADME「再処理ジョブ実行後の挙動」参照。
from pyspark import pipelines as dp
from pyspark.sql import functions as F

RAW_ORDERS_SUBPATH = "orders"


@dp.table(
    name="bronze_orders",
    comment="raw_structured_data/orders 配下の受注イベントを Append Only で取り込む Bronze テーブル。カード番号は取り込み時点でハッシュ化済み。",
    table_properties={
        "quality": "bronze",
        "delta.deletedFileRetentionDuration": "interval 30 days",
        "delta.logRetentionDuration": "interval 30 days",
    },
)
# --- Bronze = Warn（観測のみ。ここでは絶対に行を落とさない） ---
@dp.expect("order_id_present", "order_id IS NOT NULL")
@dp.expect("amount_present", "amount IS NOT NULL")
def bronze_orders():
    raw_volume_path = spark.conf.get("raw_orders_path")

    secret_scope = spark.conf.get("pii_hash_salt_secret_scope")
    secret_key = spark.conf.get("pii_hash_salt_secret_key")
    # dbutils は Lakeflow の実行環境で spark と同様にグローバルへ注入される
    # （bronze_documents.py が spark.conf.get(...) をimportなしで呼べるのと同じ理由）。
    card_hash_salt = dbutils.secrets.get(scope=secret_scope, key=secret_key)

    raw = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{raw_volume_path}/_schema")
        # Bronzeのみ schemaEvolutionMode = addNewColumns を許可する。
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(f"{raw_volume_path}/{RAW_ORDERS_SUBPATH}")
    )

    return raw.select(
        F.col("order_id"),
        F.col("customer_id"),
        F.to_date(F.col("order_date")).alias("order_date"),
        F.col("amount").cast("double").alias("amount"),
        F.col("currency"),
        F.col("status"),
        F.col("region"),
        F.col("unit_price").cast("double").alias("unit_price"),
        F.col("discount_rate").cast("double").alias("discount_rate"),
        F.col("source_system"),
        F.col("ingestion_batch"),
        # --- ここから、カード番号を一切平文で残さない変換 ---
        # 初回取り込み(payment_card_numberあり)ならその場でハッシュ化。
        # 再処理由来(payment_card_number無し、既にハッシュ化済みの値のみ)ならそのまま採用。
        F.coalesce(
            F.substring(F.col("payment_card_number"), -4, 4),
            F.col("payment_card_last4"),
        ).alias("payment_card_last4"),
        F.coalesce(
            F.sha2(F.concat(F.col("payment_card_number"), F.lit(card_hash_salt)), 256),
            F.col("payment_card_hash"),
        ).alias("payment_card_hash"),
        # payment_card_number 列そのものは select に含めない = Deltaへは書き込まれない。
        F.coalesce(F.col("reprocessed_from_quarantine"), F.lit(False)).alias(
            "reprocessed_from_quarantine"
        ),
        F.col("_metadata.file_path").alias("source_file"),
        F.current_timestamp().alias("ingestion_time"),
    )
