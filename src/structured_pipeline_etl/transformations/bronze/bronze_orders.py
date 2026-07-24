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
# そのまま採用する。
#
# 【実機で遭遇した落とし穴】 当初は `F.coalesce(F.col("payment_card_last4"), ...)` の
# ように常に両方の列を参照するコードにしていたが、Auto Loader が一度も
# payment_card_last4 等を含むファイルを読んだことがない状態（＝再処理ジョブを
# 一度も実行していない初回デプロイ時）では、そもそもスキーマにその列が存在しない。
# Sparkは「NULLになる」のではなく `[UNRESOLVED_COLUMN.WITH_SUGGESTION]` で
# 即座に分析エラーになる（存在しない列への参照は、値がNULLになる話ではなく
# 列名解決の話であるため）。そのため `raw.columns` で列の実在を確認してから
# 参照するかどうかを分岐している。再処理ジョブが最初に実行され
# payment_card_hash 等を含むJSONが読み込まれると、schemaEvolutionMode=
# addNewColumns によりスキーマへ新規列として追加され（一度ストリームが
# 再起動する。`UnknownFieldException` で一時的にジョブが失敗したように
# 見えるが、Auto Loaderの正常な仕様で自動的に再起動して継続する）、
# 以後は再処理由来のレコードも同じコードで正しく扱われるようになる。
from pyspark import pipelines as dp
from pyspark.sql import functions as F


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
    # raw_orders_path は pipeline.yml の configuration で既に
    # ".../raw_structured_data/orders" まで指しているため、ここでさらに
    # "/orders" を追加しない（二重ネストになり
    # `CF_EMPTY_DIR_FOR_SCHEMA_INFERENCE` で失敗する。実際にワークスペースへ
    # デプロイして遭遇したバグのため、二度と踏まないようこの位置に明記しておく）。
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
        # reprocess_quarantine.py は補正済みレコードを orders/reprocessed/ 配下へ
        # 書き出す（1階層下）。bronze_documents.py と同じ理由で recursiveFileLookup
        # を明示しておかないと、その再投入ファイルを拾い漏らす。
        .option("recursiveFileLookup", "true")
        .load(raw_volume_path)
    )

    # スキーマに存在しない列は F.lit(None) で「常に存在するが値がNULL」の列に
    # 差し替える。存在する列だけを F.col() で参照することで、
    # UNRESOLVED_COLUMN エラーを避ける（上記コメント参照）。
    has_raw_card_number = "payment_card_number" in raw.columns
    has_reprocessed_hash_columns = "payment_card_hash" in raw.columns and "payment_card_last4" in raw.columns
    has_reprocessed_flag = "reprocessed_from_quarantine" in raw.columns

    # F.lit(None) は型未指定の NullType になるため、substring/sha2/concat 等に
    # 渡してもエラーにならないよう明示的に string/boolean へ cast しておく。
    #
    # 【実機で遭遇した落とし穴: Auto Loaderのスキーマ進化で追加された列は
    # 期待した型で推論されるとは限らない】
    # reprocess_quarantine.py が書き出す再投入JSONには `"reprocessed_from_quarantine":
    # true` のように正しいJSON真偽値リテラルを書いているにもかかわらず、
    # schemaEvolutionMode=addNewColumns によって初めてこの列が追加された際、
    # Auto Loaderの推論結果が STRING 型になり、
    # `coalesce(reprocessed_from_quarantine, false)` が
    # `[DATATYPE_MISMATCH.DATA_DIFF_TYPES]`（STRINGとBOOLEANの混在）で失敗した。
    # 教訓: スキーマ進化で後から追加された列は、たとえ元のJSON値が正しい型で
    # あっても、Auto Loaderの推論結果が期待通りの型になるとは限らない。
    # F.col(...) で取得した列は必ず明示的に .cast(...) してから使うこと
    # （推論結果に依存しない）。
    raw_card_number_col = (
        F.col("payment_card_number").cast("string")
        if has_raw_card_number
        else F.lit(None).cast("string")
    )
    reprocessed_last4_col = (
        F.col("payment_card_last4").cast("string")
        if has_reprocessed_hash_columns
        else F.lit(None).cast("string")
    )
    reprocessed_hash_col = (
        F.col("payment_card_hash").cast("string")
        if has_reprocessed_hash_columns
        else F.lit(None).cast("string")
    )
    reprocessed_flag_col = (
        F.col("reprocessed_from_quarantine").cast("boolean")
        if has_reprocessed_flag
        else F.lit(None).cast("boolean")
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
            F.substring(raw_card_number_col, -4, 4),
            reprocessed_last4_col,
        ).alias("payment_card_last4"),
        F.coalesce(
            F.sha2(F.concat(raw_card_number_col, F.lit(card_hash_salt)), 256),
            reprocessed_hash_col,
        ).alias("payment_card_hash"),
        # payment_card_number 列そのものは select に含めない = Deltaへは書き込まれない。
        F.coalesce(reprocessed_flag_col, F.lit(False)).alias("reprocessed_from_quarantine"),
        F.col("_metadata.file_path").alias("source_file"),
        F.current_timestamp().alias("ingestion_time"),
    )
