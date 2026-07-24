# Silver: customers の CDC イベントを AUTO CDC（SCD Type 2）で現在/履歴状態へ正規化する。
#
# レイヤー設計:
#   - データの持ち方   : Late Arrival対策として SCD Type 2 を有効化
#                        （sequence_by=updated_at により、順不同で届くUPDATEイベントを
#                          正しい時系列に補正する。サンプルデータの CUST001 は
#                          わざと3件目のUPDATEを「時系列的には2件目より古い」順序で
#                          並べており、AUTO CDCが正しく無視することを確認できる）
#   - Deletion Vectors : 有効化（SCD2運用ではUPDATE/DELETEが頻発するため必須。
#                        Deletion Vectorsを使うと、ファイル全体を書き直さずに
#                        「この行は削除済み」というビットマップだけを追加できる）
#   - Expectation      : Drop（silver_customers_cleaned の時点で無効データを除外）
#   - 保持期間          : 90〜180日
#   - 匿名化            : email→ハッシュ化、phone→部分マスク、birth_date→年のみ一般化、
#                        address→地域(region)レベルへ一般化（番地情報は保持しない）
#
# 【なぜ customers には検疫（quarantine）テーブルを用意していないか】
# orders 側（silver_orders_quarantine.py）では、Drop されたレコードを別テーブルへ
# 明示的に捕捉する「検疫」パターンを実装している。customers でも全く同じパターンを
# 適用できるが、本サンプルでは意図的に「Dropだけで検疫を用意しない場合、何が起きるか」
# の悪い例として customers を残している。
# サンプルデータの CUST003 は不正なメール形式（"yuki.yamamoto.example.com"）を持ち、
# valid_email_format ルールに違反して silver_customers_cleaned で Drop される。
# その結果 CUST003 は Silver 以降のどのテーブルにも一切現れず、跡形もなく消える。
# 検疫を用意していれば「なぜCUST003が消えたか」を silver_customers_quarantine から
# 追跡できたはずである。実運用でこの挙動が許容できない場合は、
# silver_orders_quarantine.py と全く同じ書き方で silver_customers_quarantine を
# 追加すればよい（ORDER_RULES を CUSTOMER_RULES に置き換えるだけ）。
#
# 【実装上の注意: なぜここでは CUSTOMER_RULES を import せず値を直接書いているか】
# @dp.expect_all_or_drop(...) はデコレータなので、その引数は def 文が実行される
# タイミング（＝モジュールのロード時点）で評価済みでなければならない。
# ところが structured_common への sys.path 追加には spark.conf.get(...) が必要で、
# これは既存の bronze_documents.py 等では常に「関数の中」でのみ実行が確認されて
# おり、モジュールのトップレベル（関数の外）で spark が参照可能かは実機で
# 未検証である。デコレータ引数はモジュールのトップレベルで評価されるため、
# ここで spark.conf.get(...) に依存するのは安全とは言えない。
# ORDER_RULES と違い CUSTOMER_RULES はこのファイル以外から参照されないため
# （検疫テーブルを持たない設計。理由は上記コメント参照）、「複数箇所での乖離を
# 防ぐための単一の情報源」という共有化のメリットが無い。そのため、あえて
# structured_common.quality_rules.CUSTOMER_RULES と同じ内容をこのファイル内に
# 直接定義し、import・sys.path操作を避けている。
from pyspark import pipelines as dp
from pyspark.sql import functions as F

# structured_common.quality_rules.CUSTOMER_RULES と同じ内容（上記コメント参照）。
CUSTOMER_RULES: dict[str, str] = {
    "customer_id_not_null": "customer_id IS NOT NULL",
    "valid_operation": "operation IN ('INSERT', 'UPDATE', 'DELETE')",
    "updated_at_not_null": "updated_at IS NOT NULL",
    "valid_email_format": "email IS NOT NULL AND email RLIKE '^[^@\\\\s]+@[^@\\\\s]+\\\\.[^@\\\\s]+$'",
}

SILVER_CUSTOMERS_TABLE_PROPERTIES = {
    "quality": "silver",
    "delta.enableDeletionVectors": "true",
    "delta.deletedFileRetentionDuration": "interval 180 days",
    "delta.logRetentionDuration": "interval 180 days",
}


@dp.view(
    comment="Bronze customers を品質ルールで Drop フィルタし、PIIを匿名化した中間ビュー（AUTO CDCのsourceとしてのみ使用）",
)
@dp.expect_all_or_drop(CUSTOMER_RULES)
def silver_customers_cleaned():
    secret_scope = spark.conf.get("pii_hash_salt_secret_scope")
    secret_key = spark.conf.get("pii_hash_salt_secret_key")
    pii_hash_salt = dbutils.secrets.get(scope=secret_scope, key=secret_key)

    bronze = dp.read_stream("bronze_customers")

    return bronze.select(
        F.col("customer_id"),
        F.col("name"),
        # --- 匿名化・仮名化（このテーブルより先には生の値を一切引き継がない） ---
        F.sha2(F.concat(F.col("email"), F.lit(pii_hash_salt)), 256).alias("email_hash"),
        F.when(
            F.size(F.split(F.col("phone"), "-")) == 3,
            F.concat_ws(
                "-",
                F.split(F.col("phone"), "-").getItem(0),
                F.lit("****"),
                F.split(F.col("phone"), "-").getItem(2),
            ),
        )
        .otherwise(F.lit("****"))
        .alias("phone_masked"),
        F.col("region").alias("address_region"),  # 番地レベルの address は引き継がない
        F.substring(F.col("birth_date"), 1, 4).alias("birth_year"),
        F.col("status"),
        F.col("operation"),
        F.col("updated_at"),
        F.col("source_system"),
        F.col("ingestion_time"),
    )


# AUTO CDC のターゲットは事前に streaming table として宣言しておく
# （table_properties / comment をこのテーブルに設定するため）。
dp.create_streaming_table(
    name="silver_customers",
    comment="顧客の現在／履歴状態（SCD Type 2）。email/phone/birth_date/address は匿名化済み。",
    table_properties=SILVER_CUSTOMERS_TABLE_PROPERTIES,
)

# NOTE: channel: PREVIEW を使用しているため、将来 dp.create_auto_cdc_flow の構文が
# 変わる可能性がある。実際にワークスペースへデプロイし、__START_AT/__END_AT を含む
# SCD2出力になっていることを確認すること。
dp.create_auto_cdc_flow(
    target="silver_customers",
    source="silver_customers_cleaned",
    keys=["customer_id"],
    sequence_by="updated_at",
    stored_as_scd_type=2,
    apply_as_deletes="operation = 'DELETE'",
)
