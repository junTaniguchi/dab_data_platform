# Bronze: raw_structured_data/customers 配下のCDC風レコード(CSV)をそのまま取り込む。
#
# レイヤー設計（README/添付ガイドの「メダリオン設計原則」に準拠）:
#   - Compute        : Serverless Jobs Compute（pipeline.yml で serverless: true）
#   - schemaLocation : cloudFiles.schemaEvolutionMode = addNewColumns（Bronzeのみ許可）
#   - データの持ち方   : Append Only（CDCイベントを1件も上書き・削除せず積み上げる）
#   - Deletion Vectors: 設定不要（Bronzeは追記のみで更新/削除が発生しないため）
#   - Expectation     : Warn（警告のみ。壊れていても再処理可能性を優先して保持する）
#   - 保持期間         : 7〜30日（delta.deletedFileRetentionDuration/logRetentionDuration）
#
# email/phone/address/birth_date はここではまだ生の値のまま保持する。
# これらは「Silver層の匿名化対象」（reference table参照）であり、Bronzeでの
# anonymization対象ではない。Bronzeでの anonymization 対象はクレジットカード番号・
# 銀行口座番号・生体認証情報のような「一度も平文で永続化してはいけない」項目のみ
# （customers には該当項目がないため、ここでは何も変換しない）。
from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(
    name="bronze_customers",
    comment="raw_structured_data/customers 配下のCDCイベントを Append Only で取り込む Bronze テーブル",
    table_properties={
        "quality": "bronze",
        "delta.deletedFileRetentionDuration": "interval 30 days",
        "delta.logRetentionDuration": "interval 30 days",
    },
)
# --- Bronze = Warn（観測のみ。ここでは絶対に行を落とさない） ---
@dp.expect("customer_id_present", "customer_id IS NOT NULL")
@dp.expect("email_looks_like_email", "email IS NULL OR email RLIKE '.+@.+'")
def bronze_customers():
    # raw_customers_path は pipeline.yml の configuration で既に
    # ".../raw_structured_data/customers" まで指しているため、ここでさらに
    # "/customers" を追加しない（二重ネストになり
    # `CF_EMPTY_DIR_FOR_SCHEMA_INFERENCE` で失敗する。実際にワークスペースへ
    # デプロイして遭遇したバグのため、二度と踏まないようこの位置に明記しておく）。
    raw_volume_path = spark.conf.get("raw_customers_path")

    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.schemaLocation", f"{raw_volume_path}/_schema")
        # Bronzeのみ schemaEvolutionMode = addNewColumns を許可する。
        # Silver/Goldは Lakeflow の宣言的変換（SQL/DataFrame）でスキーマを明示するため、
        # Auto Loader のスキーマ推論・進化はそもそも介在しない（=「許可しない」が自然に成立する）。
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(raw_volume_path)
        .select(
            F.col("customer_id"),
            F.col("name"),
            F.col("email"),
            F.col("phone"),
            F.col("address"),
            F.col("birth_date"),
            F.col("region"),
            F.col("status"),
            F.col("operation"),
            F.to_timestamp(F.col("updated_at")).alias("updated_at"),
            F.col("source_system"),
            # Unity Catalog では input_file_name() は非対応
            # (`UC_COMMAND_NOT_SUPPORTED`)。_metadata.file_path を使う。
            F.col("_metadata.file_path").alias("source_file"),
            F.current_timestamp().alias("ingestion_time"),
        )
    )
