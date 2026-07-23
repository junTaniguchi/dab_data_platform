# Bronze: UC Volume に配置された生ドキュメントファイルをそのまま取り込む
#
# 想定するボリュームレイアウト（sample_data/documents 参照）:
#   /Volumes/<catalog>/<schema>/raw_documents/<department>/<classification>/<file_name>
#
# department / classification はファイルパスから抽出し、そのまま Silver/Gold まで
# 引き継いで ABAC（行フィルタ）の判定属性列として使用する。
from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(
    name="bronze_documents",
    comment="raw_documents Volume 配下の生ドキュメントを取り込む Bronze テーブル",
    table_properties={"quality": "bronze"},
)
def bronze_documents():
    # Lakeflow Direct Publishing の実行環境では、このファイルは
    # `prepare_to_execute_user_notebook_in_isolated_pipeline_module(...)` により
    # 分離モジュールとして exec されるため `__file__` が定義されず、モジュールの
    # トップレベルで `sys.path.append(str(Path(__file__)...))` すると
    # `NameError: name '__file__' is not defined` になる。そのため src/rag_pipeline_etl/common
    # への絶対パスは、パイプラインの configuration（rag_pipeline_etl.pipeline.yml の
    # `rag_src_root: ${workspace.file_path}/src/rag_pipeline_etl`）経由で
    # spark.conf.get() から取得し、関数内で import する。
    import sys

    sys.path.append(spark.conf.get("rag_src_root"))
    from common.path_parsing import RAW_PATH_PATTERN

    raw_volume_path = spark.conf.get("raw_volume_path")

    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "binaryFile")
        .option("pathGlobFilter", "*.*")
        .option("recursiveFileLookup", "true")
        .load(raw_volume_path)
        .select(
            # Unity Catalog では input_file_name() が使えない
            # (`UC_COMMAND_NOT_SUPPORTED: ... Please use _metadata.file_path instead`)。
            # binaryFile ソースは path 列をそのまま持っているので、そちらを使えば十分。
            F.col("path").alias("doc_path"),
            F.regexp_extract(F.col("path"), RAW_PATH_PATTERN, 1).alias("department"),
            F.regexp_extract(F.col("path"), RAW_PATH_PATTERN, 2).alias("classification"),
            F.element_at(F.split(F.col("path"), "/"), -1).alias("file_name"),
            F.element_at(F.split(F.col("path"), "\\."), -1).alias("file_extension"),
            F.col("content").alias("raw_content"),
            F.col("length").alias("file_size_bytes"),
            F.col("modificationTime").alias("source_modification_time"),
            F.current_timestamp().alias("ingestion_time"),
        )
    )
