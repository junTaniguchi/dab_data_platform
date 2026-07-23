"""Bronze -> Silver -> Gold -> Vector Search Index 同期の E2E テスト。

実際の Databricks ワークスペースにデプロイ済みのバンドルに対して実行する結合テストのため、
ローカルの pytest やヘッドレスCIでは既定でスキップされる。実行するには:

    export DATABRICKS_HOST=https://dbc-a2d384f2-d156.cloud.databricks.com
    export DATABRICKS_TOKEN=...          # または databricks CLI プロファイルでの認証
    export RAG_DAB_CATALOG=rag_data_platform
    export RAG_DAB_SCHEMA=rag_dev
    pytest tests/integration -m integration

`databricks bundle deploy` でこのバンドルをデプロイし、
rag_pipeline_job（seed_sample_data -> rag_pipeline_etl）を一度実行済みであることを前提とする。
"""
import os
import time

import pytest

pytestmark = pytest.mark.integration

REQUIRED_ENV_VARS = ("DATABRICKS_HOST", "RAG_DAB_CATALOG", "RAG_DAB_SCHEMA")


def _missing_env_vars() -> list[str]:
    return [name for name in REQUIRED_ENV_VARS if not os.environ.get(name)]


@pytest.fixture(scope="module")
def workspace_client():
    missing = _missing_env_vars()
    if missing:
        pytest.skip(f"integration test requires env vars: {', '.join(missing)}")

    from databricks.sdk import WorkspaceClient

    return WorkspaceClient()


@pytest.fixture(scope="module")
def catalog_schema():
    return os.environ["RAG_DAB_CATALOG"], os.environ["RAG_DAB_SCHEMA"]


def test_bronze_silver_gold_tables_exist(workspace_client, catalog_schema):
    catalog, schema = catalog_schema
    expected_tables = {
        "bronze_documents",
        "silver_parsed_documents",
        "gold_document_chunks_for_search",
        "gold_chunk_metrics_by_department",
        "gold_chunk_metrics_by_method",
    }

    tables = {
        t.name
        for t in workspace_client.tables.list(catalog_name=catalog, schema_name=schema)
    }

    missing = expected_tables - tables
    assert not missing, f"expected tables not found in {catalog}.{schema}: {missing}"


def test_gold_document_chunks_has_rows_from_both_methods(workspace_client, catalog_schema):
    catalog, schema = catalog_schema
    warehouse_id = os.environ.get("RAG_DAB_WAREHOUSE_ID")
    if not warehouse_id:
        pytest.skip("RAG_DAB_WAREHOUSE_ID not set; cannot run SQL against a warehouse")

    query = (
        f"SELECT chunk_method, COUNT(*) AS chunk_count "
        f"FROM {catalog}.{schema}.gold_document_chunks_for_search "
        f"GROUP BY chunk_method"
    )
    statement = workspace_client.statement_execution.execute_statement(
        warehouse_id=warehouse_id, statement=query, wait_timeout="30s"
    )

    result_rows = statement.result.data_array or []
    methods_with_rows = {row[0] for row in result_rows if int(row[1]) > 0}

    assert {"ai_prep_search", "fixed_overlap"} <= methods_with_rows


def test_gold_chunk_ids_are_unique(workspace_client, catalog_schema):
    catalog, schema = catalog_schema
    warehouse_id = os.environ.get("RAG_DAB_WAREHOUSE_ID")
    if not warehouse_id:
        pytest.skip("RAG_DAB_WAREHOUSE_ID not set; cannot run SQL against a warehouse")

    query = (
        f"SELECT COUNT(*) AS total, COUNT(DISTINCT chunk_id) AS distinct_ids "
        f"FROM {catalog}.{schema}.gold_document_chunks_for_search"
    )
    statement = workspace_client.statement_execution.execute_statement(
        warehouse_id=warehouse_id, statement=query, wait_timeout="30s"
    )

    total, distinct_ids = statement.result.data_array[0]
    assert int(total) == int(distinct_ids), "duplicate chunk_id detected in gold table"


def test_vector_search_index_is_synced(workspace_client):
    index_name = os.environ.get(
        "RAG_DAB_VECTOR_INDEX",
        f"{os.environ['RAG_DAB_CATALOG']}.{os.environ['RAG_DAB_SCHEMA']}.rag_document_chunks_index",
    )

    deadline = time.monotonic() + 300
    last_status = None
    while time.monotonic() < deadline:
        index = workspace_client.vector_search_indexes.get_index(index_name)
        last_status = index.status.ready if index.status else None
        if last_status:
            break
        time.sleep(10)

    assert last_status, f"vector search index {index_name} did not become ready in time"
