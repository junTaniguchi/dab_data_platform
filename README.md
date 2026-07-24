# dab_data_platform

Databricks Free Edition（workspace: `dbc-a2d384f2-d156`）向けに構築した、Databricks Asset
Bundle（DAB）です。同一バンドル内に2つの独立した Lakeflow パイプラインを定義しています。

1. **RAGパイプライン**（`rag_pipeline_etl`）: 非構造化ドキュメント向けメダリオン基盤。
   Lakeflow Declarative Pipelines による Bronze/Silver/Gold ETL、Unity Catalog ABAC
   による行レベルアクセス制御、Vector Search index の同期までを実装。
2. **構造化データパイプライン**（`structured_pipeline_etl`）: customers/orders のような
   構造化データ向けメダリオン基盤。Lakeflow SDP（Spark Declarative Pipelines）による
   Bronze/Silver/Gold、AUTO CDC（SCD Type 2）、**検疫（Quarantine）による品質ゲート**、
   Row Filter / Column Mask を実装。詳細は
   [「構造化データパイプライン」セクション](#構造化データパイプラインcustomersorders)を参照。

## 構成（RAGパイプライン）

```
dab_data_platform/
├── .github/workflows/                 # GitHub Actions（CI: test+validate, CD: bundle deploy）
├── databricks.yml                     # DAB定義
├── pyproject.toml                     # 依存関係（pyspark, databricks-sdk, pytest 等）
├── resources/
│   ├── rag_unity_catalog.yml          # スキーマ/Volumeの宣言的作成（既存カタログ配下）
│   ├── rag_pipeline_etl.pipeline.yml  # Lakeflow SDP（bronze/silver/gold, STEP 01）
│   ├── rag_pipeline_job.job.yml       # seed_sample_data -> ETL のスケジュール実行
│   ├── rag_vector_search.yml          # vector_search_endpoints + indexes（STEP 02）
│   └── rag_abac_policies_job.job.yml  # governance/abac_policies.sql を適用する sql_task Job
├── src/rag_pipeline_etl/
│   ├── common/                        # pyspark非依存の純粋関数（テスト容易性のため分離）
│   ├── seed/seed_sample_data.py       # サンプルデータをUC Volumeへ登録するスクリプト
│   ├── explorations/sample_exploration.ipynb
│   └── transformations/
│       ├── bronze/bronze_documents.py
│       ├── silver/silver_parsed_documents.py
│       └── gold/
│           ├── stg_chunks_ai_prep_search.py      # 手法A（中間ビュー）
│           ├── stg_chunks_fixed_overlap.py       # 手法B（中間ビュー）
│           ├── gold_document_chunks_for_search.py # UNION統合。ABAC判定属性列込み
│           └── gold_chunk_metrics_views.py       # Genie Space用の集計ビュー（STEP 05）
├── governance/abac_policies.sql       # governed tags + CREATE POLICY（ABAC行フィルタ）
├── sample_data/documents/             # サンプルドキュメント（department/classification別）
└── tests/
    ├── unit/                          # pyspark非依存。common/ の純粋関数のみをテスト
    └── integration/                   # 実ワークスペースに対するE2Eテスト（既定でスキップ）
```

## サンプルデータ

`sample_data/documents/<department>/<classification>/*.txt` に、部署・機密レベルが異なる
6件のサンプル文書を用意しています（架空の企業 "Acme Analytics" の想定）。

| department  | classification | file                          |
|-------------|-----------------|--------------------------------|
| hr          | confidential     | disciplinary_process.txt      |
| hr          | internal          | onboarding_guide.txt           |
| finance     | restricted        | q3_forecast_internal.txt       |
| finance     | internal          | expense_policy.txt             |
| engineering | internal          | architecture_overview.txt      |
| general     | public            | company_faq.txt                 |

`department` / `classification` はファイルパスから抽出され、Bronze -> Silver -> Gold まで
そのまま引き継がれ、`governance/abac_policies.sql` の ABAC 行フィルタが参照する判定属性列
として使われます。

## セットアップ手順

1. Databricks CLI をインストールする（Mac）。

   Homebrew を使う方法（推奨）。
   ```
   brew tap databricks/tap
   brew install databricks
   ```

   Homebrew を使わない場合は公式インストールスクリプトでも可。
   ```
   curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
   ```

   インストール後、バージョンを確認する（0.230系以降を推奨。DAB / Lakeflow / Vector Search
   関連の新しいリソース定義を扱うため、古いバージョンだと構文が認識されないことがある）。
   ```
   databricks -v
   ```

   既に古いバージョンが入っている場合は Homebrew ならアップグレードする。
   ```
   brew upgrade databricks
   ```

2. Free Edition ワークスペースにプロファイルを設定する。**OAuthブラウザログイン（推奨）**を使う。
   ```
   databricks auth login --host https://dbc-a2d384f2-d156.cloud.databricks.com
   ```
   ブラウザが自動で開くので、Databricksアカウントでログイン・許可するとターミナル側で
   認証完了する。手動でトークンを発行・コピー・管理する必要がなく、有効期限が切れても
   再度同じコマンドを実行するだけでよい。プロファイル名はデフォルトでログインした
   アカウントのメールアドレスになる（例: `you@example.com`）。

   設定内容は `~/.databrickscfg` に保存される。認証状態は以下で確認できる。
   ```
   databricks auth describe
   databricks current-user me
   ```

   > **同じhostに複数プロファイルがあり `multiple profiles matched` エラーになる場合**
   > （例えば以前 `databricks configure` を試して host だけの `[DEFAULT]` が残っている等）、
   > `databricks.yml` 側は特定のプロファイル名を固定していない
   > （CI/CDがサービスプリンシパルの環境変数認証を使うため、bundle設定にローカルの
   > プロファイル名を書き込まない方針にしている）。実行時に以下のどちらかで解決すること。
   > ```
   > databricks bundle deploy -t dev --profile <使いたいプロファイル名>
   > # または
   > export DATABRICKS_CONFIG_PROFILE=<使いたいプロファイル名>
   > ```
   > 恒久的に1つに絞りたい場合は `~/.databrickscfg` を編集し、不要なプロファイルの
   > セクションを削除するか `databricks auth login --host <host> --profile <名前>` で
   > 上書きしてもよい。

   **代替: Personal Access Token（PAT）方式**
   `databricks configure --host <host>` を実行すると `Personal access token:` の入力を
   求められる。この場合はワークスペースUI（右上のユーザーアイコン → Settings →
   Developer タブ → Access tokens → Generate new token）で発行したトークン文字列を
   貼り付ける。トークンは発行時にしか表示されないため、その場でコピーしておくこと。
   PATには有効期限があり、失効すると再発行・差し替えが必要になるため、
   通常は上記のOAuthログインを推奨する。

4. SQL ウェアハウス ID を確認し、環境変数として設定する（ABACポリシー適用ジョブが使用）。

   **IDの確認方法**
   ```
   databricks warehouses list
   ```
   もしくはUIで 左サイドバー「SQL」→「SQL Warehouses」→ 対象のウェアハウスをクリックし、
   URL末尾（`.../sql/warehouses/<ID>`）または「Connection details」タブの HTTP Path
   （`/sql/1.0/warehouses/<ID>`）から確認する。Free Edition では既定で
   `Serverless Starter Warehouse` が1つ用意されている。

   **設定方法（`databricks.yml` を書き換えない）**
   `variables.warehouse_id` は `default: ""` のままにしてあり、各自の環境で
   Databricks Asset Bundles の公式な変数上書き機構である
   **環境変数 `BUNDLE_VAR_<変数名>`** を使って値を注入する運用にしている
   （ワークスペースごとに異なる値をリポジトリにコミットしないため）。
   ```
   export BUNDLE_VAR_warehouse_id=<確認したウェアハウスID>
   ```
   `~/.zshrc` 等に追記して恒久化してもよいし、単発なら
   `databricks bundle deploy -t dev --var="warehouse_id=<ID>"` のように
   `--var` オプションでも上書きできる（優先順位は `--var` > `BUNDLE_VAR_*` 環境変数 >
   `targets.<target>.variables` > `variables` の `default`）。
   GitHub Actions での設定方法は [CI/CD](#cicdgithub-actions) セクションを参照。

5. governance/abac_policies.sql 内の `security-admins` / `dept-hr` 等のアカウントグループを
   事前に作成しておく（存在しない場合、ABACポリシーの `IS_ACCOUNT_GROUP_MEMBER` は単に false 扱い
   になり誰も一致しない。未作成のままでもデプロイ自体は失敗しない）。

6. バンドルをデプロイする。**初回はここで `vector_search_indexes` の作成だけ失敗する**
   （Gold テーブルがまだ存在しないため。想定通りの動作なので無視してよい）。
   ```
   databricks bundle deploy -t dev
   ```
   ```
   Error: cannot create resources.vector_search_indexes.rag_document_chunks_index:
   Table 'workspace.<schema>.gold_document_chunks_for_search' does not exist.
   ```

7. サンプルデータ投入 + ETL実行（Bronze → Silver → Gold テーブルを実際に作成する）。
   ```
   databricks bundle run rag_pipeline_job -t dev
   ```
   数分かかる（サーバーレスクラスタの起動込み）。完了後、もう一度デプロイすると
   Gold テーブルが存在するようになるため Vector Search index の作成に進める。
   ```
   databricks bundle deploy -t dev
   ```
   `databricks vector-search-indexes get-index workspace.<schema>.rag_document_chunks_index`
   で `"ready": true` になっていることを確認する（初回同期は数分かかることがある）。

8. ABACポリシーの適用。
   ```
   databricks bundle run rag_abac_policies_job -t dev
   ```
   2回目以降にこのジョブを再実行する場合は、`governance/abac_policies.sql` 冒頭の
   `CREATE GOVERNED TAG` 文をコメントアウトすること（後述の「既知の制約」セクション参照）。

9. Vector Search index の同期状況を Databricks UI（Catalog Explorer > 該当スキーマ >
   Vector Search）、またはCLIの `databricks vector-search-indexes get-index <index名>` で確認する。

`rag_pipeline_job` の schedule は事故防止のため `pause_status: PAUSED` にしてあります。
動作確認後、`resources/rag_pipeline_job.job.yml` を `UNPAUSED` に変更して再デプロイしてください。

## テスト

```
pip install -e ".[dev]"
pytest tests/unit                 # pyspark / Databricks Runtime不要。純粋関数のみを検証
pytest tests/integration -m integration   # 要: デプロイ済みワークスペースへの接続情報（README内の環境変数）
```

`tests/unit` は `pyspark.pipelines`（Lakeflow）に依存しない `src/rag_pipeline_etl/common/` の
純粋関数のみを検証するため、ローカル環境や通常のCIでもそのまま実行できます。

## CI/CD（GitHub Actions）

リポジトリ: https://github.com/junTaniguchi/dab_data_platform

`.github/workflows/ci.yml` と `.github/workflows/cd.yml` を用意しています。実際に動かすには
GitHub 側で以下の設定が必要です。

### 1. 認証情報（Secrets）

Databricks への認証は、個人アクセストークンではなく **サービスプリンシパルの OAuth
（M2M / client credentials）** を使うことを推奨します（CI/CDでの利用がDatabricksの推奨方式で、
個人トークンのように退職・ローテーションで壊れないため）。

Databricksワークスペースでサービスプリンシパルを作成し、対象の catalog/schema/volume/warehouse
に必要な権限（`USE CATALOG`, `USE SCHEMA`, `CREATE TABLE`, `READ VOLUME`/`WRITE VOLUME`,
SQLウェアハウスの `CAN_USE` 等）を付与した上で、GitHubリポジトリの
**Settings > Secrets and variables > Actions** に以下を登録してください。

| Secret name                 | 内容                                                             |
|------------------------------|------------------------------------------------------------------|
| `DATABRICKS_HOST`            | `https://dbc-a2d384f2-d156.cloud.databricks.com`                  |
| `DATABRICKS_CLIENT_ID`       | サービスプリンシパルのクライアントID                              |
| `DATABRICKS_CLIENT_SECRET`   | サービスプリンシパルのシークレット                                |

`warehouse_id` はワークスペース固有の値ではあるが機密情報ではないため、Secret ではなく
**Settings > Secrets and variables > Actions > Variables** タブに **repository variable**
として登録し、ワークフロー内で `BUNDLE_VAR_warehouse_id` 環境変数として渡す
（`databricks.yml` 側は書き換えない。詳細は [セットアップ手順](#セットアップ手順) 参照）。

| Variable name            | 内容                                              |
|----------------------------|---------------------------------------------------|
| `DATABRICKS_WAREHOUSE_ID`   | `databricks warehouses list` で確認したウェアハウスID |

### 2. GitHub Environments（承認フロー・ブランチ保護）

- リポジトリの **Settings > Environments** に `dev` と `prod` の2つの Environment を作成する。
- `prod` には Required reviewers（レビュー承認）を設定し、本番デプロイ前に人の承認を挟む。
- `main` ブランチには Branch protection rule を設定し、`CI` ワークフロー（`unit-test` /
  `bundle-validate`）の成功をマージ必須条件にする。

### 3. ワークフローの役割

- **`ci.yml`**（PR作成時・main push時に実行）
  - `pytest tests/unit -m "not integration"` — pyspark非依存の純粋関数テストを実行
  - `databricks bundle validate -t dev` — バンドル定義の構文・変数解決チェック
- **`cd.yml`**（main への push、または手動実行 `workflow_dispatch`）
  - `databricks bundle deploy -t <dev|prod>` — バンドルをデプロイ
  - `databricks bundle run rag_abac_policies_job -t <dev|prod>` — ABACポリシーを適用
  - 通常の push は `dev` に、`workflow_dispatch` で `target: prod` を選んだ場合のみ `prod`
    environment（レビュー承認）を経由してデプロイする構成にしてある。

### 4. 未実装・要検討事項

- `rag_pipeline_job`（Bronze/Silver/Gold ETL本体）は cd.yml では自動実行していません。
  スケジュール実行（`resources/rag_pipeline_job.job.yml` の cron）に任せるか、
  cd.yml に `databricks bundle run rag_pipeline_job` のステップを追加するかは運用方針次第です。
- 統合テスト（`tests/integration`）をCIに組み込む場合は、`RAG_DAB_*` 環境変数をワークフロー内で
  secrets から設定し、実際にデプロイ済みのdev環境に対して実行するジョブを別途追加してください。
- Databricks CLI のインストールに使っている `databricks/setup-cli@main` は公式Actionです。
  ピン留めしたい場合はタグ付きバージョンに固定してください。

## 既知の制約・手動での対応が必要な部分

このバンドルは Databricks Free Edition のワークスペース（Databricks CLI v1.9.0）に対して
**実際に `bundle deploy` / `bundle run` を最後まで実行し、Bronze→Silver→Gold→Vector Search→
ABAC行フィルタが動作することを確認済み**です。以下は、その過程で判明した実際の制約と、
コード側で対応済みの内容・利用者側で手動対応が必要な内容です。

### コード側で対応済み（設計として理解しておくとよい点）

- **`vector_search_indexes` は実テーブルを要求する**: 中間ビュー（`stg_chunks_*`）や Gold を
  バッチ読み込み（`dp.read`）で作っていると、Lakeflow はテーブルを `MATERIALIZED_VIEW` として
  作成する。Vector Search の delta_sync index は `DESCRIBE HISTORY` が使える実テーブル
  （Change Data Feed対応）を要求するため、`MATERIALIZED_VIEW` だと
  `[EXPECT_TABLE_NOT_VIEW.NO_ALTERNATIVE] ... expects a table but ... is a view` で
  index の同期が失敗する。そのため `stg_chunks_ai_prep_search.py` / `stg_chunks_fixed_overlap.py` /
  `gold_document_chunks_for_search.py` は upstream をすべて `dp.read_stream` にし、
  `gold_document_chunks_for_search` が `STREAMING_TABLE`（実テーブル）になるようにしてある。
  加えて、Vector Search は Change Data Feed も要求する
  （`delta.enableChangeDataFeed = true` を `table_properties` に設定済み）。
- **`vector_search_indexes.endpoint_name` はリソース参照にする**: `${var.xxx}` のような
  ただの変数参照だと、値が同じでもバンドルの依存グラフには乗らず、endpoint 作成前に
  index 作成が走って `AI Search endpoint ... not found (404)` になる。
  `${resources.vector_search_endpoints.rag_vector_search_endpoint.name}` のような
  **リソース参照**にすることで、CLI が正しい順序でデプロイするようにしてある
  （`depends_on` フィールドはこのCLIバージョンでは未サポート）。
- **UDF (`F.udf`) は使わない**: `common/` パッケージを `sys.path.append` して import した
  純粋関数を `F.udf()` でラップして使うと、ドライバでは動いても executor 側で
  `ModuleNotFoundError: No module named 'common.xxx'` になる（UDFのクロージャは
  cloudpickle で別プロセスに転送されるため）。そのため chunk_id 生成・固定長チャンキングは
  `F.sha2` / `sequence`+`substring` のようなネイティブ Spark SQL 式で実装している。
  `common/` の同名関数は参照実装・単体テスト用として残している。
- **`__file__` は使えない**: Lakeflow の変換ファイルは分離モジュールとして exec されるため
  `__file__` が未定義。`spark.conf.get("rag_src_root")`
  （`rag_pipeline_etl.pipeline.yml` の `configuration.rag_src_root` 経由）で代替している。
  同じ理由で `seed_sample_data.py` も `--sample_data_dir` 引数
  （`${workspace.file_path}/sample_data/documents`）でパスを受け取る設計にしてある。
- **`input_file_name()` は Unity Catalog非対応**: `bronze_documents.py` では
  `_metadata.file_path` の代わりに `binaryFile` フォーマットが持つ `path` 列をそのまま使う。
- **spark_python_task の `environment_version`**: 旧 `client: "1"` は
  `Invalid platform channel Client-1` でクラスタ起動に失敗したため `environment_version: "2"`
  に変更済み。
- **dev target の自動プレフィックス**: `mode: development` により、実際に作成される
  スキーマ名には `dev_<user>_` が自動付与される（例: `rag_dev` → `dev_ultia0602_rag_dev`）。
  `${var.schema}` のプレーンな値を使うと存在しないスキーマ名を参照してしまうため、
  Volumeパスやジョブのパラメータでは必ず `${resources.schemas.rag_schema.name}`
  （リソース参照）を使うようにしてある。
- **カタログは新規作成しない**: Free Edition では `CREATE CATALOG` に明示的な
  storage location が必要（"Metastore storage root URL does not exist... provide a
  storage location"、UIからの作成でのみ Default Storage が自動適用される）。そのため
  新規カタログは作らず、既定で存在する管理カタログ `workspace`
  （`variables.catalog` の default）の配下にスキーマ・Volumeだけを作成する構成にしている。
  別のワークスペース/カタログを使う場合は `variables.catalog` の default を変更すること。

### 利用者側で手動対応が必要な部分

- **`ai_parse_document` の戻り値**: `ai_parse_document(content)` は VARIANT を返し、
  `document.pages` はそのVARIANT内のARRAYなので `variant_get(expr, path, 'ARRAY<VARIANT>')`
  でキャストしてから `transform` する必要がある（`:` パス記法のままだと
  `[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE]` になる）。またサンプルデータの `.txt` は
  `ai_parse_document` 非対応で `"error_status":[{"error_message":"Unsupported file format:
  unknown"}]` を返す（PDF/画像等でのみ意味のある結果が得られる）。実データで
  `src/rag_pipeline_etl/explorations/sample_exploration.ipynb` を使って構造を確認し、
  `silver_parsed_documents.py` の `PARSED_TEXT_EXPR` を必要に応じて調整すること。
- **`ai_prep_search` は生テキストではなくパース済みVARIANTを期待する**: 実際に確認した
  ところ、`ai_prep_search(<プレーン文字列>)` はエラーVARIANT
  （`{"error_message":"The input content is invalid.","response":null}`）を返す。
  `stg_chunks_ai_prep_search.py` は `silver_parsed_documents.parsed_text`（文字列）を
  そのまま渡す実装のため、非対応フォーマットのサンプルデータでは常にチャンク0件になる
  （型エラーにはならず安全に空配列になることは確認済み）。実運用でPDF等を使う場合は、
  Silver側で `ai_parse_document` の結果（VARIANT）もそのまま保持してGold側に渡す設計に
  変更したほうがよい可能性がある。
- **Unity Catalog ABAC（`CREATE POLICY` / `CREATE GOVERNED TAG`）**: 実際に動作を確認した
  正しい構文は当初の想定とかなり異なっていた。要点:
  - governed tag のキー（`abac_dimension`）は**account レベルで事前登録が必要**
    （`CREATE GOVERNED TAG <key> VALUES (...)`。UIからも作成可）。未登録のまま
    `ALTER TABLE ... SET TAGS` すると `Unknown tag policy key` になる。
  - `CREATE GOVERNED TAG` に `IF NOT EXISTS` は無く、既に存在すると
    `ALREADY_EXISTS: Tag policy already exists` でジョブ全体が失敗するため、
    **2回目以降の実行前に `governance/abac_policies.sql` 冒頭の
    `CREATE GOVERNED TAG` 行をコメントアウトすること**。
  - 1テーブルに ROW FILTER ポリシーは1つしかアタッチできない
    （`UC_ABAC_MULTIPLE_ROW_FILTERS`）。classification / department の判定は
    1つのUDF・1つのPOLICYにまとめてある。
  - `CREATE POLICY ... ON SCHEMA <securable>` の `<securable>` 部分は `IDENTIFIER()`
    非対応（`ALTER TABLE` 等とは異なる）。dev target のスキーマ名は動的
    （`dev_<user>_...` プレフィックス）なので `EXECUTE IMMEDIATE` で動的SQLとして
    実行している。埋め込む単一引用符は `''` ではなく `CHR(39)` を使うこと
    （`''` はこの用途で正しく機能しないことを確認済み）。
  - `security-admins` / `dept-hr` 等のアカウントグループが未作成の場合、
    `IS_ACCOUNT_GROUP_MEMBER` は単に `false` を返すだけで、ジョブ自体は失敗しない
    （＝管理者以外閲覧不可、というfail-closed側に倒れる）。
- **Vector Search リソース宣言**: `resources.vector_search_endpoints` /
  `resources.vector_search_indexes` は Free Edition でも実際に動作することを確認済み
  （STANDARD endpoint、HYBRID index_subtype）。ただし初回の index 作成・同期には
  数分〜十数分程度かかることがある（`ready: false` の間は
  `"Delta sync index creation is pending endpoint provisioning."` 等のメッセージになる）。
- サンプルデータは `.txt` のプレーンテキストです。実際の非テキスト文書（PDF等）での
  動作確認は別途 Volume に配置して行ってください
  （`bronze_documents.py` / `silver_parsed_documents.py` は拡張子で分岐する実装）。

---

# 構造化データパイプライン（customers/orders）

customers（顧客マスタ・CDC）/ orders（受注ファクト）を題材にした、構造化データ向けの
Lakeflow SDP（Spark Declarative Pipelines）実装です。RAGパイプラインと同じ
`databricks.yml` バンドル内に、独立したパイプライン・ジョブ群として定義しています。

## 概要・アーキテクチャ

```
raw_structured_data (UC Volume)
├── customers/*.csv  ──▶ bronze_customers ──▶ silver_customers_cleaned(view)
│                                                     │  (Drop: CUSTOMER_RULES)
│                                                     ▼
│                                          AUTO CDC (SCD Type 2)
│                                                     ▼
│                                            silver_customers ──▶ gold_customer_summary (MV)
│
└── orders/*.json    ──▶ bronze_orders ──┬─▶ silver_orders             (Drop: ORDER_RULES, 正常系)
                                          │        │
                                          │        ├─▶ gold_daily_sales_by_region (MV)
                                          │        └─▶ gold_order_quality_gate    (MV, Fail)
                                          │
                                          └─▶ silver_orders_quarantine  (ORDER_RULESの否定, 検疫系)
                                                   │
                                                   ▼
                                          gold_data_quality_summary (MV)

reprocessing/reprocess_quarantine.py（Lakeflowの外側で動く別ジョブ）:
  silver_orders_quarantine を読む
    → 是正可能なら補正して bronze_orders の取り込みVolumeへ"生JSON"として再投入
    → 是正結果は quarantine_resolution_log（Lakeflow管理外の素のDeltaテーブル）へ記録
```

Bronze/Silver/Gold それぞれの設計判断は、添付いただいたリファレンス表（Compute /
schemaLocation / Deletion Vectors / Expectation Action / 匿名化 / Row Filter・Column
Mask / Tag / Predictive Optimization）に沿って実装しています。対応関係は本セクション末尾の
[表](#レイヤー別設定一覧添付リファレンス表との対応)にまとめています。

## 構成（ファイルツリー）

```
sample_data/structured/
├── customers/customers_seed.csv       # CDCイベント風の顧客サンプル（SCD2実演用）
├── orders/orders_seed.json            # 受注サンプル（検疫実演用。3件が意図的に不正）
└── orders_incident/                   # 既定では取り込まないオプションデータ
    └── orders_incident_duplicate.json # Gold Fail挙動を確認したい場合のみ手動投入

src/structured_pipeline_etl/
├── structured_common/                 # pyspark非依存の純粋関数（テスト容易性のため分離）
│   ├── quality_rules.py               # ★検疫パターンの要。ORDER_RULES/CUSTOMER_RULES等
│   ├── pii.py                         # 匿名化・仮名化の参照実装（hash/mask/generalize）
│   └── reprocessing_rules.py          # 是正可否の判定ロジック（CORRECTED/UNCORRECTABLE）
├── seed/seed_structured_sample_data.py
├── reprocessing/reprocess_quarantine.py  # 検疫の是正・再投入ジョブ
└── transformations/
    ├── bronze/
    │   ├── bronze_customers.py
    │   └── bronze_orders.py           # カード番号を即時ハッシュ化し生値を一切保持しない
    ├── silver/
    │   ├── silver_customers.py        # AUTO CDC (SCD Type 2) + PII匿名化
    │   ├── silver_orders.py           # 正常系（Drop）
    │   └── silver_orders_quarantine.py # ★検疫系（ORDER_RULESの否定を捕捉）
    └── gold/
        ├── gold_daily_sales_by_region.py  # MV。Row Filter対象
        ├── gold_customer_summary.py       # MV。Row Filter + Column Mask対象
        ├── gold_order_quality_gate.py     # MV。Fail（重複キー等の重大ゲート）
        └── gold_data_quality_summary.py   # MV。検疫状況の観測用サマリ

resources/
├── structured_unity_catalog.yml            # スキーマ/Volume
├── structured_pipeline_etl.pipeline.yml    # Lakeflow SDP本体
├── structured_pipeline_job.job.yml         # seed → ETL のスケジュール実行
├── structured_quarantine_reprocessing_job.job.yml  # 検疫是正ジョブのスケジュール実行
└── structured_governance_job.job.yml       # governance/structured_governance.sql 適用ジョブ

governance/structured_governance.sql   # Owner設定 + 記述タグ + Row Filter + Column Mask

tests/unit/
├── test_structured_quality_rules.py       # 検疫パターンの回帰テスト（データ欠落が無いことを保証）
├── test_structured_pii.py
└── test_structured_reprocessing_rules.py
```

## サンプルデータ

**customers_seed.csv**（`customer_id, name, email, phone, address, birth_date, region,
status, operation, updated_at, source_system`）: CUST001 は INSERT → UPDATE（Osaka
へ転居、`updated_at` が最新）→ UPDATE（Tokyo のまま、`updated_at` は2番目に古い＝**わざと
順序を入れ替えた「遅延到着（Late Arrival）」イベント**）の3件を持ちます。AUTO CDC の
`sequence_by=updated_at` により、ファイル内の記載順に関わらず正しく最新（Osaka）が
採用されることを確認できます。CUST002 は INSERT → DELETE（解約）で `apply_as_deletes`
を実演します。CUST003 は不正なメール形式を持ち、`CUSTOMER_RULES` の
`valid_email_format` に違反して Drop されます（検疫を用意していないため、この顧客は
Silver 以降から跡形もなく消えます。これは意図的な悪い例です。理由は
`silver_customers.py` のコメントを参照）。

**orders_seed.json**（JSON Lines）: 10件のうち3件が `ORDER_RULES` のいずれかに意図的に
違反しています。

| order_id | 違反ルール | 説明 |
|----------|-----------|------|
| ORD1003  | `positive_amount` | `amount = -500.0`（金額が負） |
| ORD1004  | `customer_id_not_null` | `customer_id = null` |
| ORD1005  | `order_date_not_future` | `order_date = "2099-01-01"`（未来日） |

残り7件（ORD1001, 1002, 1006〜1010）は正常に `silver_orders` へ入ります。ORD1007 は
`status = "DRAFT"` で、Gold の Row Filter（承認前取引の非表示）の実演に使います。

## セットアップ手順

RAGパイプラインのセットアップ（Databricks CLI・OAuthログイン・warehouse_id）が
完了している前提で、追加で以下を行います。

### 1. Secretsの事前準備（PIIハッシュ化ソルト）

カード番号・メールアドレスのハッシュ化に使うソルトは、**バンドル変数やコードに直接
書きません**。事前に Secret Scope を作成し、値を投入してください。

```
databricks secrets create-scope structured_pii
databricks secrets put-secret structured_pii hash_salt
# プロンプトでソルト文字列を入力（十分ランダムな値。例: openssl rand -hex 32 の出力）
```

Scope/Key名を変更したい場合は `databricks.yml` の
`variables.pii_hash_salt_secret_scope` / `pii_hash_salt_secret_key` を変更してください
（値そのものではなく、値を保持する場所の"名前"だけをバンドルで管理する設計です）。

サービスプリンシパルでCI/CDから実行する場合は、そのサービスプリンシパルに対象
Secret Scopeの `READ` 権限を付与しておく必要があります。

### 2. ガバナンス用アカウントグループの作成（任意）

`governance/structured_governance.sql` が参照するグループです。未作成でも
SQL自体は失敗しません（`IS_ACCOUNT_GROUP_MEMBER` が単に `false` を返すだけの
fail-closedになります）。

- `security-admins`（既存グループを流用可）
- `region-tokyo` / `region-osaka` / `region-nagoya` / `region-fukuoka` / `region-sapporo`
- `sales-approvers`
- `customer-retention-team`
- `pricing-team`
- `data-engineering`（既存グループを流用可）

### 3. デプロイ・実行

```
databricks bundle deploy -t dev
databricks bundle run structured_pipeline_job -t dev
```

初回実行後、以下でテーブルが作成されていることを確認できます。

```
databricks bundle run structured_governance_job -t dev
```

`structured_pipeline_job` / `structured_quarantine_reprocessing_job` の schedule は
事故防止のため `pause_status: PAUSED` にしてあります。動作確認後、必要に応じて
`UNPAUSED` に変更してください。

## 検疫（Quarantine）の実装方式

**これが本パイプラインの中核です。** Databricks / Lakeflow には「検疫」という名前の
組み込み機能はありません。`@dp.expect_or_drop` のような Expectations は、違反した行を
pipeline の出力から除外する（Drop）か、更新自体を失敗させる（Fail）ことはできますが、
**除外された行そのものを後から SELECT できる場所を提供しません**。イベントログ
（`event_log()` や `system.lakeflow.*` System Tables）に残るのは「どのルールが」
「何件」違反したかという**集計値**だけで、「どの行が」「どの値で」違反したかは
どこにも残りません。

つまり「検疫」は Lakeflow の設定でONにする機能ではなく、**自分で組み立てるアーキテク
チャパターン**です。本実装では次の構成で実現しています。

```
bronze_orders（Auto Loaderで取り込んだ生イベント、streaming）
     │
     ├─→ silver_orders            : ORDER_RULES を満たす行だけを残す（正常系）
     │                              @dp.expect_all_or_drop(ORDER_RULES)
     │
     └─→ silver_orders_quarantine : ORDER_RULES のいずれかに違反した行を明示的に捕捉
                                    （検疫系。違反ルール名を violated_rules 列に記録）
```

2つの flow は同じ `bronze_orders` を**独立した streaming read** として消費します
（Lakeflow は同一テーブルへの複数 flow によるファンアウトをサポートしています）。
両方が `structured_common/quality_rules.py` の **同じ `ORDER_RULES` 辞書**を参照して
いるため、

```
silver_orders の行数 + silver_orders_quarantine の行数 == bronze_orders の行数
```

が常に成り立ちます（`tests/unit/test_structured_quality_rules.py` の
`test_valid_and_quarantine_sets_partition_all_sample_orders_without_loss` で
サンプルデータに対してこの性質を固定しています）。ルールを追加・変更する際は
`quality_rules.py` の辞書だけを編集すればよく、正常系・検疫系のロジックが
乖離する心配がありません。

### レイヤーごとの Expectation Action の使い分け

| レイヤー | Action | 意味 | 実装 |
|---------|--------|------|------|
| Bronze  | Warn   | 観測のみ。壊れていても再処理可能性を優先して保持する | `bronze_customers.py` / `bronze_orders.py` の `@dp.expect(...)` |
| Silver  | Drop（＋検疫） | 個々の行の品質問題。除外はするが「消さず隔離する」 | `silver_orders.py`（正常系）+ `silver_orders_quarantine.py`（検疫系） |
| Gold    | Fail   | 集計・重大な不変条件の違反。個別行の問題ではなく上流契約そのものの破綻とみなし、パイプライン更新自体を止める | `gold_order_quality_gate.py` の `@dp.expect_all_or_fail(...)` |

### 是正・再投入ライフサイクル

検疫テーブルの行は「捕捉して終わり」ではありません。以下のサイクルを
`reprocessing/reprocess_quarantine.py` が回します。

1. **Quarantine**: `silver_orders_quarantine` へ捕捉される（パイプライン内で自動）。
2. **Validation**: `gold_data_quality_summary` でルール別の違反件数・Pass Rateを確認する。
3. **Correction**: `structured_common/reprocessing_rules.py` の
   `decide_resolution()` が、既知のルール違反（`positive_amount` /
   `order_date_not_future`）なら `CORRECTED`、原因不明・機械的に補正不能な違反
   （`customer_id_not_null` / `valid_currency`）なら `UNCORRECTABLE` と判定します。
   1行の中に1つでも `UNCORRECTABLE` な違反があれば、他が補正可能でも全体を
   `UNCORRECTABLE` として扱います（中途半端な補正はしない設計）。
4. **Reprocessing**: `CORRECTED` と判定された行だけを、`apply_correction()` で
   値を補正した上で、Bronzeの取り込みVolume（`raw_structured_data/orders/reprocessed/`）
   へ**生のJSONファイルとして再投入**します。**Silver/Bronzeの Lakeflow 管理テーブルへ
   外部から直接 MERGE/UPDATE することはしません**（Lakeflowパイプラインが所有する
   テーブルへパイプライン外部から書き込む挙動はサポートが曖昧なため）。次回の
   パイプライン実行で Bronze → Silver の Expectations 検証を"もう一度正面から"
   通すことで、安全に再評価させます。
5. **Expectation再評価**: 次回の `structured_pipeline_job` 実行で、再投入された
   レコードが Bronze → Silver へ流れ、正しければ `silver_orders` / `gold_*` へ
   反映されます（違反が残っていれば再び `silver_orders_quarantine` へ入ります）。

是正結果（`CORRECTED`/`UNCORRECTABLE`、理由、時刻）は `silver_orders_quarantine`
自体を UPDATE するのではなく、**`quarantine_resolution_log`** という
`reprocess_quarantine.py` が単独で所有する素の Delta テーブル（Lakeflow管理外）に
追記します。これにより「Lakeflowが所有するテーブルへパイプライン外から書き込む」
という曖昧な操作を完全に避けつつ、監査証跡（いつ・どう是正したか）を失わずに残せます。
`silver_orders_quarantine` は検疫時点のスナップショットとして不変のまま保持されます。

**注意（カード番号のような Bronze で破棄済みの項目について）**: `silver_orders_quarantine`
は Bronze より後段のテーブルなので、Bronzeで即座に破棄したフィールド（生の
`payment_card_number`）はそもそも保持していません。再投入時は `payment_card_hash` /
`payment_card_last4`（既にハッシュ化済みの値）をそのまま引き継ぎ、
`bronze_orders.py` はどちらの形（生カード番号 or ハッシュ済み値）でも取り込める
ように coalesce ロジックを持っています。**一般に、Bronzeで不可逆に破棄した情報は
検疫テーブルから復元できないという制約は、どのフィールドを Bronze 段階の
匿名化対象にするかを設計する際に必ず意識してください。**

再処理ジョブ実行後の挙動: 初めて再処理由来のJSON（`payment_card_hash`等の新しい列を
含む）が Auto Loader に読み込まれると、`cloudFiles.schemaEvolutionMode=addNewColumns`
によりスキーマ進化が発生し、ストリームが一度再起動します（`UnknownFieldException` で
一時的にジョブが失敗したように見えますが、Auto Loaderの正常な仕様で自動的に再起動して
継続します）。

### 検疫状況の確認方法

```sql
-- ルール別の違反件数・全体Pass Rate
SELECT * FROM <catalog>.<schema>.gold_data_quality_summary;

-- 検疫された行そのものを確認する
SELECT * FROM <catalog>.<schema>.silver_orders_quarantine;

-- 是正結果（quarantine_resolution_log は reprocess_quarantine.py を
-- 一度実行するまでは存在しない）
SELECT * FROM <catalog>.<schema>.quarantine_resolution_log;

-- 検疫 × 是正結果を突き合わせ、未解決の件数を確認する
SELECT
  q.order_id,
  q.violated_rules,
  q.quarantined_at,
  l.resolution_status,
  l.resolved_at
FROM <catalog>.<schema>.silver_orders_quarantine q
LEFT JOIN <catalog>.<schema>.quarantine_resolution_log l
  ON q.order_id = l.order_id
ORDER BY q.quarantined_at DESC;
```

### 再処理ジョブの実行

```
databricks bundle run structured_quarantine_reprocessing_job -t dev
databricks bundle run structured_pipeline_job -t dev   # 再投入分を実際にSilverへ反映する
```

## PIIハンドリング: Silverの匿名化 vs Goldの Row Filter/Column Mask

添付リファレンス表の「匿名化・仮名化」行と「Row Filter/Column Mask」行は、実装方式が
まったく異なる2つの仕組みに対応します。混同しないよう明確に分離しています。

| | Silverの匿名化・仮名化 | GoldのRow Filter/Column Mask |
|---|---|---|
| いつ変換されるか | ETL変換の中で**一度だけ**（書き込み時） | **クエリ実行のたびに**（読み取り時） |
| ストレージ上の値 | 変換後の値（不可逆）で永続化される | 元の値のまま変わらない |
| 実装場所 | `silver_customers.py`（Pythonコード） | `governance/structured_governance.sql`（UC機能） |
| 実装方式 | `sha2` によるハッシュ化／`substring`等による部分マスク・一般化 | `SET ROW FILTER` / `ALTER COLUMN ... SET MASK` |
| 誰が結果を変えるか | 変換ロジック自体が固定（誰が見ても同じ結果） | クエリを実行したユーザーのグループ member ship |

実装した匿名化・仮名化（`silver_customers.py`、いずれも `structured_common/pii.py`
に参照実装あり）:

| 項目 | 手法 | 変換前 → 変換後 |
|------|------|-----------------|
| email | ハッシュ化（Hashing） | `aiko.tanaka@example.com` → `email_hash`（SHA-256、64桁16進文字列） |
| phone | 抑制（Suppression） | `090-1111-2222` → `090-****-2222` |
| birth_date | 一般化（Generalization） | `1988-04-12` → `birth_year = "1988"` |
| address | 一般化（Generalization） | 番地情報は保持せず `address_region` のみ引き継ぐ |

実装した Row Filter / Column Mask（`governance/structured_governance.sql`）:

| 対象テーブル | 種別 | 内容 |
|---|---|---|
| `gold_daily_sales_by_region` | Row Filter | `region-<region>` グループのみ閲覧可（地域別アクセス制御） + `status='DRAFT'` は `sales-approvers` のみ閲覧可（承認前取引の非表示） |
| `gold_customer_summary` | Row Filter | `status='CHURNED'` は `customer-retention-team` のみ閲覧可（解約済み顧客の非表示） |
| `gold_customer_summary.discount_rate` | Column Mask | `pricing-team` 以外には `NULL` を返す（取引先ごとの値引き率の保護） |

「Delta Sharingで取得した列」に対する Row Filter は、本サンプルが自己完結型で外部の
Delta Share を消費していないため実装していません。同じ `SET ROW FILTER` の仕組みが
そのまま適用できます。

RAGパイプライン側（`governance/abac_policies.sql`）は、これとは別の
**governed tag駆動のスキーマレベル ABAC ポリシー**（`CREATE POLICY ... ON SCHEMA`）を
使っています。複数テーブルへ横断的にロジックを適用したい場合はそちらの方式、
1テーブルの特定列・行だけを守りたい場合は本セクションの直接アタッチ方式が
シンプルで壊れにくい、という使い分けです。

## レイヤー別設定一覧（添付リファレンス表との対応）

| 項目 | Bronze | Silver | Gold |
|---|---|---|---|
| Compute | Serverless（`serverless: true`） | 同左 | 同左 |
| schemaLocation | `addNewColumns`（Auto Loader） | Auto Loader不使用のため該当なし（SQL/DataFrameでスキーマ明示） | 同左 |
| データの持ち方 | Append Only | orders=Drop済み正常系／customers=SCD Type 2（AUTO CDC, Late Arrival対応） | 最新集計（MV） |
| Deletion Vectors | 設定なし（追記のみのため不要） | 有効化（`delta.enableDeletionVectors=true`） | Silverで有効化済みのため、GoldのMV（Enzyme増分更新）が自動的にその恩恵を受ける |
| table/view | Streaming Table（`@dp.table`） | Streaming Table | Materialized View（`@dp.materialized_view`） |
| 保持期間 | 30日 | 180日 | 365日 |
| Photon / AQE | Serverless下で自動有効（明示設定不要） | 同左 | 同左 |
| Expectation Action | Warn | Drop（+検疫） | Fail |
| 匿名化対象 | カード番号（即時ハッシュ化・生値は一切保存しない） | email/phone/birth_date/address | Silverで完了済み |
| Owner | schema単位（`data-engineering`） | 同左 | 同左 |
| Row Filter | 不要 | 不要 | region/status（`gold_daily_sales_by_region`）、CHURNED顧客（`gold_customer_summary`） |
| Column Mask | 不要 | 不要 | discount_rate（`gold_customer_summary`） |
| Tag | domain/confidentiality/source_system/business_owner等 | 同左 + pii | 同左 |
| Predictive Optimization | アカウントレベルの既定機能に委ねる（本バンドルではOPTIMIZE/VACUUMジョブを個別定義していない） | 同左 | 同左 |

**添付リファレンス表からの補足・修正点**:

- Predictive Optimization は 2024-11-11 以降に作成されたアカウントで既定有効、既存
  アカウントも順次有効化される account レベルの機能のため、本バンドルでは
  `OPTIMIZE`/`VACUUM` を実行する専用ジョブは定義していません。有効化されていない
  ワークスペースでは、必要に応じて `OPTIMIZE <table>` / `VACUUM <table> RETAIN
  <保持期間に対応する時間> HOURS` を手動またはメンテナンスジョブとして実行してください。
- `dp.create_auto_cdc_flow`（AUTO CDC）は `channel: PREVIEW` を要する比較的新しい
  API です。将来 GA・構文変更の可能性があるため、実際にデプロイして
  `silver_customers` が `__START_AT`/`__END_AT` 付きのSCD2出力になっていることを
  確認してください（列名は Preview機能のため変わる可能性があります）。
- 「schemaLocation: Silver/Gold は許可しない」という記載は、Silver/Gold が
  Auto Loader を使わず Lakeflow の宣言的変換（SQL/DataFrame）でスキーマを明示する
  ため、そもそも Auto Loader のスキーマ推論・進化が介在せず自然に成立します
  （明示的に「禁止」を設定する対象が存在しません）。

## Gold Fail挙動を確認する

デフォルトのサンプルデータには重複 `order_id` を含めていません（初回デプロイで
`gold_order_quality_gate` が意図せず失敗して驚かないようにするため）。意図的に
Gold の Fail 挙動（重複キー検知）を確認したい場合は、以下の手順で重複を注入します。

```
databricks fs cp sample_data/structured/orders_incident/orders_incident_duplicate.json \
  dbfs:/Volumes/<catalog>/<schema>/raw_structured_data/orders/orders_incident_duplicate.json
databricks bundle run structured_pipeline_job -t dev
```

`ORD1002` が重複するため、`gold_order_quality_gate` の
`@dp.expect_all_or_fail(GOLD_ORDER_GATE_RULES)` が `no_duplicate_order_id` 違反を検知し、
パイプライン更新が失敗します。重複ファイルを取り除いてから再実行すると成功に戻ります。

## テスト（構造化データパイプライン分）

```
pip install -e ".[dev]"
pytest tests/unit/test_structured_quality_rules.py tests/unit/test_structured_pii.py \
       tests/unit/test_structured_reprocessing_rules.py -v
```

いずれも pyspark / Databricks Runtime 不要（`structured_common/` の純粋関数と
サンプルデータのみを検証する）。既存の `pytest tests/unit` にも含まれる。

## 既知の制約（構造化データパイプライン）

- 本セクションの実装は、RAGパイプラインと同様に実際のワークスペースへの
  `bundle deploy`/`bundle run` を一貫して行った上での動作確認は完了していません
  （Preview機能である `dp.create_auto_cdc_flow` を含むため、特に `silver_customers`
  の SCD2 出力形状は実際にデプロイして確認してください）。RAGパイプライン側で
  判明した既存の制約（`__file__` が使えない、`input_file_name()` 非対応、
  `environment_version` 指定必須 等）は本パイプラインにも同様に適用済みです。
- `gold_data_quality_summary` は検疫が0件の場合、0行になります
  （`gold_data_quality_summary.py` のコメント参照）。
- `reprocess_quarantine.py` は検疫テーブル全体を driver へ `collect()` する実装です。
  デモ・レビュー用途で検疫件数が少数（数百〜数千件程度）であることを前提にしており、
  大規模な検疫件数が常態化する場合は Spark ネイティブな分散処理へ書き換えることを
  検討してください（そもそも検疫件数が常時大量に発生している場合、Silverの
  ルール設計か上流データ品質自体を見直すべきシグナルでもあります）。
