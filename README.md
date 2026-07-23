# dab_data_platform

Databricks Free Edition（workspace: `dbc-a2d384f2-d156`）向けに構築した、RAG（Retrieval-Augmented
Generation）用メダリオン基盤の Databricks Asset Bundle（DAB）です。Lakeflow Declarative
Pipelines による Bronze/Silver/Gold ETL、Unity Catalog ABAC による行レベルアクセス制御、
Vector Search index の同期までを1つのバンドルで定義しています。

## 構成

```
dab_data_platform/
├── .github/workflows/                 # GitHub Actions（CI: test+validate, CD: bundle deploy）
├── databricks.yml                     # DAB定義（engine: direct = Direct publishing mode）
├── pyproject.toml                     # 依存関係（pyspark, databricks-sdk, pytest 等）
├── resources/
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
   になり誰も一致しない）。
6. バンドルをデプロイする。
   ```
   databricks bundle deploy -t dev
   ```
7. サンプルデータ投入 + ETL実行。
   ```
   databricks bundle run rag_pipeline_job -t dev
   ```
8. ABACポリシーの適用。
   ```
   databricks bundle run rag_abac_policies_job -t dev
   ```
9. Vector Search index の同期状況を Databricks UI（Catalog Explorer > Vector Search）で確認する。

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

## 既知の制約・要確認事項（プレビュー機能に依存する部分）

このバンドルは 2026-07 時点の情報をもとに作成しており、以下はいずれもプレビュー/新機能で
構文や挙動が変わりうるため、実際にデプロイする前に最新のドキュメントで確認してください。

- **`ai_parse_document` / `ai_prep_search`**: 戻り値 struct のフィールド名は
  `src/rag_pipeline_etl/explorations/sample_exploration.ipynb` で実際に確認し、
  `silver_parsed_documents.py` の `PARSED_TEXT_EXPR` と
  `stg_chunks_ai_prep_search.py` の `AI_PREP_SEARCH_EXPR` を必要に応じて調整してください。
- **Unity Catalog ABAC（`CREATE POLICY`）**: `governance/abac_policies.sql` の構文はプレビュー版
  ドキュメントに基づくベストエフォートの実装です。アカウントで ABAC の Public Preview が
  有効になっていること、グループ名・列名が実環境と一致していることを確認してください。
- **Vector Search リソース（`resources.vector_search_endpoints` / `vector_search_indexes`）**:
  DABでの宣言的サポートもプレビュー段階のため、フィールド名はリリースノートで要確認です。
  当初 `vector_search_indexes` に `depends_on` を書いていましたが、手元の Databricks CLI
  v1.9.0 では `Warning: unknown field: depends_on` として認識されなかったため削除しました
  （`endpoint_name` での参照により依存関係は自動解決されます）。同様に databricks.yml の
  トップレベルにあった `engine: direct` も `Warning: unknown field: engine` となったため
  削除済みです。CLIのバージョンによってサポートされるフィールドが異なるため、
  `databricks bundle validate` の警告は都度確認してください。
- サンプルデータは `.txt` のプレーンテキストです。`ai_parse_document` は本来 PDF / 画像 /
  Office文書向けの機能なので、実際の非テキスト文書での動作確認は別途 volume に
  PDF等を配置して行ってください（`bronze_documents.py` / `silver_parsed_documents.py` は
  拡張子で分岐する実装になっています）。
