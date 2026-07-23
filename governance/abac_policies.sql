-- governance/abac_policies.sql
-- STEP 01: governed tags 付与 + ABAC（属性ベースアクセス制御）行フィルタポリシーの適用
--
-- 2026-07 時点で実際にワークスペースに対して検証済みの構文（`databricks bundle run
-- rag_abac_policies_job` で実行し、実際に行フィルタが機能することを確認した）。
-- ただし ABAC は Public Preview 機能のため、正式リリースまでに構文が変わる可能性がある。
--
-- 検証の過程でわかった、素朴に書くと失敗する重要な制約:
--   1. governed tag のキー（例: abac_dimension）は UI もしくは
--      `CREATE GOVERNED TAG <key> VALUES (...)` で事前に**account レベルで登録**しないと、
--      `ALTER TABLE ... SET TAGS` 時に "Unknown tag policy key" になる。
--      `CREATE GOVERNED TAG` には `IF NOT EXISTS` が無く、既に存在すると
--      `ALREADY_EXISTS: Tag policy already exists` でジョブ全体が失敗する。
--      そのため下記の CREATE GOVERNED TAG 文は、初回実行後は行頭に `--` を付けて
--      コメントアウトすること（このファイルを再実行する2回目以降は不要な文）。
--   2. 同じテーブルに対して ROW FILTER ポリシーを複数アタッチすることはできない
--      （`UC_ABAC_MULTIPLE_ROW_FILTERS: At most one row filter is allowed`）。
--      そのため classification / department の判定は1つの UDF・1つの POLICY にまとめている。
--   3. `CREATE POLICY ... ON SCHEMA <securable>` の `<securable>` 部分は `IDENTIFIER()` を
--      サポートしていない（`ALTER TABLE` 等とは異なる）。dev target では
--      `mode: development` によりスキーマ名に `dev_<user>_` プレフィックスが付き、
--      デプロイのたびに実際のスキーマ名が変わるため、`EXECUTE IMMEDIATE` で
--      動的に組み立てた SQL文字列として実行する。その際、文字列内に埋め込む単一引用符は
--      `''`（二重にしたシングルクォート）ではなく `CHR(39)` を連結する形にすること。
--      複数行・複数の `||` 連結にまたがる長い文字列リテラルの中で `''` エスケープを使うと、
--      実際にジョブを実行した際に引用符が消えてしまい
--      `COMMENT classification/department...`（引用符なしの生テキスト）のような
--      壊れた動的SQLが生成され `[PARSE_SYNTAX_ERROR]` になることを確認済み。
--   4. `DROP POLICY` / `DROP GOVERNED TAG` にも `IF EXISTS` が無い。
--      `CREATE OR REPLACE POLICY` / `CREATE OR REPLACE FUNCTION` は存在するため、
--      再実行可能性が必要なオブジェクトは極力 `OR REPLACE` を使う設計にしている。
--
-- 前提（実行前に用意しておくこと）:
--   ・以下のアカウントグループ（Account Console > User management > Groups）:
--       - security-admins   : すべての classification / department を閲覧可能な管理者グループ
--       - dept-hr / dept-finance / dept-engineering : 部署ごとの閲覧グループ
--     未作成でも SQL自体は失敗しない（IS_ACCOUNT_GROUP_MEMBER は単に false を返すだけ）。
--   ・:catalog / :schema はジョブパラメータ（rag_abac_policies_job.job.yml）から渡される。
--     schema は resources.schemas.rag_schema.name（dev target ではプレフィックス付き）を渡すこと。

USE CATALOG IDENTIFIER(:catalog);
USE SCHEMA IDENTIFIER(:schema);

-- ------------------------------------------------------------------
-- 0. governed tag の登録（account レベル、通常は初回のみ）
--    2回目以降にこのファイルを実行する場合は、この文をコメントアウトすること
--    （`CREATE GOVERNED TAG` に `IF NOT EXISTS` が無く、既に存在すると
--    `ALREADY_EXISTS: Tag policy already exists` でジョブ全体が失敗するため）。
-- ------------------------------------------------------------------
CREATE GOVERNED TAG abac_dimension VALUES ('classification', 'department');

-- ------------------------------------------------------------------
-- 1. governed tags の付与（再実行可能）
--    classification / department 列に、ポリシーが参照できるタグを付与する。
--    タグ自体は列の意味（何を表す属性か）を宣言するためのメタデータで、
--    実際の絞り込みロジックは後段の CREATE FUNCTION / CREATE POLICY 側に書く。
-- ------------------------------------------------------------------
ALTER TABLE IDENTIFIER(:catalog || '.' || :schema || '.gold_document_chunks_for_search')
  ALTER COLUMN classification
  SET TAGS ('abac_dimension' = 'classification');

ALTER TABLE IDENTIFIER(:catalog || '.' || :schema || '.gold_document_chunks_for_search')
  ALTER COLUMN department
  SET TAGS ('abac_dimension' = 'department');

-- ------------------------------------------------------------------
-- 2. 行フィルタ判定用 UDF（再実行可能: CREATE OR REPLACE）
--    classification = 'restricted' の行は security-admins グループのみ閲覧可能。
--    department 列に一致する dept-<department> グループのメンバー、または
--    security-admins のみが閲覧可能（department = 'general' は全員に開示）。
--    2つの条件を1つの関数にまとめているのは、UC ABAC が1テーブルにつき
--    ROW FILTER ポリシーを1つしかアタッチできないため（上記コメント参照）。
-- ------------------------------------------------------------------
CREATE OR REPLACE FUNCTION IDENTIFIER(
  :catalog || '.' || :schema || '.classification_department_visible'
)(classification STRING, department STRING)
RETURNS BOOLEAN
COMMENT 'ABAC行フィルタ判定: classification/department に基づき閲覧可否を返す'
RETURN
  (classification != 'restricted' OR IS_ACCOUNT_GROUP_MEMBER('security-admins'))
  AND (
    department = 'general'
    OR IS_ACCOUNT_GROUP_MEMBER('security-admins')
    OR IS_ACCOUNT_GROUP_MEMBER(CONCAT('dept-', department))
  );

-- ------------------------------------------------------------------
-- 3. ABAC 行フィルタポリシー（再実行可能: CREATE OR REPLACE）
--    SCHEMA レベルにアタッチしているため、abac_dimension タグが付いた
--    classification / department 列を持つ将来のテーブルにも自動適用される。
--    ON SCHEMA の securable 名は IDENTIFIER() 非対応のため EXECUTE IMMEDIATE で組み立てる。
-- ------------------------------------------------------------------
EXECUTE IMMEDIATE
  'CREATE OR REPLACE POLICY restrict_gold_chunks_rows ON SCHEMA ' || :catalog || '.' || :schema
  || ' COMMENT ' || CHR(39) || 'classification/departmentに基づくABAC行フィルタ（gold_document_chunks_for_search等）' || CHR(39)
  || ' ROW FILTER ' || :catalog || '.' || :schema || '.classification_department_visible'
  || ' TO `account users`'
  || ' FOR TABLES'
  || ' MATCH COLUMNS'
  || '   has_tag_value(' || CHR(39) || 'abac_dimension' || CHR(39) || ', ' || CHR(39) || 'classification' || CHR(39) || ') AS classification_col,'
  || '   has_tag_value(' || CHR(39) || 'abac_dimension' || CHR(39) || ', ' || CHR(39) || 'department' || CHR(39) || ') AS department_col'
  || ' USING COLUMNS (classification_col, department_col)';
