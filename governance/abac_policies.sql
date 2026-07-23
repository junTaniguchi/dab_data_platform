-- governance/abac_policies.sql
-- STEP 01: governed tags 付与 + ABAC（属性ベースアクセス制御）行フィルタポリシーの適用
--
-- 前提:
--   ・Unity Catalog ABAC（CREATE POLICY）は 2026-07 時点でプレビュー機能。
--     アカウントで Public Preview が有効になっていること、
--     また構文は正式リリースまでに変更される可能性があるため、実行前に
--     最新の Unity Catalog ABAC ドキュメントで CREATE POLICY の文法を確認すること。
--   ・以下のアカウントグループが事前に作成されていることを想定:
--       - security-admins   : すべての classification / department を閲覧可能な管理者グループ
--       - dept-hr           : department = 'hr' の行を閲覧可能
--       - dept-finance      : department = 'finance' の行を閲覧可能
--       - dept-engineering  : department = 'engineering' の行を閲覧可能
--   ・:catalog / :schema はジョブパラメータ（rag_abac_policies_job.job.yml）から渡される。

USE CATALOG IDENTIFIER(:catalog);
USE SCHEMA IDENTIFIER(:schema);

-- ------------------------------------------------------------------
-- 1. governed tags の付与
--    classification / department 列に、ポリシーが参照できるタグを付与する。
--    タグ自体は列の意味（何を表す属性か）を宣言するためのメタデータで、
--    実際の絞り込みロジックは後段の CREATE POLICY 側に書く。
-- ------------------------------------------------------------------
ALTER TABLE gold_document_chunks_for_search
  ALTER COLUMN classification
  SET TAGS ('governed_attribute' = 'classification');

ALTER TABLE gold_document_chunks_for_search
  ALTER COLUMN department
  SET TAGS ('governed_attribute' = 'department');

-- ------------------------------------------------------------------
-- 2. classification に基づく行フィルタポリシー
--    classification = 'restricted' の行は security-admins グループのみ閲覧可能。
--    それ以外の classification（public / internal / confidential）は全アカウントユーザーに開示。
-- ------------------------------------------------------------------
DROP POLICY IF EXISTS restrict_classification_rows
  ON gold_document_chunks_for_search;

CREATE POLICY restrict_classification_rows
  ON gold_document_chunks_for_search
  COMMENT 'classification=restricted の行は security-admins グループのみ閲覧可能にする ABAC 行フィルタ'
  ROW FILTER
    USING COLUMNS (classification)
    TO `account users`
    FOR ALL ROWS
    WHEN
      classification != 'restricted'
      OR IS_ACCOUNT_GROUP_MEMBER('security-admins');

-- ------------------------------------------------------------------
-- 3. department に基づく行フィルタポリシー
--    department 列に一致する dept-<department> グループのメンバー、
--    または security-admins のみが行を閲覧可能。
--    department = 'general'（全社公開）は誰でも閲覧可能。
-- ------------------------------------------------------------------
DROP POLICY IF EXISTS restrict_department_rows
  ON gold_document_chunks_for_search;

CREATE POLICY restrict_department_rows
  ON gold_document_chunks_for_search
  COMMENT 'department 列に応じて dept-<department> グループのメンバーのみ閲覧可能にする ABAC 行フィルタ'
  ROW FILTER
    USING COLUMNS (department)
    TO `account users`
    FOR ALL ROWS
    WHEN
      department = 'general'
      OR IS_ACCOUNT_GROUP_MEMBER('security-admins')
      OR IS_ACCOUNT_GROUP_MEMBER(CONCAT('dept-', department));
