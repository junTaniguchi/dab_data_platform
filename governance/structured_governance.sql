-- governance/structured_governance.sql
-- 構造化データ（customers/orders）メダリオンのガバナンス定義。
--
-- rag側の abac_policies.sql とは異なる Unity Catalog の機能を使っている点に注意。
--   - abac_policies.sql       : governed tag + タグ駆動の CREATE POLICY（ABAC）。
--                               1テーブルにつき ROW FILTER ポリシーを1つしか
--                               アタッチできない制約があり、判定ロジックを
--                               1つのUDFへ集約する必要があった。
--   - このファイル             : テーブルへ直接アタッチする Row Filter / Column Mask
--                               （`ALTER TABLE ... SET ROW FILTER` /
--                               `ALTER TABLE ... ALTER COLUMN ... SET MASK`）。
--                               より古くから安定している UC の基本機能で、
--                               governed tag の事前登録が不要。
-- 使い分けの目安: 複数テーブルへ同じロジックを横断適用したい・列の値に基づいて
-- 動的に判定したい場合はタグ駆動ABAC、1テーブルの特定列/行だけを守りたい場合は
-- 本ファイルの直接アタッチ方式、が単純で壊れにくい。
--
-- 前提（実行前に用意しておくこと。未作成でもSQL自体は失敗しない
-- ＝ IS_ACCOUNT_GROUP_MEMBER は単に false を返すだけで fail-closed 側に倒れる）:
--   - security-admins           : 全データ閲覧可能な管理者グループ
--   - region-tokyo / region-osaka / region-nagoya / region-fukuoka / region-sapporo
--                                 : 地域別の閲覧グループ（gold_daily_sales_by_regionのRow Filterで使用）
--   - sales-approvers           : DRAFT（承認前）状態の取引データを閲覧できるグループ
--   - customer-retention-team   : 解約済み(CHURNED)顧客のデータを閲覧できるグループ
--   - pricing-team              : 値引き率(discount_rate)の実値を閲覧できるグループ
--   - data-engineering          : スキーマの Owner に設定するグループ
--
-- :catalog / :schema はジョブパラメータ（structured_abac_policies_job.job.yml）
-- から渡される。schema は resources.schemas.structured_schema.name
-- （dev target ではプレフィックス付き）を渡すこと。

USE CATALOG IDENTIFIER(:catalog);
USE SCHEMA IDENTIFIER(:schema);

-- ============================================================================
-- 0. Owner設定（スキーマ単位）
--    ALTER SCHEMA ... OWNER TO の <securable> 部分も、abac_policies.sql で
--    CREATE POLICY ON SCHEMA について確認した制約と同様、IDENTIFIER() が
--    確実にサポートされているとは限らない（dev targetではスキーマ名が動的な
--    ため、安全側に倒して EXECUTE IMMEDIATE で組み立てる）。
-- ============================================================================
EXECUTE IMMEDIATE
  'ALTER SCHEMA ' || :catalog || '.' || :schema || ' OWNER TO `data-engineering`';

-- ============================================================================
-- 1. 記述的タグ付与（Bronze/Silver/Gold 共通）
--    governed tag（事前account登録が必要・値のホワイトリストを強制できる）は
--    ここでは使わない。値の一覧を強制する必要が無い、単なる分類・検索用の
--    メタデータなので、事前登録不要な plain tag（SET TAGS）で十分。
-- ============================================================================
ALTER TABLE IDENTIFIER(:catalog || '.' || :schema || '.bronze_customers')
  SET TAGS ('domain' = 'sales', 'source_system' = 'crm', 'business_owner' = 'data-engineering');

ALTER TABLE IDENTIFIER(:catalog || '.' || :schema || '.bronze_orders')
  SET TAGS ('domain' = 'sales', 'source_system' = 'pos', 'business_owner' = 'data-engineering', 'confidentiality' = 'restricted');

ALTER TABLE IDENTIFIER(:catalog || '.' || :schema || '.silver_customers')
  SET TAGS ('domain' = 'sales', 'confidentiality' = 'confidential', 'regulation' = 'apppi', 'business_owner' = 'data-engineering');

ALTER TABLE IDENTIFIER(:catalog || '.' || :schema || '.silver_customers')
  ALTER COLUMN email_hash SET TAGS ('pii' = 'email_hashed');

ALTER TABLE IDENTIFIER(:catalog || '.' || :schema || '.silver_customers')
  ALTER COLUMN phone_masked SET TAGS ('pii' = 'phone_masked');

ALTER TABLE IDENTIFIER(:catalog || '.' || :schema || '.silver_orders')
  SET TAGS ('domain' = 'sales', 'confidentiality' = 'internal', 'business_owner' = 'data-engineering');

ALTER TABLE IDENTIFIER(:catalog || '.' || :schema || '.silver_orders_quarantine')
  SET TAGS ('domain' = 'sales', 'confidentiality' = 'internal', 'business_owner' = 'data-engineering');

ALTER TABLE IDENTIFIER(:catalog || '.' || :schema || '.gold_daily_sales_by_region')
  SET TAGS ('domain' = 'sales', 'confidentiality' = 'internal', 'business_owner' = 'sales-ops');

ALTER TABLE IDENTIFIER(:catalog || '.' || :schema || '.gold_customer_summary')
  SET TAGS ('domain' = 'sales', 'confidentiality' = 'confidential', 'business_owner' = 'sales-ops');

-- ============================================================================
-- 2. Row Filter: gold_daily_sales_by_region
--    - 地域別アクセス制御: region に対応する `region-<region>` グループのメンバー、
--      または security-admins のみ閲覧可能。
--    - 承認前・下書き状態の取引の非表示: status = 'DRAFT' の行は
--      sales-approvers または security-admins のみ閲覧可能。
-- ============================================================================
CREATE OR REPLACE FUNCTION IDENTIFIER(
  :catalog || '.' || :schema || '.regional_and_status_visible'
)(region STRING, status STRING)
RETURNS BOOLEAN
COMMENT 'Row Filter: 地域別アクセス制御 + DRAFT(承認前)取引の非表示'
RETURN
  (IS_ACCOUNT_GROUP_MEMBER('security-admins') OR IS_ACCOUNT_GROUP_MEMBER(CONCAT('region-', region)))
  AND (status != 'DRAFT' OR IS_ACCOUNT_GROUP_MEMBER('security-admins') OR IS_ACCOUNT_GROUP_MEMBER('sales-approvers'));

ALTER TABLE IDENTIFIER(:catalog || '.' || :schema || '.gold_daily_sales_by_region')
  SET ROW FILTER IDENTIFIER(:catalog || '.' || :schema || '.regional_and_status_visible') ON (region, status);

-- ============================================================================
-- 3. Row Filter: gold_customer_summary
--    退職者・解約済み顧客(status = 'CHURNED')のデータを、
--    customer-retention-team または security-admins 以外からは隠す。
-- ============================================================================
CREATE OR REPLACE FUNCTION IDENTIFIER(
  :catalog || '.' || :schema || '.customer_visibility'
)(status STRING)
RETURNS BOOLEAN
COMMENT 'Row Filter: 解約済み(CHURNED)顧客の非表示'
RETURN
  status != 'CHURNED'
  OR IS_ACCOUNT_GROUP_MEMBER('security-admins')
  OR IS_ACCOUNT_GROUP_MEMBER('customer-retention-team');

ALTER TABLE IDENTIFIER(:catalog || '.' || :schema || '.gold_customer_summary')
  SET ROW FILTER IDENTIFIER(:catalog || '.' || :schema || '.customer_visibility') ON (status);

-- ============================================================================
-- 4. Column Mask: gold_customer_summary.discount_rate
--    取引先ごとの値引き率は pricing-team（または security-admins）以外には
--    NULL を返す。ストレージ上の実値は変えず、クエリ時にのみ動的に隠す点が
--    Silver層の匿名化（silver_customers.py の email_hash 等、書き込み時に
--    不可逆変換する）との違い。
-- ============================================================================
CREATE OR REPLACE FUNCTION IDENTIFIER(
  :catalog || '.' || :schema || '.mask_discount_rate'
)(discount_rate DOUBLE)
RETURNS DOUBLE
COMMENT 'Column Mask: pricing-team以外には値引き率を非開示にする'
RETURN
  CASE
    WHEN IS_ACCOUNT_GROUP_MEMBER('security-admins') OR IS_ACCOUNT_GROUP_MEMBER('pricing-team')
      THEN discount_rate
    ELSE NULL
  END;

ALTER TABLE IDENTIFIER(:catalog || '.' || :schema || '.gold_customer_summary')
  ALTER COLUMN discount_rate SET MASK IDENTIFIER(:catalog || '.' || :schema || '.mask_discount_rate');
