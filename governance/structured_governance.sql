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
--
--    テーブル単位で以下6種類を可能な限り付与する（該当しないものは省略）。
--      - domain          : 業務ドメイン（本パイプラインは一貫して 'sales'）
--      - source_system   : データ出自システム（customers系='crm', orders系='pos'。
--                          Bronzeで確定した値を Silver/Gold にもそのまま引き継ぐ
--                          ＝「このテーブルの行は突き詰めるとどこから来たか」を
--                          レイヤーを跨いで即座に追跡できるようにするため）
--      - business_owner  : 業務オーナー（ETL系='data-engineering',
--                          BI/分析寄りのGold集計='sales-ops'）
--      - confidentiality : 機密度。生PIIを保持するほど・監査前の生データほど
--                          高くする（restricted > confidential > internal）
--      - regulation      : 適用法規。個人情報を含む/そこから派生した列がある
--                          テーブルには 'appi'（個人情報保護法）、
--                          決済カード情報（ハッシュ化済みでも）を含むテーブルには
--                          'pci_dss' を付与する（複数該当する場合はカンマ区切り）
--    さらに列単位で pii タグを付与し、「どの列にどの種類の個人情報が
--    入っているか」を列レベルで検索・棚卸しできるようにする（値は
--    「識別子の種類（未加工か仮名化済みかを含む）」を表す）。
--
--    【実機で遭遇した落とし穴】 `ALTER TABLE IDENTIFIER(:catalog || '.' || :schema
--    || '.テーブル名') SET TAGS ('k1'='v1', 'k2'='v2', ...)` のように、動的に組み立てた
--    IDENTIFIER(...) と複数キーの SET TAGS を組み合わせると
--    `[PARSE_SYNTAX_ERROR] Syntax error at or near 'TAGS'` になることを実機で確認した
--    （ALTER TABLE ... ALTER COLUMN ... SET TAGS の列単位の形では発生しない。
--    テーブル単位の SET TAGS 特有の構文上の制約と考えられる）。そのため本セクションの
--    テーブル単位の SET TAGS だけは、ファイル冒頭の USE CATALOG/USE SCHEMA が
--    設定済みであることを利用し、IDENTIFIER()を使わずテーブル名のみで参照する。
-- ============================================================================

-- --- Bronze: customers（未加工の直接PIIを保持する最も機密度が高いテーブル） ---
ALTER TABLE bronze_customers
  SET TAGS (
    'domain' = 'sales',
    'source_system' = 'crm',
    'business_owner' = 'data-engineering',
    'confidentiality' = 'restricted',
    'regulation' = 'appi'
  );

ALTER TABLE bronze_customers
  ALTER COLUMN name SET TAGS ('pii' = 'name');
ALTER TABLE bronze_customers
  ALTER COLUMN email SET TAGS ('pii' = 'email');
ALTER TABLE bronze_customers
  ALTER COLUMN phone SET TAGS ('pii' = 'phone');
ALTER TABLE bronze_customers
  ALTER COLUMN address SET TAGS ('pii' = 'address');
ALTER TABLE bronze_customers
  ALTER COLUMN birth_date SET TAGS ('pii' = 'birth_date');

-- --- Bronze: orders（決済カード情報は取り込み時点でハッシュ化済みだが、
--     ハッシュ値・下4桁自体も PCI-DSS の管理対象であり続けるため regulation を付与） ---
ALTER TABLE bronze_orders
  SET TAGS (
    'domain' = 'sales',
    'source_system' = 'pos',
    'business_owner' = 'data-engineering',
    'confidentiality' = 'restricted',
    'regulation' = 'appi,pci_dss'
  );

ALTER TABLE bronze_orders
  ALTER COLUMN customer_id SET TAGS ('pii' = 'customer_id');
ALTER TABLE bronze_orders
  ALTER COLUMN payment_card_last4 SET TAGS ('pii' = 'payment_card_partial');
ALTER TABLE bronze_orders
  ALTER COLUMN payment_card_hash SET TAGS ('pii' = 'payment_card_hashed');

-- --- Silver: customers（email/phone/birth_date/addressは匿名化済みだが、
--     name はそのまま引き継いでいるため直接PIIとして扱う） ---
ALTER TABLE silver_customers
  SET TAGS (
    'domain' = 'sales',
    'source_system' = 'crm',
    'confidentiality' = 'confidential',
    'regulation' = 'appi',
    'business_owner' = 'data-engineering'
  );

ALTER TABLE silver_customers
  ALTER COLUMN customer_id SET TAGS ('pii' = 'customer_id');
ALTER TABLE silver_customers
  ALTER COLUMN name SET TAGS ('pii' = 'name');
ALTER TABLE silver_customers
  ALTER COLUMN email_hash SET TAGS ('pii' = 'email_hashed');
ALTER TABLE silver_customers
  ALTER COLUMN phone_masked SET TAGS ('pii' = 'phone_masked');
ALTER TABLE silver_customers
  ALTER COLUMN birth_year SET TAGS ('pii' = 'birth_year_generalized');
ALTER TABLE silver_customers
  ALTER COLUMN address_region SET TAGS ('pii' = 'address_generalized');

-- --- Silver: orders（bronze_ordersの列をそのまま引き継ぐため、
--     customer_id・カード関連列は同様にPII扱い） ---
ALTER TABLE silver_orders
  SET TAGS (
    'domain' = 'sales',
    'source_system' = 'pos',
    'confidentiality' = 'internal',
    'regulation' = 'appi,pci_dss',
    'business_owner' = 'data-engineering'
  );

ALTER TABLE silver_orders
  ALTER COLUMN customer_id SET TAGS ('pii' = 'customer_id');
ALTER TABLE silver_orders
  ALTER COLUMN payment_card_last4 SET TAGS ('pii' = 'payment_card_partial');
ALTER TABLE silver_orders
  ALTER COLUMN payment_card_hash SET TAGS ('pii' = 'payment_card_hashed');

-- --- Silver: orders_quarantine（是正前の生の違反行を保持するため、
--     confidentialityはsilver_ordersより一段階高い'restricted'にする） ---
ALTER TABLE silver_orders_quarantine
  SET TAGS (
    'domain' = 'sales',
    'source_system' = 'pos',
    'confidentiality' = 'restricted',
    'regulation' = 'appi,pci_dss',
    'business_owner' = 'data-engineering'
  );

ALTER TABLE silver_orders_quarantine
  ALTER COLUMN customer_id SET TAGS ('pii' = 'customer_id');
ALTER TABLE silver_orders_quarantine
  ALTER COLUMN payment_card_last4 SET TAGS ('pii' = 'payment_card_partial');
ALTER TABLE silver_orders_quarantine
  ALTER COLUMN payment_card_hash SET TAGS ('pii' = 'payment_card_hashed');

-- --- Gold: customer_summary（customersから派生した匿名化済みPIIを含む） ---
ALTER TABLE gold_customer_summary
  SET TAGS (
    'domain' = 'sales',
    'source_system' = 'crm',
    'confidentiality' = 'confidential',
    'regulation' = 'appi',
    'business_owner' = 'sales-ops'
  );

ALTER TABLE gold_customer_summary
  ALTER COLUMN customer_id SET TAGS ('pii' = 'customer_id');
ALTER TABLE gold_customer_summary
  ALTER COLUMN name SET TAGS ('pii' = 'name');
ALTER TABLE gold_customer_summary
  ALTER COLUMN email_hash SET TAGS ('pii' = 'email_hashed');
ALTER TABLE gold_customer_summary
  ALTER COLUMN phone_masked SET TAGS ('pii' = 'phone_masked');
ALTER TABLE gold_customer_summary
  ALTER COLUMN address_region SET TAGS ('pii' = 'address_generalized');
ALTER TABLE gold_customer_summary
  ALTER COLUMN birth_year SET TAGS ('pii' = 'birth_year_generalized');

-- --- Gold: daily_sales_by_region（region/date/statusの集計のみでPII列は無い） ---
ALTER TABLE gold_daily_sales_by_region
  SET TAGS (
    'domain' = 'sales',
    'source_system' = 'pos',
    'confidentiality' = 'internal',
    'business_owner' = 'sales-ops'
  );

-- --- Gold: data_quality_summary（検疫状況の集計のみでPII列は無い） ---
ALTER TABLE gold_data_quality_summary
  SET TAGS (
    'domain' = 'sales',
    'source_system' = 'pos',
    'confidentiality' = 'internal',
    'business_owner' = 'data-engineering'
  );

-- --- Gold: order_quality_gate（customer_idを含むためPII/regulationを付与） ---
ALTER TABLE gold_order_quality_gate
  SET TAGS (
    'domain' = 'sales',
    'source_system' = 'pos',
    'confidentiality' = 'internal',
    'regulation' = 'appi',
    'business_owner' = 'data-engineering'
  );

ALTER TABLE gold_order_quality_gate
  ALTER COLUMN customer_id SET TAGS ('pii' = 'customer_id');

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
