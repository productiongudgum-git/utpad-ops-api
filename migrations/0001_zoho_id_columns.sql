-- 0001_zoho_id_columns.sql
--
-- Phase 0 of the Zoho-CSV-import migration.
--
-- Adds two stable Zoho-side identifiers used for strict matching during
-- CSV invoice imports:
--
--   gg_flavors.zoho_product_id    e.g. '1469229000000239001'
--   gg_customers.zoho_customer_id e.g. '1469229000000175079'
--
-- Both columns are nullable. Uniqueness is enforced via partial unique
-- indexes that ignore NULLs, so unmapped rows do not conflict with each
-- other.
--
-- Idempotent: safe to run more than once. The legacy Zoho sync code
-- already references gg_customers.zoho_customer_id and
-- gg_invoices.zoho_invoice_id; if those columns already exist this
-- migration is a no-op for them.
--
-- Rollback:
--   DROP INDEX IF EXISTS gg_flavors_zoho_product_id_idx;
--   DROP INDEX IF EXISTS gg_customers_zoho_customer_id_idx;
--   ALTER TABLE gg_flavors   DROP COLUMN IF EXISTS zoho_product_id;
--   ALTER TABLE gg_customers DROP COLUMN IF EXISTS zoho_customer_id;
--

-- ──────────────────────────────────────────────────────────────────────
-- gg_flavors.zoho_product_id
-- ──────────────────────────────────────────────────────────────────────

ALTER TABLE gg_flavors
  ADD COLUMN IF NOT EXISTS zoho_product_id text;

CREATE UNIQUE INDEX IF NOT EXISTS gg_flavors_zoho_product_id_idx
  ON gg_flavors (zoho_product_id)
  WHERE zoho_product_id IS NOT NULL;

COMMENT ON COLUMN gg_flavors.zoho_product_id IS
  'Zoho Books Product ID (Item ID) used for strict matching when importing invoice CSVs. Nullable; populate via the inline mapping UI on first import.';

-- ──────────────────────────────────────────────────────────────────────
-- gg_customers.zoho_customer_id
-- ──────────────────────────────────────────────────────────────────────

ALTER TABLE gg_customers
  ADD COLUMN IF NOT EXISTS zoho_customer_id text;

CREATE UNIQUE INDEX IF NOT EXISTS gg_customers_zoho_customer_id_idx
  ON gg_customers (zoho_customer_id)
  WHERE zoho_customer_id IS NOT NULL;

COMMENT ON COLUMN gg_customers.zoho_customer_id IS
  'Zoho Books Customer ID used as the primary match key when importing invoice CSVs (falls back to case-insensitive name match).';
