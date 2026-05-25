-- 0004_canonical_units.sql
--
-- Phase 10. Normalize all stored quantities to canonical units:
--   weight  → g  (grams)
--   volume  → ml (millilitres)
--   count   → pcs
--
-- Existing rows in kg / L are multiplied by 1000 and their unit column is
-- rewritten. Recipes (gg_recipe_lines.qty) are already in grams per the
-- mobile FIFO wizard and are NOT touched.
--
-- Idempotent: each UPDATE filters on the *old* unit value, then rewrites
-- it. A second run sees only 'g'/'ml'/'pcs' and skips. Safe to re-execute.
--
-- IMPORTANT — run this BEFORE deploying the new mobile build. The new
-- mobile will submit canonical inward events; if old non-canonical rows
-- are still in the DB at that point, displays will show mixed units.
--
-- Rollback (manual; keep this around just in case):
--   UPDATE inventory_raw_materials SET current_qty = current_qty / 1000,
--          low_stock_threshold = low_stock_threshold / 1000, unit = 'kg'
--    WHERE unit = 'g' AND <some criteria to identify converted rows>;
--   ... etc. There is no automatic way back because we have no marker.
--   Suggest taking a Supabase snapshot before applying.

BEGIN;

-- ──────────────────────────────────────────────────────────────────────
-- 1. inventory_raw_materials: kg → g, L → ml
-- ──────────────────────────────────────────────────────────────────────

UPDATE inventory_raw_materials
   SET current_qty         = current_qty * 1000,
       low_stock_threshold = low_stock_threshold * 1000,
       unit                = 'g'
 WHERE unit = 'kg';

UPDATE inventory_raw_materials
   SET current_qty         = current_qty * 1000,
       low_stock_threshold = low_stock_threshold * 1000,
       unit                = 'ml'
 WHERE unit = 'L';

-- ──────────────────────────────────────────────────────────────────────
-- 2. gg_inwarding historical events: kg → g, L → ml
-- ──────────────────────────────────────────────────────────────────────

UPDATE gg_inwarding SET qty = qty * 1000, unit = 'g'  WHERE unit = 'kg';
UPDATE gg_inwarding SET qty = qty * 1000, unit = 'ml' WHERE unit = 'L';

-- ──────────────────────────────────────────────────────────────────────
-- 3. gg_ingredients: convert current_stock + reorder_point BEFORE
--    rewriting default_unit (so we still know which to multiply).
-- ──────────────────────────────────────────────────────────────────────

UPDATE gg_ingredients
   SET current_stock = current_stock * 1000,
       reorder_point = COALESCE(reorder_point, 0) * 1000
 WHERE default_unit = 'kg';

UPDATE gg_ingredients
   SET current_stock = current_stock * 1000,
       reorder_point = COALESCE(reorder_point, 0) * 1000
 WHERE default_unit = 'L';

UPDATE gg_ingredients SET default_unit = 'g'  WHERE default_unit = 'kg';
UPDATE gg_ingredients SET default_unit = 'ml' WHERE default_unit = 'L';

-- ──────────────────────────────────────────────────────────────────────
-- 4. Optional integrity check — enforce canonical unit values on new
--    inserts. Commented out so it doesn't break inserts during the
--    transition. Uncomment once mobile + web are deployed:
--
-- ALTER TABLE gg_inwarding
--   ADD CONSTRAINT gg_inwarding_canonical_unit_chk
--   CHECK (unit IN ('g', 'ml', 'pcs'));
--
-- ALTER TABLE inventory_raw_materials
--   ADD CONSTRAINT inventory_raw_materials_canonical_unit_chk
--   CHECK (unit IN ('g', 'ml', 'pcs'));
--
-- ALTER TABLE gg_ingredients
--   ADD CONSTRAINT gg_ingredients_canonical_unit_chk
--   CHECK (default_unit IN ('g', 'ml', 'pcs'));
-- ──────────────────────────────────────────────────────────────────────

COMMIT;

-- Sanity check after running:
--   SELECT unit, COUNT(*) FROM gg_inwarding GROUP BY unit;
--   SELECT default_unit, COUNT(*) FROM gg_ingredients GROUP BY default_unit;
--   SELECT unit, COUNT(*) FROM inventory_raw_materials GROUP BY unit;
-- Expect only 'g', 'ml', 'pcs' in the unit columns (no 'kg' or 'L').
