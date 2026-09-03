-- 0009_reset_rows_skip_packing_materials.sql
--
-- Stop inventory resets from moving packing-material stock.
--
-- An inventory reset corrects a flavour's box count. It writes adjustment rows
-- into packing_sessions carrying `status = 'reset-adjustment'` (see
-- inventory.component.ts → confirmReset), because that is the table every stock
-- figure is summed from.
--
-- But deduct_packing_materials() fires on EVERY insert into packing_sessions,
-- whatever the status, and moves inventory_raw_materials by
--
--     qty_per_box * NEW.boxes_packed
--
-- A reduction writes a NEGATIVE boxes_packed, so subtracting a negative has
-- been quietly CREDITING monocartons and ziplocks back into raw-material stock
-- on every reset since the feature shipped. Nothing in migration 0007
-- describes that, and it looks like an unnoticed side effect rather than a
-- decision: correcting a box count is an accounting fix, and no monocarton
-- comes back off a shelf because someone recounted.
--
-- It becomes actively wrong once resets can also increase stock (repairing a
-- negative balance). Those rows are POSITIVE, so they would consume packing
-- materials that were never used — and could trip the low-stock alerts on the
-- Ingredients page off the back of a data correction.
--
-- So the trigger now ignores reset adjustments entirely. Real packing sessions
-- ('partial', 'complete', 'topup', or NULL for legacy rows) are unaffected and
-- deduct exactly as before. When packing materials genuinely need correcting,
-- that is done deliberately on the Ingredients page rather than as a side
-- effect of a box-count fix.
--
-- This does not retrospectively undo the credits already applied by past
-- resets; it only changes behaviour from here. If those need unwinding, do it
-- as a separate, explicit correction.
--
-- Rollback (restores the previous behaviour, side effect included):
--   CREATE OR REPLACE FUNCTION deduct_packing_materials()
--   RETURNS TRIGGER AS $$
--   BEGIN
--     UPDATE inventory_raw_materials AS inv
--     SET current_qty = GREATEST(0, inv.current_qty - (gi.qty_per_box * NEW.boxes_packed)),
--         updated_at  = now()
--     FROM gg_ingredients AS gi
--     WHERE gi.id = inv.ingredient_id
--       AND gi.packing_role IS NOT NULL
--       AND (gi.packing_flavor_id = NEW.flavor_id OR gi.packing_flavor_id IS NULL);
--     RETURN NEW;
--   END;
--   $$ LANGUAGE plpgsql SECURITY DEFINER;

CREATE OR REPLACE FUNCTION deduct_packing_materials()
RETURNS TRIGGER AS $$
BEGIN
  -- Inventory corrections are not packing runs. They move box counts only.
  IF NEW.status IS NOT DISTINCT FROM 'reset-adjustment' THEN
    RETURN NEW;
  END IF;

  UPDATE inventory_raw_materials AS inv
  SET current_qty = GREATEST(0, inv.current_qty - (gi.qty_per_box * NEW.boxes_packed)),
      updated_at  = now()
  FROM gg_ingredients AS gi
  WHERE gi.id = inv.ingredient_id
    AND gi.packing_role IS NOT NULL
    AND (
         gi.packing_flavor_id = NEW.flavor_id   -- flavour-specific (monocarton)
      OR gi.packing_flavor_id IS NULL           -- generic (ziplock / other)
    );

  RETURN NEW;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- The trigger itself is unchanged; recreated only so applying this file on a
-- database that never ran 0006's packing-materials migration still ends up
-- with a working trigger.
DROP TRIGGER IF EXISTS trg_deduct_packing_materials ON packing_sessions;

CREATE TRIGGER trg_deduct_packing_materials
  AFTER INSERT ON packing_sessions
  FOR EACH ROW
  EXECUTE FUNCTION deduct_packing_materials();

NOTIFY pgrst, 'reload schema';

-- ──────────────────────────────────────────────────────────────────────
-- Verification (run after applying):
--
--   -- 1. The guard is present in the installed function:
--   select prosrc like '%reset-adjustment%' as guard_installed
--     from pg_proc where proname = 'deduct_packing_materials';
--
--   -- 2. Real packing still deducts. Pick a flavour with a linked monocarton,
--   --    note its inventory_raw_materials.current_qty, insert a normal
--   --    packing_sessions row, and confirm the qty dropped by
--   --    qty_per_box * boxes_packed.
--
--   -- 3. A reset no longer moves materials. Run a reset from the Inventory
--   --    screen and confirm current_qty is unchanged for that flavour's
--   --    monocarton and for the generic ziplocks.
-- ──────────────────────────────────────────────────────────────────────
