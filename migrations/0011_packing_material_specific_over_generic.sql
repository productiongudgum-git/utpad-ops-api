-- 0011_packing_material_specific_over_generic.sql
--
-- A flavour's own packing material replaces the generic one of that role,
-- instead of both being deducted.
--
-- deduct_packing_materials() matches a material when
--
--     packing_flavor_id = NEW.flavor_id   OR   packing_flavor_id IS NULL
--
-- Both arms can be true at once. That was harmless while only monocartons were
-- flavour-specific and no generic monocarton existed, but it breaks as soon as
-- a packing variant gets its own ziplock: packing one box of "Lemon 10s" would
-- deduct the variant's ziplock AND the generic ziplock that every flavour
-- shares, consuming two pouches for one box.
--
-- Packing variants make that concrete. A ziplock is one per box, but a pouch
-- sized for 15 gums is a physically different item from one sized for 10, so a
-- variant needs its own with its own stock. Its existence must then suppress
-- the parent's generic pouch for that flavour, not add to it.
--
-- The rule added here is per ROLE, so the two kinds of material stay
-- independent:
--
--   Lemon 10s  has its own ziplock and its own monocarton
--              → deducts both; the generic ziplock is skipped
--   Lemon      has a monocarton but no ziplock of its own
--              → deducts its monocarton and the generic ziplock, as before
--
-- Nothing changes for a flavour that has no specific material of a given role,
-- which is every flavour today. This migration is a no-op until the first
-- flavour-specific ziplock exists.
--
-- Keeps the reset-adjustment guard added in 0009 — an inventory correction is
-- not a packing run and must not move materials at all.
--
-- Known limitation, unchanged from before: `active` is not consulted. An
-- inactive material still deducts, and now also still suppresses the generic
-- one. Filtering on active would change behaviour for existing ingredients, so
-- it is left alone deliberately rather than folded in here.
--
-- Rollback: re-apply 0009's version of the function, which has the
-- reset-adjustment guard but not the specificity rule.

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
          -- This flavour's own material for that role always applies.
          gi.packing_flavor_id = NEW.flavor_id
          -- A generic one applies only when the flavour has nothing of its own
          -- in that role, so specific replaces generic rather than stacking.
       OR (
            gi.packing_flavor_id IS NULL
        AND NOT EXISTS (
              SELECT 1
                FROM gg_ingredients AS specific
               WHERE specific.packing_role      = gi.packing_role
                 AND specific.packing_flavor_id = NEW.flavor_id
            )
          )
    );

  RETURN NEW;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

DROP TRIGGER IF EXISTS trg_deduct_packing_materials ON packing_sessions;

CREATE TRIGGER trg_deduct_packing_materials
  AFTER INSERT ON packing_sessions
  FOR EACH ROW
  EXECUTE FUNCTION deduct_packing_materials();

NOTIFY pgrst, 'reload schema';

-- ──────────────────────────────────────────────────────────────────────
-- Verification (run after applying):
--
--   -- 1. Both guards present in the installed function:
--   select prosrc like '%reset-adjustment%' as reset_guard,
--          prosrc like '%specific.packing_role%' as specificity_rule
--     from pg_proc where proname = 'deduct_packing_materials';
--
--   -- 2. Which flavours currently have a material of their own, by role.
--   --    Before any variant exists this should list monocartons only.
--   select packing_role, count(*) as flavours_with_own
--     from gg_ingredients
--    where packing_role is not null and packing_flavor_id is not null
--    group by packing_role;
--
--   -- 3. After creating a variant with its own ziplock, pack one box of it and
--   --    confirm the generic ziplock's current_qty did NOT move, while the
--   --    variant's own ziplock dropped by 1.
-- ──────────────────────────────────────────────────────────────────────

-- Applied to prod: 2026-09-03 by productiongudgum-git
