-- 0003_auto_dispatch_invoice.sql
--
-- Phase 5 of the inventory work.
--
-- Adds a trigger on dispatch_events that auto-flips
-- gg_invoices.is_dispatched = true once every line item on an invoice
-- has been fully covered by committed dispatch_events (matched by
-- invoice_number + flavor_id, summed by boxes_dispatched).
--
-- Why a trigger (not app code):
--   * Mobile and web both write dispatch_events; we want one place
--     enforcing the invariant.
--   * No round trip — invoice flips the instant the last truck logs.
--
-- Direction: forward-only.
--   * Whenever conditions are first met → flips to true.
--   * If a dispatch_event is later un-dispatched, the invoice flag
--     does NOT auto-revert. Reversals require manual SQL.
--   * Rationale: most "un-dispatch" events are mistakes being corrected
--     immediately. Auto-reversing creates flicker that confuses ops.
--
-- Idempotent (uses CREATE OR REPLACE + DROP TRIGGER IF EXISTS).
--
-- Rollback:
--   DROP TRIGGER IF EXISTS trg_dispatch_events_auto_invoice ON dispatch_events;
--   DROP FUNCTION IF EXISTS fn_invoice_auto_dispatch_check();

CREATE OR REPLACE FUNCTION fn_invoice_auto_dispatch_check()
RETURNS TRIGGER AS $$
DECLARE
  v_invoice_id     uuid;
  v_items          jsonb;
  v_item           jsonb;
  v_flavor_id      text;
  v_needed         numeric;
  v_dispatched     numeric;
  v_all_satisfied  boolean := true;
BEGIN
  -- Only act when the new/updated dispatch_event is committed.
  IF NEW.is_dispatched IS NOT TRUE THEN
    RETURN NEW;
  END IF;

  -- Need an invoice_number to look up the invoice.
  IF NEW.invoice_number IS NULL OR NEW.invoice_number = '' THEN
    RETURN NEW;
  END IF;

  -- Find a not-yet-dispatched invoice with this invoice_number.
  SELECT id, items
    INTO v_invoice_id, v_items
    FROM gg_invoices
   WHERE invoice_number = NEW.invoice_number
     AND is_dispatched IS NOT TRUE
   LIMIT 1;

  IF v_invoice_id IS NULL THEN
    -- No matching pending invoice (already dispatched, or unknown number).
    RETURN NEW;
  END IF;

  IF v_items IS NULL OR jsonb_typeof(v_items) <> 'array' OR jsonb_array_length(v_items) = 0 THEN
    -- Invoice has no line items — nothing to satisfy.
    RETURN NEW;
  END IF;

  -- Walk each invoice line item; require every flavor to be fully covered.
  FOR v_item IN SELECT * FROM jsonb_array_elements(v_items)
  LOOP
    v_flavor_id := v_item->>'flavor_id';
    v_needed    := COALESCE((v_item->>'quantity_boxes')::numeric, 0);

    -- Skip malformed/zero rows.
    IF v_flavor_id IS NULL OR v_flavor_id = '' OR v_needed <= 0 THEN
      CONTINUE;
    END IF;

    SELECT COALESCE(SUM(boxes_dispatched), 0)
      INTO v_dispatched
      FROM dispatch_events
     WHERE invoice_number = NEW.invoice_number
       AND flavor_id::text = v_flavor_id
       AND is_dispatched IS TRUE;

    IF v_dispatched < v_needed THEN
      v_all_satisfied := false;
      EXIT;
    END IF;
  END LOOP;

  IF v_all_satisfied THEN
    UPDATE gg_invoices
       SET is_dispatched = true
     WHERE id = v_invoice_id;
  END IF;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_dispatch_events_auto_invoice ON dispatch_events;

CREATE TRIGGER trg_dispatch_events_auto_invoice
  AFTER INSERT OR UPDATE OF is_dispatched, boxes_dispatched, flavor_id, invoice_number
  ON dispatch_events
  FOR EACH ROW
  EXECUTE FUNCTION fn_invoice_auto_dispatch_check();

-- ──────────────────────────────────────────────────────────────────────
-- Verification query (run after the migration):
--
--   SELECT tgname, proname
--     FROM pg_trigger
--     JOIN pg_proc ON pg_proc.oid = pg_trigger.tgfoid
--    WHERE tgname = 'trg_dispatch_events_auto_invoice';
--
--   Expected:
--     trg_dispatch_events_auto_invoice | fn_invoice_auto_dispatch_check
-- ──────────────────────────────────────────────────────────────────────
