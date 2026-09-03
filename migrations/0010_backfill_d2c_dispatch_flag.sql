-- 0010_backfill_d2c_dispatch_flag.sql
--
-- Release D2C channel allocations that have been stuck as "reserved" forever.
--
-- The D2C → Allocations tab writes one dispatch_events row per FIFO batch,
-- tagged `invoice_number = 'D2C-<allocation id>'`, so that allocating boxes to
-- a channel immediately reduces sellable stock. It never set `is_dispatched`.
--
-- The Inventory screen counts an event as dispatched when EITHER the event's
-- own is_dispatched is true, OR its parent invoice is dispatched — matched by
-- invoice_number. A `D2C-<uuid>` reference matches no gg_invoices row, so
-- neither condition can ever hold. Every one of these rows is classified as
-- RESERVED, is subtracted from Available, and has nothing that could ever
-- release it: there is no invoice to ship and reservations do not expire.
--
-- Observed effect: Cardamom showed Available -64 from On-hand -24 and 40 boxes
-- "reserved" against `D2C-98f34d70-…` for Amazon — an allocation that was
-- always meant to be a completed dispatch. The reserved-by-invoice panel even
-- labels these FULL, which is meaningless: with no invoice there is no
-- "needed" figure, and the status rule calls anything with needed = 0 full.
--
-- The ops-api's newer D2C request path already sets is_dispatched = true and
-- says why in a comment. Only the older Allocations tab was missed. The web
-- fix ships alongside this migration; this repairs the rows already written.
--
-- Safe with respect to the auto-dispatch trigger: fn_invoice_auto_dispatch_check
-- fires on UPDATE OF is_dispatched, but returns immediately when it cannot find
-- a pending gg_invoices row for the invoice_number — which is precisely the case
-- for every `D2C-%` reference. No invoice flags are touched.
--
-- Rows zeroed by a re-allocation (clearD2CDispatchEvents sets
-- boxes_dispatched = 0 rather than deleting, to keep the audit trail) are
-- included. Flagging a zero-box row as dispatched moves no stock.
--
-- Rollback:
--   update public.dispatch_events
--      set is_dispatched = false
--    where invoice_number like 'D2C-%';
--   -- Note this also un-flags any allocation made AFTER the web fix ships,
--   -- which would put those boxes back into Reserved. Prefer to roll back by
--   -- restoring the row set captured in the preview query below.

-- ── Run this FIRST to see exactly what will change ────────────────────
--
--   select flavor_id, count(*) as rows, sum(boxes_dispatched) as boxes
--     from public.dispatch_events
--    where invoice_number like 'D2C-%'
--      and is_dispatched is distinct from true
--    group by flavor_id
--    order by boxes desc;
--
-- Each flavour listed will gain that many boxes back in Available.
-- ──────────────────────────────────────────────────────────────────────

update public.dispatch_events
   set is_dispatched = true
 where invoice_number like 'D2C-%'
   and is_dispatched is distinct from true;

-- ──────────────────────────────────────────────────────────────────────
-- Verification (run after applying):
--
--   -- 1. Nothing left stuck. Expect 0.
--   select count(*) from public.dispatch_events
--    where invoice_number like 'D2C-%' and is_dispatched is distinct from true;
--
--   -- 2. Cardamom's phantom reservation is gone. Open Inventory and confirm
--   --    Reserved no longer lists a D2C- reference for it, and Available has
--   --    risen by the boxes that were stuck.
--
--   -- 3. Real invoice reservations are untouched. Expect these to still exist:
--   select count(*) from public.dispatch_events
--    where is_dispatched is distinct from true
--      and invoice_number not like 'D2C-%';
-- ──────────────────────────────────────────────────────────────────────

-- Applied to prod: 2026-09-03 by productiongudgum-git
