-- 0002_invoice_batch_allocation.sql
--
-- Phase 1 of the inventory work.
--
-- Adds gg_invoices.allocated_batches — a JSONB column that records which
-- production batches an invoice has been packed against. Populated by the
-- Mark Packed flow (FIFO reservation). Read by the Inventory screen to
-- compute "Reserved" stock per flavor.
--
-- Shape (TypeScript): { flavor_id: string; batch_code: string; boxes_reserved: number }[]
--
-- Example:
--   [
--     { "flavor_id": "c5492475-...", "batch_code": "BD0426", "boxes_reserved": 30 },
--     { "flavor_id": "c5492475-...", "batch_code": "BE0427", "boxes_reserved": 5  },
--     { "flavor_id": "906fbcf2-...", "batch_code": "BG0501", "boxes_reserved": 50 }
--   ]
--
-- A flavor may appear multiple times if its boxes were drawn from multiple
-- batches (FIFO across the batch list).
--
-- Idempotent: safe to run more than once.
--
-- Rollback:
--   ALTER TABLE gg_invoices DROP COLUMN IF EXISTS allocated_batches;

ALTER TABLE gg_invoices
  ADD COLUMN IF NOT EXISTS allocated_batches jsonb;

COMMENT ON COLUMN gg_invoices.allocated_batches IS
  'FIFO batch reservation set by the Mark Packed flow. Each entry: { flavor_id, batch_code, boxes_reserved }. Null when unpacked.';

-- GIN index lets the inventory screen efficiently aggregate reserved boxes
-- across packed-but-not-dispatched invoices when computing "available" stock.
CREATE INDEX IF NOT EXISTS gg_invoices_allocated_batches_idx
  ON gg_invoices USING gin (allocated_batches);
