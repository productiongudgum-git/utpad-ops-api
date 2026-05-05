-- Migration: Create production_batches and production_batch_ingredients tables
-- Run this in the Supabase SQL Editor.
--
-- IMPORTANT: Run the full script in one go. It will drop and recreate the tables
-- cleanly, then reload the PostgREST schema cache.

-- ── Drop existing tables (clean slate) ───────────────────────────────────────
-- CASCADE drops all dependent objects automatically:
--   • packing_sessions_batch_code_fkey
--   • dispatch_events_batch_code_fkey
--   • returns_events_batch_code_fkey
--   • inventory_finished_goods_batch_code_fkey
--   • view v_inventory_ledger
-- The tables themselves (packing_sessions, dispatch_events, etc.) are NOT dropped —
-- only the FK constraints and the view that reference production_batches are removed.
-- ⚠️  Recreate v_inventory_ledger and those FK constraints after running this script
--     if you still need them.
DROP TABLE IF EXISTS production_batch_ingredients CASCADE;
DROP TABLE IF EXISTS production_batches CASCADE;

-- ── production_batches ────────────────────────────────────────────────────────
CREATE TABLE production_batches (
    batch_code      TEXT PRIMARY KEY,
    sku_id          TEXT NOT NULL,
    recipe_id       TEXT NOT NULL,
    production_date DATE NOT NULL,
    worker_id       TEXT NOT NULL,
    planned_yield   FLOAT8,
    actual_yield    FLOAT8,
    status          TEXT NOT NULL DEFAULT 'open',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ── production_batch_ingredients ─────────────────────────────────────────────
-- NOTE: No FK from batch_code → production_batches because the Android app
-- inserts ingredients FIRST, before the batch row exists (so a FK would block
-- every insert). The batch_code is the logical link; enforce consistency in app.
CREATE TABLE production_batch_ingredients (
    id              BIGSERIAL PRIMARY KEY,
    batch_code      TEXT NOT NULL,
    ingredient_id   TEXT NOT NULL,
    planned_qty     FLOAT8 NOT NULL,
    actual_qty      FLOAT8 NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_batch_ingredient UNIQUE (batch_code, ingredient_id)
);

-- ── Row Level Security ───────────────────────────────────────────────────────
ALTER TABLE production_batches ENABLE ROW LEVEL SECURITY;
ALTER TABLE production_batch_ingredients ENABLE ROW LEVEL SECURITY;

-- Allow the anon key (used by the Android app) to read and write
CREATE POLICY "anon can select production_batches"
    ON production_batches FOR SELECT TO anon USING (true);

CREATE POLICY "anon can insert production_batches"
    ON production_batches FOR INSERT TO anon WITH CHECK (true);

CREATE POLICY "anon can select production_batch_ingredients"
    ON production_batch_ingredients FOR SELECT TO anon USING (true);

CREATE POLICY "anon can insert production_batch_ingredients"
    ON production_batch_ingredients FOR INSERT TO anon WITH CHECK (true);

-- ── Force PostgREST schema cache reload ──────────────────────────────────────
-- This tells PostgREST to discover the new tables immediately.
NOTIFY pgrst, 'reload schema';
