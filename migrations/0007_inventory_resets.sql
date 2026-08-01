-- 0007_inventory_resets.sql
--
-- Audit trail for admin inventory resets.
--
-- A reset corrects a flavour's sellable stock down to a target box count,
-- keeping the NEWEST boxes (stock is consumed oldest-first, so the newest is
-- what should survive a correction). The correction itself is written as
-- negative `boxes_packed` rows in packing_sessions carrying the same
-- batch_code and production_batch_id as the rows they offset — that way every
-- consumer that sums packing_sessions (8 web screens, the ops-api FIFO, and
-- the Android app) nets out correctly without any change to those consumers.
--
-- Those negative rows are indistinguishable from ordinary packing corrections
-- once written, so this table is the record of what a reset actually did:
-- who ran it, what the stock was before, and which adjustment rows belong to
-- it (adjustment_session_ids makes an undo buildable later).
--
-- Adjustment rows inherit the session_date of the row they offset, never
-- today's date — dashboard-home computes "packed today" with a
-- `session_date = today` filter, and a negative row dated today would corrupt
-- that day's production figure.
--
-- RLS note: the browser client connects with the anon key and no auth session
-- (src/app/core/supabase.service.ts), so anon needs explicit policies. Creating
-- the table without them is what silently broke gg_invoice_flavor_aliases —
-- writes failed with 42501 into a console.warn and nobody noticed.
--
-- Rollback:
--   drop table if exists public.inventory_resets;

create table if not exists public.inventory_resets (
  id uuid primary key default gen_random_uuid(),

  flavor_id   uuid not null references public.gg_flavors (id),
  -- Snapshot of the name at reset time so the audit row stays readable even if
  -- the flavour is later renamed.
  flavor_name text not null default '',

  -- Available boxes before the reset, and the target the admin asked for.
  previous_available integer not null,
  target_available   integer not null,
  boxes_reset        integer not null,

  -- Per-batch record: [{ batch_code, session_date, available_before, keep, reset }]
  batch_breakdown jsonb not null default '[]'::jsonb,

  -- packing_sessions.id values inserted by this reset. Undo = delete these rows.
  adjustment_session_ids uuid[] not null default '{}',

  created_at timestamptz not null default now(),
  -- gg_users.username of the admin who ran it (the app has no Supabase auth
  -- session, so this is recorded by the client rather than derived from a JWT).
  created_by text not null default ''
);

create index if not exists inventory_resets_flavor_idx
  on public.inventory_resets (flavor_id, created_at desc);

create index if not exists inventory_resets_created_at_idx
  on public.inventory_resets (created_at desc);

alter table public.inventory_resets enable row level security;

grant select, insert on public.inventory_resets to anon, authenticated;

-- Read: the reset dialog shows the flavour's reset history.
drop policy if exists inventory_resets_select on public.inventory_resets;
create policy inventory_resets_select
  on public.inventory_resets
  for select
  to anon, authenticated
  using (true);

-- Insert: written once per reset, immediately after the adjustment rows land.
drop policy if exists inventory_resets_insert on public.inventory_resets;
create policy inventory_resets_insert
  on public.inventory_resets
  for insert
  to anon, authenticated
  with check (true);

-- Deliberately no update/delete policy: an audit trail should be append-only.
-- An undo deletes the adjustment rows in packing_sessions and writes a NEW
-- audit row; it does not erase the original.

-- Applied to prod: 2026-08-01 by productiongudgum-git
