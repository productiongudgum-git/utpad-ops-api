-- 0006_invoice_flavor_alias_rls.sql
--
-- Fix: PDF invoice import never remembers flavour mappings.
--
-- `gg_invoice_flavor_aliases` was created with RLS enabled but no write
-- policy, so the alias upsert in the PDF import modal has always failed with
--   42501: new row violates row-level security policy
-- The upsert is fire-and-forget (console.warn only), so the failure was
-- invisible and users re-mapped every flavour on every upload. The table is
-- empty as a result.
--
-- Sibling tables in the same import flow (gg_invoices, gg_customers) accept
-- writes from the anon role — verified by probe — so this migration brings the
-- alias table in line with them rather than inventing a stricter rule for one
-- lookup table.
--
-- The browser client connects with the anon key and no auth session
-- (src/app/core/supabase.service.ts), which is why anon needs the grant;
-- authenticated is included so nothing breaks if real auth lands later.
--
-- Rollback:
--   drop policy if exists gg_invoice_flavor_aliases_select on public.gg_invoice_flavor_aliases;
--   drop policy if exists gg_invoice_flavor_aliases_insert on public.gg_invoice_flavor_aliases;
--   drop policy if exists gg_invoice_flavor_aliases_update on public.gg_invoice_flavor_aliases;
--   drop policy if exists gg_invoice_flavor_aliases_delete on public.gg_invoice_flavor_aliases;

alter table public.gg_invoice_flavor_aliases enable row level security;

grant select, insert, update, delete
  on public.gg_invoice_flavor_aliases
  to anon, authenticated;

-- Read: the import modal loads the full alias map on every PDF parse.
drop policy if exists gg_invoice_flavor_aliases_select
  on public.gg_invoice_flavor_aliases;
create policy gg_invoice_flavor_aliases_select
  on public.gg_invoice_flavor_aliases
  for select
  to anon, authenticated
  using (true);

-- Insert + update together back the `upsert(..., { onConflict: 'description' })`
-- call: a first-time mapping inserts, a corrected mapping updates in place.
drop policy if exists gg_invoice_flavor_aliases_insert
  on public.gg_invoice_flavor_aliases;
create policy gg_invoice_flavor_aliases_insert
  on public.gg_invoice_flavor_aliases
  for insert
  to anon, authenticated
  with check (true);

drop policy if exists gg_invoice_flavor_aliases_update
  on public.gg_invoice_flavor_aliases;
create policy gg_invoice_flavor_aliases_update
  on public.gg_invoice_flavor_aliases
  for update
  to anon, authenticated
  using (true)
  with check (true);

-- Delete: needed to unlearn a mapping that was saved wrong.
drop policy if exists gg_invoice_flavor_aliases_delete
  on public.gg_invoice_flavor_aliases;
create policy gg_invoice_flavor_aliases_delete
  on public.gg_invoice_flavor_aliases
  for delete
  to anon, authenticated
  using (true);

-- Applied to prod: 2026-08-01 by productiongudgum-git
