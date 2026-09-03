-- 0008_flavor_pack_variants.sql
--
-- Packing variants: the same gum sold in a different box format.
--
-- Lemon is normally 15 gums to a box. Sometimes a customer wants it packed
-- differently — say 10 to a box, in its own monocarton. A 10-box and a
-- 15-box are physically different goods, so they cannot share a stock line:
-- every stock query in this system (web Inventory, the D2C FIFO in
-- index.ts, PackAllocationService, the Android dispatch FIFO,
-- /finished-goods-available, Reports) computes availability as
--
--   sum(packing_sessions.boxes_packed) - sum(dispatch_events.boxes_dispatched)
--
-- grouped by flavor_id alone. If one flavour meant two box formats, FIFO
-- would cheerfully allocate a 15-box against an order for a 10-box, and the
-- packing-materials trigger would deduct the wrong monocarton. The variant
-- therefore has to be part of the stock identity, not a label on the
-- invoice line.
--
-- The cheapest correct way to do that here is to give the variant its own
-- gg_flavors row, linked to its parent:
--
--   parent_flavor_id   NULL  = this is a base flavour
--                      set   = this is a packing variant of that flavour
--   units_per_box            how many gums go in one box (default 15)
--   default_customer_id      the customer this pack is normally for; a hint
--                            that floats the variant to the top of the
--                            invoice picker, NOT a restriction
--
-- Because flavor_id is already the stock key, every consumer keeps variants
-- apart with no change: packing_sessions, dispatch_events, returns_events,
-- gg_invoices.items, and deduct_packing_materials() (which matches on
-- gi.packing_flavor_id = NEW.flavor_id, so the variant's own monocarton is
-- registered against the variant row and deducts correctly).
--
-- units_per_box defaults to 15 and backfills every existing flavour, so all
-- history stays valid and reads exactly as it did before this migration.
-- Base flavours ARE the 15s variant; nothing needs re-pointing.
--
-- Variants are packed, never produced. A production batch is always a base
-- flavour; the packer chooses the box format at packing time, so one batch
-- can be split across formats. Production flavour pickers must therefore
-- filter to parent_flavor_id IS NULL.
--
-- Nesting is one level deep — a variant cannot itself have variants. That is
-- enforced by trigger below rather than a CHECK, because the rule spans rows.
--
-- RLS: gg_flavors already has policies covering the anon browser client;
-- adding columns to an existing table inherits them, so no new grants here.
--
-- Rollback:
--   drop trigger if exists trg_gg_flavors_variant_depth on public.gg_flavors;
--   drop function if exists fn_gg_flavors_variant_depth();
--   drop index if exists gg_flavors_parent_idx;
--   drop index if exists gg_flavors_default_customer_idx;
--   alter table public.gg_flavors
--     drop column if exists parent_flavor_id,
--     drop column if exists units_per_box,
--     drop column if exists default_customer_id;

-- 1. Columns (idempotent)
alter table public.gg_flavors
  add column if not exists parent_flavor_id    uuid references public.gg_flavors (id),
  add column if not exists units_per_box       integer not null default 15,
  add column if not exists default_customer_id uuid references public.gg_customers (id);

-- A box has to hold at least one gum. Guards against a typo'd 0 silently
-- producing units_packed = 0 on every session for that variant.
alter table public.gg_flavors
  drop constraint if exists gg_flavors_units_per_box_positive;
alter table public.gg_flavors
  add constraint gg_flavors_units_per_box_positive
  check (units_per_box > 0);

-- 2. Indexes — the variant list is fetched per parent, and the invoice picker
--    looks variants up by customer.
create index if not exists gg_flavors_parent_idx
  on public.gg_flavors (parent_flavor_id)
  where parent_flavor_id is not null;

create index if not exists gg_flavors_default_customer_idx
  on public.gg_flavors (default_customer_id)
  where default_customer_id is not null;

-- 3. One level of nesting only.
--
-- Two rules, both needed:
--   a) a new/updated row may not point at a parent that is itself a variant
--   b) a row that already has children may not be turned into a variant
--
-- Without (b) you could create Lemon 10s under Lemon, then edit Lemon to be a
-- variant of something else, and end up two deep by the back door.
create or replace function fn_gg_flavors_variant_depth()
returns trigger as $$
declare
  v_parent_is_variant boolean;
  v_has_children      boolean;
begin
  if new.parent_flavor_id is null then
    return new;
  end if;

  if new.parent_flavor_id = new.id then
    raise exception 'A flavour cannot be a packing variant of itself.';
  end if;

  select (parent_flavor_id is not null)
    into v_parent_is_variant
    from public.gg_flavors
   where id = new.parent_flavor_id;

  if v_parent_is_variant then
    raise exception
      'Packing variants nest one level only — % is already a variant.',
      new.parent_flavor_id;
  end if;

  select exists (
    select 1 from public.gg_flavors where parent_flavor_id = new.id
  ) into v_has_children;

  if v_has_children then
    raise exception
      'This flavour already has packing variants, so it cannot become one itself.';
  end if;

  return new;
end;
$$ language plpgsql;

drop trigger if exists trg_gg_flavors_variant_depth on public.gg_flavors;

create trigger trg_gg_flavors_variant_depth
  before insert or update of parent_flavor_id
  on public.gg_flavors
  for each row
  execute function fn_gg_flavors_variant_depth();

-- 4. Refresh PostgREST's schema cache so the new columns are visible to the
--    web app and the Android client immediately.
notify pgrst, 'reload schema';

-- ──────────────────────────────────────────────────────────────────────
-- Verification (run after applying):
--
--   select column_name, data_type, column_default, is_nullable
--     from information_schema.columns
--    where table_name = 'gg_flavors'
--      and column_name in ('parent_flavor_id','units_per_box','default_customer_id');
--
--   -- every existing flavour backfilled to 15, no variants yet:
--   select count(*) filter (where units_per_box = 15)      as at_fifteen,
--          count(*) filter (where parent_flavor_id is not null) as variants
--     from public.gg_flavors;
--
--   -- nesting guard works (both should raise):
--   -- update gg_flavors set parent_flavor_id = id where id = '<any id>';
-- ──────────────────────────────────────────────────────────────────────

-- Applied to prod: 2026-09-03 by productiongudgum-git
