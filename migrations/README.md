# Migrations

Numbered SQL migrations applied in order against the Supabase Postgres
database backing this app.

## Naming convention

`NNNN_short_description.sql`

- `NNNN` is a zero-padded sequence starting at `0001`
- One migration per feature/change — no megafiles
- Filename in lowercase with underscores

## Authoring rules

Every migration should be:

- **Idempotent** — use `IF NOT EXISTS` / `OR REPLACE` so re-running does
  no harm. This protects us when the same script is applied to multiple
  environments at different times.
- **Reversible** — include a `Rollback:` section in the comment header
  with the exact SQL to undo the change.
- **Self-contained** — no dependencies on uncommitted state, no
  hardcoded IDs from one environment.

## Applying a migration

Two options:

1. **Supabase SQL editor (manual, recommended for now)**
   - Open https://app.supabase.com → your project → SQL Editor
   - Paste the contents of the migration file
   - Run it
   - Note the date applied somewhere (until we set up a tracking table)

2. **Supabase CLI (when we adopt it)**
   ```
   supabase db push
   ```
   This requires the CLI to be linked to the project and the
   `supabase/migrations/` folder structure. We are not on this yet.

## Tracking applied migrations

Until we set up a `_migrations` table, the convention is: when a
migration is applied to production, add a comment to the bottom of the
file:

```
-- Applied to prod: YYYY-MM-DD by <name>
```
