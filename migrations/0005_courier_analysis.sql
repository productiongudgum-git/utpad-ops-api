create table if not exists courier_analysis_runs (
  id uuid primary key,
  created_at timestamptz not null default now(),
  status text not null default 'complete',
  input_files jsonb not null default '[]'::jsonb,
  summary jsonb not null default '{}'::jsonb,
  reports jsonb not null default '[]'::jsonb,
  courier_summary_path text,
  created_by text
);

create index if not exists courier_analysis_runs_created_at_idx
  on courier_analysis_runs (created_at desc);

insert into storage.buckets (id, name, public)
values ('courier-analysis', 'courier-analysis', false)
on conflict (id) do update set public = false;
