alter table public.grid_practices
  add column if not exists ai_model text,
  add column if not exists ai_summary jsonb not null default '{}'::jsonb,
  add column if not exists ai_classified_at timestamptz,
  add column if not exists ai_source_hash text;
