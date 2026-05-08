alter table public.grid_practices
  add column if not exists six_m_categories text[] not null default '{}',
  add column if not exists reviewed_tags text[] not null default '{}',
  add column if not exists admin_notes text;

create index if not exists grid_practices_sixm_idx
  on public.grid_practices using gin (six_m_categories);

create index if not exists grid_practices_reviewed_tags_idx
  on public.grid_practices using gin (reviewed_tags);
