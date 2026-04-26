create extension if not exists pgcrypto;

create table if not exists public.grid_sync_runs (
  id uuid primary key default gen_random_uuid(),
  status text not null default 'running' check (status in ('running', 'success', 'failed')),
  requested_by text,
  started_at timestamptz not null default now(),
  finished_at timestamptz,
  vendor_count integer not null default 0,
  product_count integer not null default 0,
  error_message text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists public.grid_sync_state (
  state_key text primary key,
  next_offset integer not null default 0,
  last_total integer not null default 0,
  last_started_at timestamptz,
  last_finished_at timestamptz,
  updated_at timestamptz not null default now()
);

create table if not exists public.grid_innovators (
  id uuid primary key default gen_random_uuid(),
  portal_vendor_id text not null unique,
  vendor_name text not null,
  about_vendor text,
  website_details text,
  location_text text,
  city text,
  state text,
  country text,
  district text,
  pin_code text,
  agro_ecological_zone text,
  service_locations text[] not null default '{}',
  tags text[] not null default '{}',
  portal_vendor_link text,
  portal_contact_name text,
  portal_email text,
  portal_phone text,
  website_email text,
  website_phone text,
  website_address text,
  final_contact_email text,
  final_contact_phone text,
  final_contact_address text,
  contact_source_url text,
  website_status text,
  legacy_products_links text,
  contact_notes text,
  innovator_image_urls jsonb not null default '[]'::jsonb,
  innovator_media_urls jsonb not null default '[]'::jsonb,
  latitude double precision,
  longitude double precision,
  products_count integer not null default 0,
  search_text text,
  raw_vendor jsonb not null default '{}'::jsonb,
  synced_at timestamptz not null default now(),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists public.grid_practices (
  id uuid primary key default gen_random_uuid(),
  portal_product_id text not null unique,
  portal_vendor_id text not null references public.grid_innovators(portal_vendor_id) on delete cascade,
  vendor_name text not null,
  product_name text not null,
  product_description text,
  product_link text,
  product_image_url text,
  product_gallery_urls jsonb not null default '[]'::jsonb,
  product_video_urls jsonb not null default '[]'::jsonb,
  product_attachment_urls jsonb not null default '[]'::jsonb,
  product_location_text text,
  product_categories text[] not null default '{}',
  product_subcategories text[] not null default '{}',
  product_specifications jsonb not null default '[]'::jsonb,
  practice_summary text,
  innovator_details text,
  practice_details text,
  source_reference text,
  tags text[] not null default '{}',
  search_text text,
  raw_product jsonb not null default '{}'::jsonb,
  synced_at timestamptz not null default now(),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists grid_innovators_name_idx on public.grid_innovators (lower(vendor_name));
create index if not exists grid_innovators_tags_idx on public.grid_innovators using gin (tags);
create index if not exists grid_innovators_location_idx on public.grid_innovators (lower(coalesce(location_text, '')));
create index if not exists grid_practices_vendor_idx on public.grid_practices (portal_vendor_id);
create index if not exists grid_practices_name_idx on public.grid_practices (lower(product_name));
create index if not exists grid_practices_tags_idx on public.grid_practices using gin (tags);
create index if not exists grid_practices_location_idx on public.grid_practices (lower(coalesce(product_location_text, '')));

alter table public.grid_sync_runs enable row level security;
alter table public.grid_sync_state enable row level security;
alter table public.grid_innovators enable row level security;
alter table public.grid_practices enable row level security;

drop policy if exists "grid innovators are public" on public.grid_innovators;
drop policy if exists "grid practices are public" on public.grid_practices;

create policy "grid innovators are public"
on public.grid_innovators
for select
to anon, authenticated
using (true);

create policy "grid practices are public"
on public.grid_practices
for select
to anon, authenticated
using (true);
