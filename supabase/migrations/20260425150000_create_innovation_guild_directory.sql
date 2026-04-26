create extension if not exists pgcrypto;

create table if not exists public.innovation_guild_sync_runs (
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

create table if not exists public.innovation_guild_vendors (
  id uuid primary key default gen_random_uuid(),
  portal_vendor_id text not null unique,
  vendor_name text not null,
  about_vendor text,
  website_details text,
  location_text text,
  city text,
  state text,
  country text,
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
  latitude double precision,
  longitude double precision,
  products_count integer not null default 0,
  search_text text,
  raw_vendor jsonb not null default '{}'::jsonb,
  synced_at timestamptz not null default now(),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists public.innovation_guild_products (
  id uuid primary key default gen_random_uuid(),
  portal_product_id text not null unique,
  portal_vendor_id text not null references public.innovation_guild_vendors(portal_vendor_id) on delete cascade,
  vendor_name text not null,
  product_name text not null,
  product_description text,
  product_link text,
  product_image_url text,
  product_gallery_urls jsonb not null default '[]'::jsonb,
  product_video_urls jsonb not null default '[]'::jsonb,
  product_location_text text,
  product_categories text[] not null default '{}',
  product_subcategories text[] not null default '{}',
  product_specifications jsonb not null default '[]'::jsonb,
  tags text[] not null default '{}',
  search_text text,
  raw_product jsonb not null default '{}'::jsonb,
  synced_at timestamptz not null default now(),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists innovation_guild_vendors_name_idx on public.innovation_guild_vendors (lower(vendor_name));
create index if not exists innovation_guild_vendors_tags_idx on public.innovation_guild_vendors using gin (tags);
create index if not exists innovation_guild_products_vendor_idx on public.innovation_guild_products (portal_vendor_id);
create index if not exists innovation_guild_products_name_idx on public.innovation_guild_products (lower(product_name));
create index if not exists innovation_guild_products_tags_idx on public.innovation_guild_products using gin (tags);

alter table public.innovation_guild_sync_runs enable row level security;
alter table public.innovation_guild_vendors enable row level security;
alter table public.innovation_guild_products enable row level security;

drop policy if exists "innovation guild vendors are public" on public.innovation_guild_vendors;
drop policy if exists "innovation guild products are public" on public.innovation_guild_products;

create policy "innovation guild vendors are public"
on public.innovation_guild_vendors
for select
to anon, authenticated
using (true);

create policy "innovation guild products are public"
on public.innovation_guild_products
for select
to anon, authenticated
using (true);
