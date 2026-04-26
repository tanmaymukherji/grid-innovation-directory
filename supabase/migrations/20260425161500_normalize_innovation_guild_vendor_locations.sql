create or replace function public.innovation_guild_clean_location_value(value text)
returns text
language sql
immutable
as $$
  select nullif(
    regexp_replace(
      regexp_replace(
        regexp_replace(trim(coalesce(value, '')), '\s+', ' ', 'g'),
        '\s*,\s*', ', ', 'g'
      ),
      '\s*\|\s*', ' | ', 'g'
    ),
    ''
  );
$$;

create or replace function public.innovation_guild_normalize_service_locations(
  location_values text[],
  primary_address text,
  state text,
  country text
)
returns text[]
language sql
immutable
as $$
  with refs as (
    select
      public.innovation_guild_clean_location_value(primary_address) as address_value,
      public.innovation_guild_clean_location_value(concat_ws(', ', state, country)) as state_country_value
  ),
  cleaned as (
    select distinct on (lower(value))
      value,
      lower(value) as key
    from (
      select public.innovation_guild_clean_location_value(entry) as value
      from unnest(coalesce(location_values, '{}'::text[])) as entry
    ) normalized
    where value is not null
    order by lower(value), value
  )
  select coalesce(array_agg(cleaned.value order by cleaned.value), '{}'::text[])
  from cleaned
  cross join refs
  where (refs.address_value is null or cleaned.key <> lower(refs.address_value))
    and (
      refs.state_country_value is null
      or cleaned.key <> lower(refs.state_country_value)
      or refs.address_value is null
      or position(lower(refs.state_country_value) in lower(refs.address_value)) = 0
    );
$$;

with normalized as (
  select
    portal_vendor_id,
    public.innovation_guild_clean_location_value(
      coalesce(final_contact_address, split_part(coalesce(location_text, ''), ' | ', 1))
    ) as cleaned_address,
    public.innovation_guild_normalize_service_locations(
      service_locations,
      coalesce(final_contact_address, split_part(coalesce(location_text, ''), ' | ', 1)),
      state,
      country
    ) as cleaned_service_locations
  from public.innovation_guild_vendors
)
update public.innovation_guild_vendors as vendors
set
  final_contact_address = normalized.cleaned_address,
  service_locations = normalized.cleaned_service_locations,
  location_text = nullif(
    array_to_string(
      array_remove(
        array_prepend(normalized.cleaned_address, normalized.cleaned_service_locations),
        null
      ),
      ' | '
    ),
    ''
  ),
  updated_at = now()
from normalized
where vendors.portal_vendor_id = normalized.portal_vendor_id;
