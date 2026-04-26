create or replace function public.innovation_guild_address_contains_location(
  address_value text,
  candidate_value text
)
returns boolean
language sql
immutable
as $$
  with normalized as (
    select
      lower(public.innovation_guild_clean_location_value(address_value)) as address_text,
      lower(public.innovation_guild_clean_location_value(candidate_value)) as candidate_text
  ),
  direct_match as (
    select
      address_text,
      candidate_text,
      (
        address_text is not null
        and candidate_text is not null
        and (
          address_text = candidate_text
          or address_text like '% ' || candidate_text
          or address_text like '%,' || candidate_text
          or address_text like '%,' || ' ' || candidate_text
          or position(candidate_text in address_text) > 0
        )
      ) as matched
    from normalized
  ),
  token_match as (
    select coalesce(bool_and(position(trim(part) in direct_match.address_text) > 0), false) as matched
    from direct_match,
    lateral regexp_split_to_table(coalesce(direct_match.candidate_text, ''), '\s*,\s*') as part
    where trim(part) <> ''
  )
  select
    coalesce((select matched from direct_match), false)
    or coalesce((select matched from token_match), false);
$$;

with normalized as (
  select
    portal_vendor_id,
    public.innovation_guild_clean_location_value(final_contact_address) as cleaned_address,
    coalesce(
      array_agg(entry) filter (
        where entry is not null
          and not public.innovation_guild_address_contains_location(final_contact_address, entry)
      ),
      '{}'::text[]
    ) as cleaned_service_locations
  from public.innovation_guild_vendors
  cross join lateral (
    select distinct on (lower(cleaned_entry))
      cleaned_entry as entry
    from (
      select public.innovation_guild_clean_location_value(item) as cleaned_entry
      from unnest(coalesce(service_locations, '{}'::text[])) as item
    ) deduped
    where cleaned_entry is not null
    order by lower(cleaned_entry), cleaned_entry
  ) entries
  group by portal_vendor_id, public.innovation_guild_clean_location_value(final_contact_address), final_contact_address
)
update public.innovation_guild_vendors as vendors
set
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
