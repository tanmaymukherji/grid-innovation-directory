with normalized as (
  select
    portal_vendor_id,
    public.innovation_guild_clean_location_value(final_contact_address) as cleaned_address,
    coalesce(
      array_agg(entry) filter (
        where entry is not null
          and (
            public.innovation_guild_clean_location_value(final_contact_address) is null
            or lower(public.innovation_guild_clean_location_value(final_contact_address)) <> lower(entry)
          )
          and (
            public.innovation_guild_clean_location_value(final_contact_address) is null
            or lower(public.innovation_guild_clean_location_value(final_contact_address)) not like '%'
              || lower(entry)
          )
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
  group by portal_vendor_id, public.innovation_guild_clean_location_value(final_contact_address)
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
