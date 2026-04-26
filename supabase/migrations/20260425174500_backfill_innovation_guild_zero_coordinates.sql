update public.innovation_guild_vendors
set
  latitude = 13.0108677,
  longitude = 77.5207301,
  updated_at = now()
where portal_vendor_id in (
  'UNG_A1772518016839',
  'UNG_A1772532462841',
  'UNG_A1772531045640'
);

update public.innovation_guild_vendors
set
  latitude = 21.1498134,
  longitude = 79.0820556,
  updated_at = now()
where portal_vendor_id = 'BIOWA1754123610379';

update public.innovation_guild_vendors
set
  latitude = 23.0215374,
  longitude = 72.5800568,
  updated_at = now()
where portal_vendor_id = 'ORANG1751702260147';
