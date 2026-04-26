# GRID Innovation Directory

Standalone GRID practice directory with the Selco-style public search flow, Supabase-backed storage, and a GIAN-style admin refresh path.

Project folder:
`C:\github\grid-innovation-directory`

Included app surfaces:
- Public search page: `index.html`
- Innovator detail page: `vendor-detail.html`
- Practice detail page: `product-detail.html`
- Admin-triggered sync page: `admin.html`
- Shared Supabase loader: `innovation-store.js`
- Supabase migration: `supabase/migrations/20260426003000_create_grid_innovation_directory.sql`
- Supabase edge function: `supabase/functions/grid-innovation-admin/index.ts`

Implementation notes:
- GRID practices are normalized into vendor-style `grid_innovators` rows and product-style `grid_practices` rows so the app stays parallel with Selco and GIAN.
- The edge function scrapes GRID list pages plus detail pages, captures practice content, innovator fields, linked references, image URLs, attachment URLs, video URLs, and map coordinates, then upserts them into Supabase.
- Admin sync behavior mirrors the GIAN project pattern: login, run manual refresh, inspect recent sync runs, and edit innovator-side fields from the admin panel.
- Public search uses dropdowns for `Innovator`, `Practice Name`, `Location`, and `Tags`, plus a free-text keyword search box.
- Because GRID has a large scrape surface and strict TLS/domain quirks, the reliable bulk-refresh path is the local importer script: `scripts/grid-local-import.mjs`, using direct Supabase upserts with a service-role key.

Deployment:
- GitHub Pages deploys automatically from `.github/workflows/deploy-pages.yml`
- The static frontend uses the configured Supabase URL and anon key in `config.js`
- Add a `MAPMYINDIA_MAP_KEY` in `config.js` to enable the live map

Backend requirement:
- The `grid-innovation-admin` edge function reads `SUPABASE_SERVICE_ROLE_KEY` or falls back to `SELCO_VENDOR_SERVICE_ROLE_KEY`
- Run the new migration, deploy the edge function, and update `config.js` if you want to point this project at a different Supabase instance
