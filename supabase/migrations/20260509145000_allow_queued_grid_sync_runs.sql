alter table public.grid_sync_runs
  drop constraint if exists grid_sync_runs_status_check;

alter table public.grid_sync_runs
  add constraint grid_sync_runs_status_check
  check (status in ('queued', 'running', 'success', 'failed'));
