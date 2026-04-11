alter table if exists users enable row level security;
alter table if exists scrape_jobs enable row level security;
alter table if exists leads enable row level security;

alter table if exists scrape_jobs add column if not exists worker_type text;
alter table if exists scrape_jobs add column if not exists worker_id text;
alter table if exists scrape_jobs add column if not exists website_filter text;
alter table if exists scrape_jobs add column if not exists max_per_query integer;
alter table if exists scrape_jobs add column if not exists niche text;
alter table if exists scrape_jobs add column if not exists progress_message text;
alter table if exists scrape_jobs add column if not exists current_query text;
alter table if exists scrape_jobs add column if not exists total_areas integer default 0;
alter table if exists scrape_jobs add column if not exists processed_areas integer default 0;
alter table if exists scrape_jobs add column if not exists progress_marker jsonb default '{"version":1,"items":{},"current_item":null}'::jsonb;
alter table if exists scrape_jobs add column if not exists recent_events jsonb default '[]'::jsonb;
alter table if exists scrape_jobs add column if not exists root_job_id text;
alter table if exists scrape_jobs add column if not exists resumed_from_job_id text;
alter table if exists scrape_jobs add column if not exists claimed_at timestamptz;
alter table if exists scrape_jobs add column if not exists started_at timestamptz;
alter table if exists scrape_jobs add column if not exists finished_at timestamptz;
alter table if exists scrape_jobs add column if not exists error_message text;
alter table if exists scrape_jobs add column if not exists cancel_requested boolean default false;
create index if not exists idx_scrape_jobs_worker_status on scrape_jobs(worker_type, status, created_at);

create policy if not exists "users_select_self"
on users for select
using (auth.uid() = id);

create policy if not exists "jobs_select_self"
on scrape_jobs for select
using (auth.uid() = user_id);

create policy if not exists "jobs_insert_self"
on scrape_jobs for insert
with check (auth.uid() = user_id);

create policy if not exists "jobs_update_self"
on scrape_jobs for update
using (auth.uid() = user_id);

create policy if not exists "leads_select_self"
on leads for select
using (auth.uid() = user_id);

create policy if not exists "leads_insert_self"
on leads for insert
with check (auth.uid() = user_id);
