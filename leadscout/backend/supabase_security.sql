alter table if exists users enable row level security;
alter table if exists scrape_jobs enable row level security;
alter table if exists leads enable row level security;

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
