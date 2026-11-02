-- Repo x month, from the window start through the run month.
-- The month spine guarantees months with no activity show 0 instead of disappearing.
with run as (
    select {{ run_time() }} as run_time
),

months as (
    select cast(unnest(generate_series(
               timestamp '{{ var("window_start") }}',
               date_trunc('month', run_time)::timestamp,
               interval 1 month)) as date) as month_start
    from run
),

repos as (
    select distinct repo from {{ ref('stg_pull_requests') }}
),

spine as (
    select r.repo, m.month_start
    from repos r cross join months m
),

prs as (
    select * from {{ ref('fct_pull_requests') }}
),

opened as (
    select
        repo,
        date_trunc('month', created_at)::date                   as month_start,
        count(*)                                                as prs_opened,
        median(hours_to_first_review)                           as median_first_review_hours,
        count(distinct author_login) filter (where not author_is_bot) as active_contributors
    from prs
    group by all
),

merged as (
    select
        repo,
        date_trunc('month', merged_at)::date  as month_start,
        count(*)                              as prs_merged,
        median(hours_to_merge)                as median_merge_hours
    from prs
    where is_merged
    group by all
),

closed_unmerged as (
    select
        repo,
        date_trunc('month', closed_at)::date  as month_start,
        count(*)                              as prs_closed_unmerged
    from prs
    where is_closed_unmerged
    group by all
),

commits as (
    select
        repo,
        date_trunc('month', committed_at)::date as month_start,
        count(*)                                as commits
    from {{ ref('stg_commits') }}
    group by all
)

select
    s.repo || ':' || strftime(s.month_start, '%Y-%m')               as repo_month,
    s.repo,
    s.month_start,
    date_diff('month', date '{{ var("window_start") }}', s.month_start) as month_index,
    coalesce(o.prs_opened, 0)                                       as prs_opened,
    coalesce(m.prs_merged, 0)                                       as prs_merged,
    coalesce(c.prs_closed_unmerged, 0)                              as prs_closed_unmerged,
    coalesce(m.prs_merged, 0)
        / nullif(coalesce(m.prs_merged, 0) + coalesce(c.prs_closed_unmerged, 0), 0) as merge_ratio,
    o.median_first_review_hours,
    m.median_merge_hours,
    coalesce(cm.commits, 0)                                         as commits,
    coalesce(o.active_contributors, 0)                              as active_contributors,
    s.month_start = date_trunc('month', run.run_time)::date         as is_partial_month
from spine s
cross join run
left join opened          o  using (repo, month_start)
left join merged          m  using (repo, month_start)
left join closed_unmerged c  using (repo, month_start)
left join commits         cm using (repo, month_start)
order by s.repo, s.month_start
