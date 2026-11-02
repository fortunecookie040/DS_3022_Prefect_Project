-- One row per repo: every metric Task 5 can score.
-- Every column is a rate, share, median, or slope -- never a raw count -- so large
-- projects don't win on volume. PR-based metrics use human-authored PRs only
-- (bot PRs such as dependabot are merged by automation and would distort them).
with run as (
    select
        {{ run_time() }} as run_time,
        date_diff('second', {{ window_start() }}, {{ run_time() }}) / 86400.0 as window_days
),

repos as (
    select distinct repo from {{ ref('stg_pull_requests') }}
),

commit_stats as (
    select repo, count(*) as n_commits
    from {{ ref('stg_commits') }}
    where committed_at >= {{ window_start() }}
    group by repo
),

-- trends use complete months only (the run month is partial)
monthly as (
    select
        repo,
        regr_slope(commits, month_index)              as commit_trend,
        avg(commits)                                  as avg_monthly_commits,
        avg(active_contributors)                      as active_contributors_per_month,
        regr_slope(active_contributors, month_index)  as contributor_trend
    from {{ ref('mart_repo_monthly') }}
    where not is_partial_month
    group by repo
),

pr_stats as (
    select
        repo,
        median(hours_to_first_review)                                         as median_hours_to_first_review,
        avg(reviewed_within_7d::int)
            filter (where created_at <= run.run_time - interval 7 day)        as pct_reviewed_within_7d,
        avg(has_approving_review::int) filter (where is_merged)               as pct_merged_with_approval,
        count(*) filter (where is_merged)
            / nullif(count(*) filter (where is_merged or is_closed_unmerged), 0) as merge_rate,
        median(hours_to_merge)                                                as median_hours_to_merge,
        avg(is_stale::int) filter (where is_open)                             as pct_open_stale
    from {{ ref('fct_pull_requests') }}
    cross join run
    where not author_is_bot
    group by repo
),

release_stats as (
    select
        repo,
        count(*)                                                         as n_releases,
        stddev_samp(days_since_previous) / nullif(avg(days_since_previous), 0) as release_gap_cv
    from {{ ref('fct_releases') }}
    group by repo
),

-- bus factor: fewest authors who together account for >= 50% of merged human PRs
author_merges as (
    select repo, author_login, count(*) as n
    from {{ ref('fct_pull_requests') }}
    where is_merged and not author_is_bot and author_login is not null
    group by all
),

author_cumulative as (
    select
        repo,
        sum(n) over (partition by repo order by n desc, author_login
                     rows between unbounded preceding and current row) - n as merged_before,
        sum(n) over (partition by repo)                                    as total_merged
    from author_merges
),

bus as (
    select repo, count(*) as bus_factor
    from author_cumulative
    where merged_before < 0.5 * total_merged
    group by repo
)

select
    r.repo,
    -- Activity
    coalesce(cs.n_commits, 0) / (run.window_days / 7)          as commits_per_week,
    m.commit_trend,                                             -- commits/month change per month
    m.commit_trend / nullif(m.avg_monthly_commits, 0)           as commit_trend_pct,  -- size-normalized slope
    -- Responsiveness
    p.median_hours_to_first_review,
    p.pct_reviewed_within_7d,
    p.pct_merged_with_approval,
    -- Throughput
    p.merge_rate,
    p.median_hours_to_merge,
    -- Backlog
    p.pct_open_stale,
    -- Release cadence
    coalesce(rs.n_releases, 0) / (run.window_days / 365.25)    as releases_per_year,
    rs.release_gap_cv,
    -- Contributor base
    m.active_contributors_per_month,
    m.contributor_trend,
    b.bus_factor
from repos r
cross join run
left join commit_stats  cs using (repo)
left join monthly       m  using (repo)
left join pr_stats      p  using (repo)
left join release_stats rs using (repo)
left join bus           b  using (repo)
order by r.repo
