-- One row per PR created on or after the window start.
-- Open PRs are kept and flagged is_open; their merge/close durations stay NULL (never a fake close date).
with run as (
    select {{ run_time() }} as run_time
),

prs as (
    select *
    from {{ ref('stg_pull_requests') }}
    where created_at >= {{ window_start() }}
),

-- reviews that count: not by the PR's own author, not by a bot
human_reviews as (
    select r.pr_key, r.submitted_at, r.state
    from {{ ref('stg_pr_reviews') }} r
    join prs p on p.pr_key = r.pr_key
    where not r.is_bot
      and r.reviewer_login is distinct from p.author_login
),

review_summary as (
    select
        pr_key,
        min(submitted_at)            as first_review_at,
        bool_or(state = 'APPROVED')  as has_approving_review
    from human_reviews
    group by pr_key
),

flags as (
    select
        p.pr_key,
        p.repo,
        p.pr_number,
        p.author_login,
        p.is_bot                                       as author_is_bot,
        p.state,
        p.created_at,
        p.updated_at,
        p.closed_at,
        p.merged_at,
        p.merged_at is not null                        as is_merged,
        p.state = 'closed' and p.merged_at is null     as is_closed_unmerged,
        p.state = 'open'                               as is_open,
        rs.first_review_at,
        {{ hours_between('p.created_at', 'rs.first_review_at') }}  as hours_to_first_review,
        {{ hours_between('p.created_at', 'p.merged_at') }}         as hours_to_merge,
        {{ hours_between('p.created_at', 'p.closed_at') }}         as hours_to_close,
        p.state = 'open' and p.updated_at < run.run_time - interval 180 day  as is_stale,
        coalesce(rs.has_approving_review, false)       as has_approving_review
    from prs p
    left join review_summary rs on rs.pr_key = p.pr_key
    cross join run
)

select
    *,
    coalesce(hours_to_first_review <= 168, false) as reviewed_within_7d
from flags
