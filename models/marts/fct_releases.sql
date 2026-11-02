-- One row per published release on or after the window start.
-- lag() runs BEFORE the window filter so the first release in the window
-- still measures its gap to the release before it.
with with_gap as (
    select
        *,
        date_diff('day',
                  lag(published_at) over (partition by repo order by published_at),
                  published_at) as days_since_previous
    from {{ ref('stg_releases') }}
)

select
    release_key,
    repo,
    release_id,
    tag_name,
    published_at,
    is_prerelease,
    days_since_previous
from with_gap
where published_at >= {{ window_start() }}
