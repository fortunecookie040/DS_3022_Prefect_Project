-- One row per submitted review. PENDING reviews are dropped: they have no submitted_at.
with deduped as (
    select *
    from {{ source('raw', 'raw_pr_reviews') }}
    qualify row_number() over (partition by repo, id order by fetched_at desc) = 1
),

parsed as (
    select
        *,
        -- pull_request_url ends in /pulls/<number>
        regexp_extract(payload->>'pull_request_url', '([0-9]+)$', 1)::integer as pr_number
    from deduped
    where payload->>'state' <> 'PENDING'
)

select
    repo || ':' || id                          as review_key,
    repo,
    id::bigint                                 as review_id,
    pr_number,
    repo || '#' || pr_number                   as pr_key,
    payload->'user'->>'login'                  as reviewer_login,
    {{ is_bot("payload->'user'") }}            as is_bot,
    payload->>'state'                          as state,
    (payload->>'submitted_at')::timestamptz    as submitted_at,
    fetched_at
from parsed
