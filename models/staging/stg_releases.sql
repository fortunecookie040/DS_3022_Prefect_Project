-- One row per published release. Drafts are dropped (never published).
with deduped as (
    select *
    from {{ source('raw', 'raw_releases') }}
    qualify row_number() over (partition by repo, id order by fetched_at desc) = 1
)

select
    repo || ':' || id                          as release_key,
    repo,
    id::bigint                                 as release_id,
    payload->>'tag_name'                       as tag_name,
    (payload->>'published_at')::timestamptz    as published_at,
    (payload->>'prerelease')::boolean          as is_prerelease,
    fetched_at
from deduped
where not (payload->>'draft')::boolean
