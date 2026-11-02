-- One row per commit (latest landed copy per (repo, sha)).
with deduped as (
    select *
    from {{ source('raw', 'raw_commits') }}
    qualify row_number() over (partition by repo, id order by fetched_at desc) = 1
)

select
    repo || ':' || id                                        as commit_key,
    repo,
    id                                                       as sha,
    payload->'author'->>'login'                              as author_login,  -- null if email not linked to an account
    {{ is_bot("payload->'author'") }}                        as is_bot,
    (payload->'commit'->'author'->>'date')::timestamptz      as committed_at,
    fetched_at
from deduped
