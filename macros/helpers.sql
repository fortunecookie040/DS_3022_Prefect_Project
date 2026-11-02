{#
  Bot rule (applied to PR authors, reviewers and commit authors):
    is_bot = user.type = 'Bot' OR user.login LIKE '%[bot]'
  A null user (e.g. commit email not linked to a GitHub account, or a deleted
  "ghost" user) is treated as NOT a bot.
#}
{% macro is_bot(user_json) -%}
    coalesce(({{ user_json }}->>'type') = 'Bot' or ({{ user_json }}->>'login') like '%[bot]', false)
{%- endmacro %}

{#
  "Run time" = newest fetched_at across all raw tables, so rebuilding without
  new data gives identical results.
#}
{% macro run_time() -%}
    (select max(fetched_at)::timestamptz from (
        select fetched_at from {{ source('raw', 'raw_commits') }}
        union all select fetched_at from {{ source('raw', 'raw_pull_requests') }}
        union all select fetched_at from {{ source('raw', 'raw_pr_reviews') }}
        union all select fetched_at from {{ source('raw', 'raw_releases') }}
    ))
{%- endmacro %}

{% macro window_start() -%}
    timestamptz '{{ var("window_start") }} 00:00:00+00'
{%- endmacro %}

{# hours between two timestamps #}
{% macro hours_between(start_ts, end_ts) -%}
    date_diff('second', {{ start_ts }}, {{ end_ts }}) / 3600.0
{%- endmacro %}
