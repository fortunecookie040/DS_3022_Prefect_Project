-- Fails if any review was submitted before its PR was created.
select r.review_key, r.pr_key, p.created_at, r.submitted_at
from {{ ref('stg_pr_reviews') }} r
join {{ ref('stg_pull_requests') }} p on p.pr_key = r.pr_key
where r.submitted_at < p.created_at
