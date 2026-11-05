"""GitHub GraphQL ingestion for pull requests and their reviews (extended item).

One GraphQL query returns a page of PRs (sorted by last update, newest first) with each
PR's reviews nested inside, replacing one REST `pulls` page plus one REST `reviews`
call per PR. Pagination follows `pageInfo.endCursor`; the point budget is tracked from
the `rateLimit` field that every query also asks for.

Landing files keep the UNMODIFIED GraphQL response. Task 2 calls to_rest_records() to
reshape each page into REST-style records (user.login, user.type, lowercase state,
merged_at, pull_request_url, ...) so raw_pull_requests / raw_pr_reviews and every dbt
model work the same no matter which API the data came from.
"""
import time
from datetime import datetime

import httpx

from api_client import GRAPHQL_LIMITER, ODOMETER

GRAPHQL_URL = "https://api.github.com/graphql"
PRS_PER_PAGE = 50          # 50 PRs x up to 100 reviews each stays well inside GitHub's
                           # query-size limits and costs ~1 point per page
REVIEWS_PER_PR = 100       # the GraphQL maximum; PRs with more are paged separately

_REVIEW_FIELDS = "databaseId state submittedAt author { __typename login }"

PULLS_QUERY = """
query($owner: String!, $name: String!, $first: Int!, $cursor: String) {
  rateLimit { cost remaining resetAt limit }
  repository(owner: $owner, name: $name) {
    pullRequests(first: $first, after: $cursor,
                 orderBy: {field: UPDATED_AT, direction: DESC}) {
      pageInfo { hasNextPage endCursor }
      nodes {
        number state createdAt updatedAt closedAt mergedAt
        author { __typename login }
        reviews(first: %d) {
          pageInfo { hasNextPage endCursor }
          nodes { %s }
        }
      }
    }
  }
}""" % (REVIEWS_PER_PR, _REVIEW_FIELDS)

# Follow-up query for the rare PR with more than REVIEWS_PER_PR reviews.
REVIEWS_QUERY = """
query($owner: String!, $name: String!, $number: Int!, $cursor: String) {
  rateLimit { cost remaining resetAt limit }
  repository(owner: $owner, name: $name) {
    pullRequest(number: $number) {
      reviews(first: %d, after: $cursor) {
        pageInfo { hasNextPage endCursor }
        nodes { %s }
      }
    }
  }
}""" % (REVIEWS_PER_PR, _REVIEW_FIELDS)


def _parse_time(iso):
    return datetime.fromisoformat(iso.replace("Z", "+00:00")).timestamp()


def graphql_post(query, variables, headers, repo, endpoint, logger=None, max_tries=5):
    """POST one GraphQL query and return the full response body.

    GraphQL can fail in two ways: an HTTP error (403/429 secondary limits, 502/504 when
    a query is too heavy) or HTTP 200 with an `errors` array (including RATE_LIMITED).
    Transient failures are retried here with backoff; anything else is raised with the
    repo, endpoint and cursor so the Prefect task's own retries take over.
    """
    tries = 0
    where = f"{repo} {endpoint} (variables {variables})"
    while True:
        GRAPHQL_LIMITER.wait(logger)
        ODOMETER.add("graphql", repo, endpoint)
        try:
            response = httpx.post(GRAPHQL_URL, json={"query": query, "variables": variables},
                                  headers=headers, timeout=90)
        except httpx.TransportError as e:
            tries += 1
            if tries >= max_tries:
                raise RuntimeError(f"{where}: network error after {tries} tries: {e}") from e
            time.sleep(2 ** tries)
            continue

        if response.status_code in (403, 429):
            reset = response.headers.get("X-RateLimit-Reset")
            if response.headers.get("Retry-After", "").isdigit():          # secondary limit
                wait = int(response.headers["Retry-After"])
            elif response.headers.get("X-RateLimit-Remaining") == "0" and reset:
                wait = max(1, int(reset) - int(time.time())) + 5
            else:
                raise RuntimeError(f"{where}: HTTP {response.status_code}: {response.text[:300]}")
            if logger:
                logger.warning(f"{where}: HTTP {response.status_code}, sleeping {wait}s")
            time.sleep(wait)
            continue

        if response.status_code >= 500:            # 502/504 = GitHub gave up on the query
            tries += 1
            if tries >= max_tries:
                raise RuntimeError(f"{where}: HTTP {response.status_code} after {tries} tries")
            if logger:
                logger.warning(f"{where}: HTTP {response.status_code}, retry {tries}")
            time.sleep(2 ** tries)
            continue

        if response.is_error:
            raise RuntimeError(f"{where}: HTTP {response.status_code}: {response.text[:300]}")

        try:
            body = response.json()
        except ValueError as e:
            raise RuntimeError(f"{where}: response is not valid JSON: {response.text[:300]}") from e

        rate = (body.get("data") or {}).get("rateLimit")
        if rate:
            GRAPHQL_LIMITER.update(rate["remaining"], _parse_time(rate["resetAt"]))
            ODOMETER.spend("graphql", repo, endpoint, rate["cost"])

        errors = body.get("errors")
        if errors:
            if any(e.get("type") == "RATE_LIMITED" for e in errors):
                reset = response.headers.get("X-RateLimit-Reset")
                wait = max(1, int(reset) - int(time.time())) + 5 if reset else 60
                if logger:
                    logger.warning(f"{where}: GraphQL RATE_LIMITED, sleeping {wait}s")
                time.sleep(wait)
                continue
            raise RuntimeError(f"{where}: GraphQL errors: {errors[:3]}")
        return body


# ---- reshape GraphQL nodes into REST-style records (used by Task 2 and Task 4) ----

def _rest_user(author):
    """GraphQL author -> REST `user` object.

    - A deleted account comes back as author = null; REST shows it as the "ghost" user.
    - GraphQL Bot logins lack the "[bot]" suffix REST uses ("dependabot" vs
      "dependabot[bot]"), so it is added back for the bot rule and for consistent logins.
    """
    if author is None:
        return {"login": "ghost", "type": "User"}
    login, kind = author["login"], author["__typename"]
    if kind == "Bot" and not login.endswith("[bot]"):
        login += "[bot]"
    return {"login": login, "type": kind}


def _rest_pull(pr):
    return {
        "number": pr["number"],
        "state": "open" if pr["state"] == "OPEN" else "closed",   # REST has no "merged" state
        "user": _rest_user(pr["author"]),
        "created_at": pr["createdAt"],
        "updated_at": pr["updatedAt"],
        "closed_at": pr["closedAt"],
        "merged_at": pr["mergedAt"],
        "graphql_state": pr["state"],
        "_source": "graphql",
    }


def _rest_review(repo, pr_number, review):
    return {
        "id": review["databaseId"],
        "user": _rest_user(review["author"]),
        "state": review["state"],
        "submitted_at": review["submittedAt"],
        "pull_request_url": f"https://api.github.com/repos/{repo}/pulls/{pr_number}",
        "_source": "graphql",
    }


def to_rest_records(saved):
    """A GraphQL landing page -> {"pulls": [...], "pr_reviews": [...]} of REST-style records."""
    repo, data = saved["repo"], saved["data"]["data"]
    if saved["endpoint"] == "graphql_pulls":
        pulls, reviews = [], []
        for pr in data["repository"]["pullRequests"]["nodes"]:
            pulls.append(_rest_pull(pr))
            reviews += [_rest_review(repo, pr["number"], r) for r in pr["reviews"]["nodes"]]
        return {"pulls": pulls, "pr_reviews": reviews}
    if saved["endpoint"] == "graphql_reviews":
        number = saved["pr_number"]
        nodes = data["repository"]["pullRequest"]["reviews"]["nodes"]
        return {"pr_reviews": [_rest_review(repo, number, r) for r in nodes]}
    raise ValueError(f"Not a GraphQL landing page: endpoint {saved['endpoint']!r}")
