# put your prefect flow here (6 core tasks)
# please give your main @flow a name identifiable with your computing ID, such as the example below
# this will help instructional staff find and review your logs in the prefect server.

from prefect import flow, task, get_run_logger, unmapped
from prefect.cache_policies import NO_CACHE
from prefect.futures import wait
from prefect.runtime import flow_run
from pathlib import Path
from contextlib import contextmanager
from typing import Literal
import json, os, time, duckdb, logging, shutil, functools
from datetime import datetime, timezone
from dbt.cli.main import dbtRunner

from api_client import ODOMETER, rest_get
from graphql_client import (GRAPHQL_URL, PRS_PER_PAGE, PULLS_QUERY, REVIEWS_QUERY,
                            graphql_post, to_rest_records)
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")                # render PNGs straight to file; no display window needed
import matplotlib.pyplot as plt


START_DATE = "2024-01-01T00:00:00Z"
PROJECT_DIR = Path(__file__).resolve().parent   # the flow chdirs here so every relative path
                                                # (incl. dbt's packages.duckdb) works anywhere
RAW_DIR = Path("data/raw")
DATABASE = Path("packages.duckdb")
LOG_DIR = Path("logs")
DBT_DIR = PROJECT_DIR                # dbt_project.yml + profiles.yml live in the repo root
API_BASE = "https://api.github.com"
REPOS = [
    "scikit-learn/scikit-learn",
    "dask/dask",
    "plotly/plotly.py",
]

# landing-folder endpoint name -> raw table in DuckDB
ENDPOINT_TABLES = {
    "commits": "raw_commits",
    "pulls": "raw_pull_requests",
    "pr_reviews": "raw_pr_reviews",
    "releases": "raw_releases",
}

# GraphQL landing folders; Task 2 reshapes their pages into the REST-style tables above.
GRAPHQL_ENDPOINTS = {"graphql_pulls", "graphql_reviews"}

# Where the PAT comes from: the TOKEN env var locally, else this Prefect Secret block
# (used by the scheduled deployment, whose worker pod has no TOKEN variable).
SECRET_BLOCK = os.environ.get("GH_SECRET_BLOCK", "qtr7bs-pat")

# ---- Task 4: validation settings ----
# raw table -> (staging model, SQL condition for raw rows staging drops on purpose)
STAGING_MODELS = {
    "raw_commits":       ("stg_commits", None),
    "raw_pull_requests": ("stg_pull_requests", None),
    "raw_pr_reviews":    ("stg_pr_reviews", "payload->>'state' = 'PENDING'"),     # no submitted_at
    "raw_releases":      ("stg_releases", "(payload->>'draft')::boolean"),        # never published
}
MERGE_OUTLIER_HOURS = 365 * 24      # a merged PR that took over a year from open to merge
WEEKLY_COMMIT_IQR_K = 3             # Tukey "far out" fence: Q3 + 3 * IQR of a repo's weekly commits

# ---- Task 5: health model ----
# Every metric is a column of mart_health_inputs (rates, shares, medians, slopes; never raw counts).
# direction: +1 = higher is healthier, -1 = lower is healthier.
HEALTH_METRICS = [
    {"column": "commit_trend_pct", "dimension": "activity", "direction": +1,
     "definition": "Slope of monthly commits over complete months / average monthly commits "
                   "(fractional change per month; the trend component)"},
    {"column": "median_hours_to_first_review", "dimension": "responsiveness", "direction": -1,
     "definition": "Median hours from PR open to first review by someone other than the author or a bot"},
    {"column": "pct_reviewed_within_7d", "dimension": "responsiveness", "direction": +1,
     "definition": "Share of human PRs (opened 7+ days before run time) reviewed within 168 hours"},
    {"column": "merge_rate", "dimension": "throughput", "direction": +1,
     "definition": "Merged / (merged + closed unmerged) human PRs"},
    {"column": "median_hours_to_merge", "dimension": "throughput", "direction": -1,
     "definition": "Median hours from PR open to merge (merged human PRs only)"},
    {"column": "pct_open_stale", "dimension": "backlog", "direction": -1,
     "definition": "Share of open human PRs with no update in 180+ days"},
    {"column": "releases_per_year", "dimension": "release_cadence", "direction": +1,
     "definition": "Published releases in the window / (window days / 365.25)"},
    {"column": "release_gap_cv", "dimension": "release_cadence", "direction": -1,
     "definition": "Coefficient of variation of days between releases (lower = more regular)"},
    {"column": "top3_merge_share", "dimension": "contributor_base", "direction": -1,
     "definition": "Share of merged human PRs authored by the 3 most prolific authors "
                   "(concentration; a share, so it doesn't grow with project size)"},
    {"column": "contributor_trend_pct", "dimension": "contributor_base", "direction": +1,
     "definition": "Slope of monthly active PR authors / average monthly active PR authors "
                   "(fractional change per month; a second trend component)"},
]
# Weights sum to 1. Responsiveness and the contributor base are the signals ASSESSING-HEALTH.md
# ranks first and second; throughput and backlog show whether contributions actually land;
# commit activity and release cadence get the least weight because they depend on each
# project's habits (commit size, release policy) more than on community health.
DIMENSION_WEIGHTS = {
    "responsiveness":   0.25,
    "contributor_base": 0.20,
    "throughput":       0.20,
    "backlog":          0.15,
    "activity":         0.10,
    "release_cadence":  0.10,
}

# ---- Task 6: plots ----
PLOTS_DIR = Path("plots")
# Supplementary bubble chart: only data before this date (dask's maintainers stopped on
# 2026-09-29, so from October on the three repos are no longer comparable side by side).
COMPARISON_CUTOFF = "2026-10-01"
REPO_COLORS = {                     # Okabe-Ito colorblind-safe palette, same color in every plot
    "scikit-learn/scikit-learn": "#0072B2",
    "dask/dask":                 "#E69F00",
    "plotly/plotly.py":          "#009E73",
}

# Helper functions go here
@contextmanager
def task_log_file(name):
    """Send this task's Prefect logs to logs/<name>.log as well as the Prefect UI.

    The handler is removed on exit so the next task's messages don't leak in here.
    """
    logger = get_run_logger()
    LOG_DIR.mkdir(exist_ok=True)
    handler = logging.FileHandler(LOG_DIR / f"{name}.log")
    handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
    logger.logger.addHandler(handler)
    try:
        yield logger
    finally:
        logger.logger.removeHandler(handler)
        handler.close()


def log_to_file(name):
    """Run the whole task inside task_log_file(name) without re-indenting its body."""
    def decorator(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            with task_log_file(name):
                return fn(*args, **kwargs)
        return wrapper
    return decorator


def read_watermarks():
    """Newest updated_at per (repo, endpoint) from _load_state; {} on the first run."""
    if not DATABASE.exists():
        return {}
    con = duckdb.connect(str(DATABASE), read_only=True)
    try:
        rows = con.execute("SELECT repo, endpoint, newest_updated_at FROM _load_state").fetchall()
    except duckdb.CatalogException:          # database exists but Task 2 never finished
        return {}
    finally:
        con.close()
    return {(repo, endpoint): ts.strftime("%Y-%m-%dT%H:%M:%SZ")
            for repo, endpoint, ts in rows if ts is not None}


def get_record_id(endpoint, record):
    """Natural key: commits -> sha, PRs -> number, reviews and releases -> id."""
    key = {"commits": "sha", "pulls": "number"}.get(endpoint, "id")
    return str(record[key])


def get_updated_at(endpoint, record):
    """The record's 'last changed' time; feeds the _load_state watermark.

    Only PRs have a real updated_at, so the closest equivalent is used elsewhere.
    """
    if endpoint == "commits":
        return record["commit"]["committer"]["date"]    # what GitHub's `since` filters on
    if endpoint == "pulls":
        return record["updated_at"]
    if endpoint == "pr_reviews":
        return record.get("submitted_at")               # None for PENDING reviews
    return record.get("published_at") or record.get("created_at")   # releases

def close_dbt_duckdb():
    """Close the DuckDB connection dbt-duckdb keeps open after `dbt build`.

    dbt-duckdb caches it on DuckDBConnectionManager._ENV and never closes it, so it
    survives until interpreter shutdown, where duckdb's nanobind bindings print
    "nanobind: leaked N instances!". Closing it here avoids that.
    """
    from dbt.adapters.duckdb.connections import DuckDBConnectionManager
    env = DuckDBConnectionManager._ENV
    if env is not None:
        env.close()
    DuckDBConnectionManager.close_all_connections()


@functools.cache
def github_token():
    """The GitHub PAT: TOKEN env var first, else the Prefect Secret block (deployment)."""
    token = os.environ.get("TOKEN")
    if token:
        return token
    try:
        from prefect.blocks.system import Secret
        return Secret.load(SECRET_BLOCK).get()
    except Exception as e:
        raise ValueError(f"No GitHub PAT: set the TOKEN environment variable, or create a "
                         f"Prefect Secret block named {SECRET_BLOCK!r}") from e


def github_headers():
    return {"Authorization": f"Bearer {github_token()}",
            "Accept": "application/vnd.github+json"}


def landed_records(saved):
    """[(endpoint, records)] for one landing page.

    REST pages hold one endpoint's records as-is. A GraphQL page holds PRs AND their
    reviews, so it is reshaped into REST-style "pulls" and "pr_reviews" records.
    """
    if saved["endpoint"] in GRAPHQL_ENDPOINTS:
        return list(to_rest_records(saved).items())
    return [(saved["endpoint"], saved["data"])]


def page_path(repo, endpoint, page, pr_number=None):
    folder = RAW_DIR / repo.replace("/", "__") / endpoint
    if pr_number is not None:
        folder = folder / str(pr_number)
    return folder / f"page_{page:04d}.json"


def read_saved(filename):
    """Return the saved page, or None if missing or half-written."""
    try:
        with open(filename) as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return None


def save_page(filename, repo, endpoint, page, url, data, next_url, pr_number=None, extra=None):
    filename.parent.mkdir(parents=True, exist_ok=True)
    result = {
        "repo": repo,
        "endpoint": endpoint,
        "url": url,
        "page": page,
        "fetched_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "next_url": next_url,
        **(extra or {}),             # GraphQL pages: cursor, next_cursor, variables
        "data": data,
    }
    if pr_number is not None:
        result["pr_number"] = pr_number

    tmp = filename.with_suffix(".tmp")
    with open(tmp, "w") as f:
        json.dump(result, f)
    tmp.replace(filename)          # atomic: a crash never leaves a partial page


def get_all_pages(repo, endpoint, url, headers, pr_number=None, stop_field=None, stop_before=START_DATE):
    logger = get_run_logger()
    page, total = 1, 0

    while url:
        filename = page_path(repo, endpoint, page, pr_number)
        saved = read_saved(filename)

        if saved is not None:                      # resume: reuse what's on disk
            total += len(saved["data"])
            url = saved["next_url"]
            page += 1
            continue

        response = rest_get(url, headers, repo, endpoint, logger)
        try:
            data = response.json()
        except ValueError as e:
            raise RuntimeError(f"{repo} {endpoint}: page {page} is not valid JSON ({url}): "
                               f"{response.text[:300]}") from e

        next_url = response.links.get("next", {}).get("url")

        # PRs are sorted by updated desc: once the oldest record on this
        # page is before the window, nothing further is needed.
        if stop_field and data and data[-1][stop_field] < stop_before:
            next_url = None

        save_page(filename, repo, endpoint, page, url, data, next_url, pr_number)
        total += len(data)
        url, page = next_url, page + 1

    logger.debug(f"{repo} {endpoint}{'#' + str(pr_number) if pr_number else ''}: {total} records")
    if pr_number is None:
        logger.info(f"{repo} {endpoint}: {total} records")
    return total

def get_pull_requests(repo, updated_since=START_DATE):
    folder = RAW_DIR / repo.replace("/", "__") / "pulls"
    pull_requests = []

    for filename in sorted(folder.glob("page_*.json")):
        saved = read_saved(filename)

        if saved is None:
            continue

        for pr in saved["data"]:
            if pr["created_at"] >= START_DATE and pr["updated_at"] >= updated_since:
                pull_requests.append(pr)

    return pull_requests


def get_graphql_pages(repo, endpoint, query, variables, connection, headers,
                      pr_number=None, stop_before=None):
    """Cursor-paginate one GraphQL connection, writing one landing file per response page.

    `connection(data)` picks the paginated connection out of the response's `data`.
    Resume works like REST: each saved page stores its `next_cursor`, so pages already on
    disk are reused and fetching continues from the last good cursor. With `stop_before`
    (PRs sorted by updatedAt desc) paging stops once a page reaches records older than it.
    Returns every node across all pages.
    """
    logger = get_run_logger()
    page, cursor, nodes = 1, variables.get("cursor"), []
    while True:
        filename = page_path(repo, endpoint, page, pr_number)
        saved = read_saved(filename)
        if saved is not None:                               # resume: reuse what's on disk
            conn, next_cursor = connection(saved["data"]["data"]), saved["next_cursor"]
        else:
            page_vars = {**variables, "cursor": cursor}
            body = graphql_post(query, page_vars, headers, repo, endpoint, logger)
            conn = connection(body["data"])
            info = conn["pageInfo"]
            next_cursor = info["endCursor"] if info["hasNextPage"] else None
            if stop_before and conn["nodes"] and conn["nodes"][-1]["updatedAt"] < stop_before:
                next_cursor = None
            save_page(filename, repo, endpoint, page, GRAPHQL_URL, body, None, pr_number,
                      extra={"cursor": cursor, "next_cursor": next_cursor, "variables": page_vars})
        nodes += conn["nodes"]
        if next_cursor is None:
            return nodes
        cursor, page = next_cursor, page + 1


def extract_prs_graphql(repo, headers, pulls_cutoff):
    """PRs + reviews via GraphQL: one query returns 50 PRs with up to 100 reviews each."""
    logger = get_run_logger()
    owner, name = repo.split("/")
    prs = get_graphql_pages(
        repo, "graphql_pulls", PULLS_QUERY,
        {"owner": owner, "name": name, "first": PRS_PER_PAGE},
        lambda d: d["repository"]["pullRequests"], headers, stop_before=pulls_cutoff)
    reviews = sum(len(pr["reviews"]["nodes"]) for pr in prs)

    # A PR with more than 100 reviews gets follow-up pages, starting from its review cursor.
    for pr in prs:
        info = pr["reviews"]["pageInfo"]
        if info["hasNextPage"]:
            reviews += len(get_graphql_pages(
                repo, "graphql_reviews", REVIEWS_QUERY,
                {"owner": owner, "name": name, "number": pr["number"], "cursor": info["endCursor"]},
                lambda d: d["repository"]["pullRequest"]["reviews"], headers,
                pr_number=pr["number"]))
    in_window = sum(pr["createdAt"] >= START_DATE for pr in prs)
    logger.info(f"{repo} pulls (GraphQL): {len(prs)} records ({in_window} created in window)")
    logger.info(f"{repo} pr_reviews (GraphQL): {reviews} records")


def extract_prs_rest(repo, headers, pulls_cutoff):
    """PRs via REST pages, then one REST reviews call per PR created in the window."""
    logger = get_run_logger()
    base_url = f"{API_BASE}/repos/{repo}"
    pulls_url = f"{base_url}/pulls?state=all&sort=updated&direction=desc&per_page=100"
    get_all_pages(repo, "pulls", pulls_url, headers,
                  stop_field="updated_at", stop_before=pulls_cutoff)

    pull_requests = get_pull_requests(repo, updated_since=pulls_cutoff)
    review_total = 0
    for pr in pull_requests:
        reviews_url = f"{base_url}/pulls/{pr['number']}/reviews?per_page=100"
        review_total += get_all_pages(repo, "pr_reviews", reviews_url, headers,
                                      pr_number=pr["number"])
    logger.info(f"{repo} pr_reviews: {review_total} records across {len(pull_requests)} PRs")


@task(retries=3, retry_delay_seconds=[30, 60, 120], cache_policy=NO_CACHE,
      task_run_name="extract-{repo}")
def extract_repo(repo, watermarks, ingest_mode):
    """Helper task for Task 1: every endpoint for ONE repo. Mapped over the three repos so
    they download in parallel; the shared limiters in api_client keep the combined request
    rate under the single token's quota. A retry resumes from the pages already on disk."""
    logger = get_run_logger()
    headers = github_headers()
    started, base_url = time.time(), f"{API_BASE}/repos/{repo}"
    logger.info(f"Starting extraction for {repo} ({ingest_mode} PRs/reviews)")

    commits_since = watermarks.get((repo, "commits"), START_DATE)
    get_all_pages(repo, "commits", f"{base_url}/commits?since={commits_since}&per_page=100", headers)

    pulls_cutoff = watermarks.get((repo, "pulls"), START_DATE)
    if ingest_mode == "graphql":
        extract_prs_graphql(repo, headers, pulls_cutoff)
    else:
        extract_prs_rest(repo, headers, pulls_cutoff)

    get_all_pages(repo, "releases", f"{base_url}/releases?per_page=100", headers)

    seconds = time.time() - started
    logger.info(f"Finished extraction for {repo} in {seconds:,.0f}s "
                f"({ODOMETER.total(repo=repo)} API requests so far this run)")
    return {"repo": repo, "seconds": round(seconds, 1)}


@task(retries=1, retry_delay_seconds=60)
@log_to_file("extract")
def extract(ingest_mode="graphql"):
    # talks to github and writes files
    # It never writes to DuckDB. Its job is to get every page of every endpoint onto disk, without exceeding the rate limit and without losing work if the run is interrupted.
    logger = get_run_logger()
    if ingest_mode not in ("graphql", "rest"):
        raise ValueError(f"ingest_mode must be 'graphql' or 'rest', not {ingest_mode!r}")
    github_token()                       # fail fast, before any threads start, if no PAT

    marker = RAW_DIR / "_LOADED"
    if marker.exists():
        logger.info("Found _LOADED: everything on disk is in DuckDB -> starting fresh "
                    "(deleting data/raw, fetching only records newer than _load_state)")
        shutil.rmtree(RAW_DIR)
    elif RAW_DIR.exists():
        logger.info("No _LOADED: last run did not finish loading -> resuming, "
                    "reusing pages already on disk")
    watermarks = read_watermarks()
    run_type = "incremental" if watermarks else "full window"
    logger.info(f"Mode: {run_type} run, PRs/reviews via {ingest_mode.upper()}, "
                f"{len(REPOS)} repos fetched in parallel")

    # One mapped helper task per repo. Wait for ALL of them before judging the result, so a
    # retry of this task never starts while another repo's thread is still writing pages.
    started = time.time()
    futures = extract_repo.map(REPOS, watermarks=unmapped(watermarks),
                               ingest_mode=unmapped(ingest_mode))
    wait(futures)
    failed = [repo for repo, f in zip(REPOS, futures) if not f.state.is_completed()]
    if failed:
        raise RuntimeError(f"Extraction failed for {failed}; pages already on disk are kept "
                           f"and the next attempt resumes from them")
    per_repo = {f.result()["repo"]: f.result()["seconds"] for f in futures}
    seconds = time.time() - started

    # API odometer: every request this flow run, by API / repo / endpoint.
    odometer = ODOMETER.rows()
    logger.info("API odometer (requests sent to GitHub this run, retries included):\n" +
                "\n".join(f"  {r['api']:<7} {r['repo']:<27} {r['endpoint']:<16} "
                          f"{r['requests']:>6} requests"
                          + (f", {r['graphql_points']} points" if r["api"] == "graphql" else "")
                          for r in odometer))
    for repo in REPOS:
        logger.info(f"  {repo}: {ODOMETER.total(repo=repo)} requests, {per_repo[repo]:,.0f}s")
    summary = {
        "ingest_mode": ingest_mode,
        "run_type": run_type,
        "extract_seconds": round(seconds, 1),
        "rest_requests": ODOMETER.total(api="rest"),
        "graphql_requests": ODOMETER.total(api="graphql"),
        "total_requests": ODOMETER.total(),
        "odometer": odometer,
    }
    logger.info(f"Total API requests this run: {summary['total_requests']} "
                f"({summary['rest_requests']} REST + {summary['graphql_requests']} GraphQL) "
                f"in {seconds / 60:,.1f} min [{run_type}, {ingest_mode}]")
    logger.info("Task 1 extraction complete!")
    return summary

@task(retries=2, retry_delay_seconds=10)
def load():
    # Reads files and writes to DuckDB. It never calls the API.
    # Re-running it on the same files never creates duplicates (upsert on (repo, id)).
    with task_log_file("load") as logger:
        files = sorted(RAW_DIR.rglob("page_*.json"))
        if not files:
            raise FileNotFoundError("No JSON pages found in data/raw. Run extract() first.")

        # Remove any old completion marker before starting a new load.
        marker = RAW_DIR / "_LOADED"
        marker.unlink(missing_ok=True)

        # 1. Read and validate every page BEFORE touching the database,
        #    turning each record into one row: (repo, id, updated_at, fetched_at, source_file, payload)
        rows = {table: [] for table in ENDPOINT_TABLES.values()}
        newest_dates = {}       # (repo, endpoint) -> newest updated_at (ISO strings sort correctly)
        record_counts = {}      # (repo, endpoint) -> records read from disk

        for filename in files:
            try:
                saved = json.loads(filename.read_text())
                repo, fetched_at = saved["repo"], saved["fetched_at"]
                batches = landed_records(saved)      # GraphQL pages -> REST-style records
            except (json.JSONDecodeError, KeyError, TypeError) as e:
                raise ValueError(f"Could not parse landing file {filename}: {e!r}") from e

            if repo not in REPOS:
                raise ValueError(f"Unknown repo in {filename}: {repo}")

            for endpoint, records in batches:
                if endpoint not in ENDPOINT_TABLES:
                    raise ValueError(f"Unknown endpoint in {filename}: {endpoint}")
                if not isinstance(records, list):
                    raise ValueError(f"Expected a list of {endpoint} records in {filename}")

                key = (repo, endpoint)
                record_counts[key] = record_counts.get(key, 0) + len(records)

                for record in records:
                    updated_at = get_updated_at(endpoint, record)
                    rows[ENDPOINT_TABLES[endpoint]].append((
                        repo, get_record_id(endpoint, record), updated_at,
                        fetched_at, str(filename), json.dumps(record),
                    ))
                    if updated_at and updated_at > newest_dates.get(key, ""):
                        newest_dates[key] = updated_at

        # 2. Upsert everything in ONE transaction, so a crash leaves the old tables untouched.
        con = duckdb.connect(str(DATABASE))
        try:
            for table in ENDPOINT_TABLES.values():
                con.execute(f"""
                    CREATE TABLE IF NOT EXISTS {table} (
                        repo        VARCHAR NOT NULL,
                        id          VARCHAR NOT NULL,
                        updated_at  TIMESTAMP,
                        fetched_at  TIMESTAMP,
                        source_file VARCHAR,
                        payload     JSON,
                        PRIMARY KEY (repo, id)
                    )
                """)
            con.execute("""
                CREATE TABLE IF NOT EXISTS _load_state (
                    repo              VARCHAR NOT NULL,
                    endpoint          VARCHAR NOT NULL,
                    newest_updated_at TIMESTAMP,
                    loaded_at         TIMESTAMP,
                    PRIMARY KEY (repo, endpoint)
                )
            """)

            before = {t: con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
                      for t in ENDPOINT_TABLES.values()}

            con.execute("BEGIN TRANSACTION")
            for table, table_rows in rows.items():
                # Bulk-insert into a scratch table, then a single upsert into the real one.
                con.execute("""
                    CREATE OR REPLACE TEMP TABLE staged (
                        repo VARCHAR, id VARCHAR, updated_at TIMESTAMP,
                        fetched_at TIMESTAMP, source_file VARCHAR, payload JSON
                    )
                """)
                if table_rows:
                    con.executemany("INSERT INTO staged VALUES (?, ?, ?, ?, ?, ?)", table_rows)

                # The same record can land on two pages (e.g. a PR updated mid-pagination),
                # and DuckDB can't update one row twice in a statement, so keep the newest copy first.
                con.execute(f"""
                    INSERT INTO {table} BY NAME
                    SELECT * FROM staged
                    QUALIFY row_number() OVER (
                        PARTITION BY repo, id ORDER BY fetched_at DESC, source_file DESC) = 1
                    ON CONFLICT (repo, id) DO UPDATE SET
                        updated_at  = excluded.updated_at,
                        fetched_at  = excluded.fetched_at,
                        source_file = excluded.source_file,
                        payload     = excluded.payload
                    WHERE excluded.fetched_at >= {table}.fetched_at
                """)

            # Watermark is updated only after every file has loaded (same transaction).
            for (repo, endpoint) in record_counts:
                con.execute("""
                    INSERT INTO _load_state (repo, endpoint, newest_updated_at, loaded_at)
                    VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                    ON CONFLICT (repo, endpoint) DO UPDATE SET
                        newest_updated_at = GREATEST(_load_state.newest_updated_at,
                                                     excluded.newest_updated_at),
                        loaded_at = excluded.loaded_at
                """, [repo, endpoint, newest_dates.get((repo, endpoint))])
            con.execute("COMMIT")

            # Row counts before/after: running the load twice on the same files shows +0.
            after = {}
            for table in ENDPOINT_TABLES.values():
                after[table] = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                logger.info(f"{table}: {before[table]} rows before -> {after[table]} rows after "
                            f"(+{after[table] - before[table]})")
        except Exception:
            try:
                con.execute("ROLLBACK")
            except duckdb.Error:
                pass
            raise
        finally:
            con.close()

        for (repo, endpoint), count in sorted(record_counts.items()):
            logger.info(f"{repo} {endpoint}: {count} records read from {RAW_DIR}")

        # Only mark complete after the transaction has committed.
        marker.touch()
        logger.info(f"Task 2 complete: {len(files)} files loaded, created {marker}")
        return after


@task
def build_test():
    # The only place dbt runs. `dbt build` = run every model + every test, in dependency order.
    # dbt writes its own log to logs/dbt.log.
    with task_log_file("build_test") as logger:
        try:
            result = dbtRunner().invoke([
                "build",
                "--project-dir", str(DBT_DIR),
                "--profiles-dir", str(DBT_DIR),
            ])
        finally:
            close_dbt_duckdb()

        if result.exception is not None:          # dbt itself crashed (bad SQL compile, no DB...)
            raise RuntimeError(f"dbt build could not run: {result.exception}") from result.exception

        failures = []
        for r in result.result.results:
            line = f"{r.node.resource_type} {r.node.name}: {r.status}"
            if r.status in ("error", "fail"):
                failures.append(r.node.name)
                logger.error(f"{line} -> {r.message}")
            elif r.status == "warn":
                logger.warning(f"{line} -> {r.message}")
            else:
                logger.info(line)

        if not result.success or failures:
            raise RuntimeError(f"dbt build failed: {failures or 'see logs/dbt.log'}")

        logger.info(f"Task 3 complete: {len(result.result.results)} dbt models/tests passed")
        return len(result.result.results)

def query(con, sql, params=None):
    """Run SQL and return the whole result as a pandas DataFrame."""
    return con.execute(sql, params or []).df()


def dimension_label(dimension):
    return dimension.replace("_", " ").capitalize()


def landed_keys():
    """Every distinct (repo, endpoint, id) in the landing files of data/raw/."""
    keys = set()
    for filename in RAW_DIR.rglob("page_*.json"):
        saved = read_saved(filename)
        if saved is None:
            raise ValueError(f"Could not parse landing file {filename}")
        for endpoint, records in landed_records(saved):     # same reshaping Task 2 uses
            for record in records:
                keys.add((saved["repo"], endpoint, get_record_id(endpoint, record)))
    return pd.DataFrame(sorted(keys), columns=["repo", "endpoint", "id"])


def check_completeness(con, logger):
    """Files -> raw -> staging -> monthly mart. Returns a list of problems (empty = all good)."""
    problems = []

    def report(ok, msg):
        if ok:
            logger.info(f"  OK   {msg}")
        else:
            logger.error(f"  FAIL {msg}")
            problems.append(msg)

    # 1. Every distinct record in data/raw/ is in its raw table.
    #    (After an incremental run data/raw/ holds only that run's pages, so this checks those.)
    logger.info(f"Check 1: every distinct (repo, id) in {RAW_DIR}/ appears in its raw table")
    landed = landed_keys()
    con.register("landed", landed)
    try:
        for endpoint, table in ENDPOINT_TABLES.items():
            found = query(con, f"""
                select l.repo, count(*) as landed, count(t.id) as in_raw
                from landed l
                left join {table} t on t.repo = l.repo and t.id = l.id
                where l.endpoint = ?
                group by l.repo order by l.repo
            """, [endpoint])
            if found.empty:
                logger.warning(f"  no landed {endpoint} files to compare against {table}")
            for r in found.itertuples():
                report(r.in_raw == r.landed,
                       f"{r.repo} {endpoint}: {r.landed} distinct landed records, {r.in_raw} in {table}")
    finally:
        con.unregister("landed")

    # 2. Raw row count matches staging, after the rows staging drops on purpose.
    logger.info("Check 2: raw row count = staging row count (+ rows staging intentionally drops)")
    for raw_table, (stg_model, dropped_if) in STAGING_MODELS.items():
        counts = query(con, f"""
            with r as (
                select repo,
                       count(distinct id) as raw_rows,
                       count(distinct id) filter (where {dropped_if or 'false'}) as dropped
                from {raw_table} group by repo
            ),
            s as (select repo, count(*) as stg_rows from {stg_model} group by repo)
            select r.repo, r.raw_rows, r.dropped, coalesce(s.stg_rows, 0) as stg_rows
            from r left join s using (repo) order by r.repo
        """)
        for repo in sorted(set(REPOS) - set(counts.repo)):
            report(False, f"{repo}: no rows at all in {raw_table}")
        for r in counts.itertuples():
            report(r.raw_rows - r.dropped == r.stg_rows,
                   f"{r.repo} {raw_table}: {r.raw_rows} raw - {r.dropped} dropped = "
                   f"{r.raw_rows - r.dropped}, {stg_model} has {r.stg_rows}")

    # 3. Commits in every complete month. A month with zero commits is only accepted when the
    #    pull requests endpoint (fetched separately) also shows zero merges that month: merging
    #    a PR creates a commit, so "merges but no commits" means commit pages were lost, while
    #    "no merges and no commits" means the repo was genuinely quiet.
    logger.info("Check 3: every repo has commits in every complete month of the window")
    months = query(con, """
        select repo, month_start, commits, prs_merged
        from mart_repo_monthly where not is_partial_month
        order by repo, month_start
    """)
    for repo, g in months.groupby("repo"):
        zero = g[g.commits == 0]
        lost = zero[zero.prs_merged > 0]
        quiet = zero[zero.prs_merged == 0]
        report(lost.empty,
               f"{repo}: {len(g) - len(zero)}/{len(g)} complete months have commits" +
               (f"; months with merged PRs but 0 commits: "
                f"{', '.join(lost.month_start.astype(str))}" if not lost.empty else ""))
        if not quiet.empty:
            logger.warning(f"  WARN {repo}: 0 commits AND 0 merged PRs in "
                           f"{', '.join(quiet.month_start.astype(str))} -> real inactivity, "
                           f"not missing data (kept as 0)")
    return problems


def flag_outliers(con, logger):
    """Flag extreme merge times and weekly commit bursts. Flagged records are kept (see logs)."""
    window_start = f"timestamptz '{START_DATE[:10]} 00:00:00+00'"

    # Time to merge: durations are heavily right-skewed, so an IQR fence on raw hours flags
    # ~10% of perfectly normal PRs. A fixed, interpretable cutoff targets the real oddities.
    logger.info(f"Outliers 1: merged PRs with hours_to_merge > {MERGE_OUTLIER_HOURS} "
                f"({MERGE_OUTLIER_HOURS // 24} days)")
    slow = query(con, """
        select repo, pr_number, author_login, created_at, merged_at, hours_to_merge,
               count(*) over (partition by repo) as n_flagged
        from fct_pull_requests
        where is_merged and hours_to_merge > ?
        order by repo, hours_to_merge desc
    """, [MERGE_OUTLIER_HOURS])
    merged = query(con, "select repo, count(*) as n from fct_pull_requests where is_merged group by repo")
    for r in merged.sort_values("repo").itertuples():
        flagged = slow[slow.repo == r.repo]
        logger.info(f"  {r.repo}: {len(flagged)} of {r.n} merged PRs flagged")
        for p in flagged.itertuples():
            logger.info(f"    #{p.pr_number} by {p.author_login}: {p.hours_to_merge / 24:,.0f} days "
                        f"(opened {p.created_at:%Y-%m-%d}, merged {p.merged_at:%Y-%m-%d})")

    # Weekly commits: counts are much less skewed, so a per-repo Tukey far-out fence works.
    logger.info(f"Outliers 2: weeks with commits > Q3 + {WEEKLY_COMMIT_IQR_K} x IQR "
                f"of that repo's weekly commits")
    bursts = query(con, f"""
        with weekly as (
            select repo, date_trunc('week', committed_at) as week_start,
                   count(*) as commits, count(*) filter (where is_bot) as bot_commits
            from stg_commits
            where committed_at >= {window_start}
            group by all
        ),
        fences as (
            select repo,
                   quantile_cont(commits, 0.75)
                     + ? * (quantile_cont(commits, 0.75) - quantile_cont(commits, 0.25)) as cutoff
            from weekly group by repo
        )
        select w.repo, w.week_start, w.commits, w.bot_commits, f.cutoff
        from weekly w join fences f using (repo)
        order by w.repo, w.week_start
    """, [WEEKLY_COMMIT_IQR_K])
    for repo, g in bursts.groupby("repo"):
        flagged = g[g.commits > g.cutoff]
        logger.info(f"  {repo}: cutoff {g.cutoff.iloc[0]:.1f} commits/week, "
                    f"{len(flagged)} of {len(g)} active weeks flagged")
        for w in flagged.itertuples():
            logger.info(f"    week of {w.week_start:%Y-%m-%d}: {w.commits} commits "
                        f"({w.bot_commits} by bots)")

    logger.info("Outlier decision: KEEP all flagged records. They are real events (long-lived "
                "PRs that eventually merged, release or refactor pushes), not data errors. Task 5 "
                "scores medians, shares and slopes, which a handful of extreme values barely move, "
                "so capping or dropping would hide real behaviour without changing the ranking.")
    return {"slow_merges": len(slow), "commit_burst_weeks": int((bursts.commits > bursts.cutoff).sum())}


def describe(con, logger):
    """Per-repo medians and 90th percentiles of review/merge time, plus weekly and yearly rates."""
    stats = query(con, """
        select p.repo,
               median(hours_to_first_review)             as median_first_review_h,
               quantile_cont(hours_to_first_review, 0.9) as p90_first_review_h,
               median(hours_to_merge)                    as median_merge_h,
               quantile_cont(hours_to_merge, 0.9)        as p90_merge_h,
               any_value(h.commits_per_week)             as commits_per_week,
               any_value(h.releases_per_year)            as releases_per_year
        from fct_pull_requests p
        join mart_health_inputs h using (repo)
        where not p.author_is_bot
        group by p.repo order by p.repo
    """)
    logger.info("Descriptive statistics (human-authored PRs; durations in hours; open and "
                "never-reviewed PRs are NULL and fall out of the medians):\n"
                + stats.to_string(index=False, float_format=lambda v: f"{v:,.1f}"))
    return stats


@task
@log_to_file("validate")
def validate():
    # Python only, no dbt: checks the pipeline end to end and describes the data before modeling.
    logger = get_run_logger()
    con = duckdb.connect(str(DATABASE), read_only=True)
    try:
        problems = check_completeness(con, logger)
        outliers = flag_outliers(con, logger)
        stats = describe(con, logger)
    finally:
        con.close()

    if problems:
        raise ValueError(f"Task 4: {len(problems)} completeness check(s) failed:\n  "
                         + "\n  ".join(problems))
    logger.info("Task 4 complete: all completeness checks passed")
    return {"outliers": outliers, "stats": stats.to_dict("records")}


def score_repos(inputs):
    """Signed z-score per metric -> mean per dimension -> weighted composite score and rank."""
    columns = [m["column"] for m in HEALTH_METRICS]
    values = inputs[columns].astype(float)
    if values.isna().any().any():
        missing = values.columns[values.isna().any()].tolist()
        raise ValueError(f"mart_health_inputs has NULLs in scored metrics: {missing}")

    # z-score across the three repos; flip "lower is healthier" metrics so higher z = healthier.
    spread = values.std(ddof=0).replace(0, np.nan)           # identical values -> z = 0
    z = ((values - values.mean()) / spread).fillna(0)
    for m in HEALTH_METRICS:
        z[m["column"]] *= m["direction"]

    subscores = pd.DataFrame({
        dim: z[[m["column"] for m in HEALTH_METRICS if m["dimension"] == dim]].mean(axis=1)
        for dim in DIMENSION_WEIGHTS
    })
    subscores["score"] = sum(subscores[d] * w for d, w in DIMENSION_WEIGHTS.items())
    ranked = subscores.sort_values("score", ascending=False)
    ranked.insert(0, "rank", range(1, len(ranked) + 1))
    return ranked.reset_index(), z.reset_index()


@task
@log_to_file("model")
def model():
    logger = get_run_logger()
    assert abs(sum(DIMENSION_WEIGHTS.values()) - 1) < 1e-9, "dimension weights must sum to 1"

    con = duckdb.connect(str(DATABASE))
    try:
        inputs = query(con, "select * from mart_health_inputs order by repo").set_index("repo")

        logger.info("Scored metrics (from mart_health_inputs):")
        for m in HEALTH_METRICS:
            arrow = "higher" if m["direction"] > 0 else "lower"
            logger.info(f"  [{dimension_label(m['dimension'])}] {m['column']} ({arrow} is healthier): "
                        f"{m['definition']}")
        logger.info("Raw metric values:\n" + inputs[[m["column"] for m in HEALTH_METRICS]].T
                    .to_string(float_format=lambda v: f"{v:,.3f}"))

        ranked, z = score_repos(inputs)
        logger.info("Signed z-scores (positive = healthier than the 3-repo average):\n"
                    + z.set_index("repo").T.to_string(float_format=lambda v: f"{v:+.2f}"))
        logger.info("Weights: " + ", ".join(f"{dimension_label(d)} {w:.0%}"
                                            for d, w in DIMENSION_WEIGHTS.items()))
        logger.info("Health ranking (score = weighted sum of dimension subscores):\n"
                    + ranked.to_string(index=False, float_format=lambda v: f"{v:+.3f}"))

        # Write back to the database for Task 6 and anyone querying packages.duckdb.
        metric_rows = z.melt(id_vars="repo", var_name="metric", value_name="signed_z").merge(
            inputs.reset_index().melt(id_vars="repo", var_name="metric", value_name="value"),
            on=["repo", "metric"])
        metric_rows["dimension"] = metric_rows.metric.map(
            {m["column"]: m["dimension"] for m in HEALTH_METRICS})
        metric_rows["direction"] = metric_rows.metric.map(
            {m["column"]: m["direction"] for m in HEALTH_METRICS})
        con.register("ranked_df", ranked)
        con.register("metric_df", metric_rows)
        con.execute("create or replace table health_scores as "
                    "select *, current_timestamp as scored_at from ranked_df order by rank")
        con.execute("create or replace table health_metric_scores as "
                    "select repo, dimension, metric, direction, value, signed_z from metric_df "
                    "order by repo, dimension, metric")
        con.unregister("ranked_df")
        con.unregister("metric_df")
    finally:
        con.close()

    logger.info(f"Task 5 complete: wrote health_scores ({len(ranked)} rows) and "
                f"health_metric_scores ({len(metric_rows)} rows) to {DATABASE}")
    return ranked.to_dict("records")


def rank_labels(scores):
    """'#1 scikit-learn/scikit-learn (+0.42)' for each repo, keyed by repo, in rank order."""
    return {r.repo: f"#{r.rank} {r.repo} (score {r.score:+.2f})" for r in scores.itertuples()}


def plot_dimension_profile(scores):
    """Radar chart: every scored dimension for every repo, legend ordered by final rank."""
    dims = list(DIMENSION_WEIGHTS)
    angles = np.linspace(0, 2 * np.pi, len(dims), endpoint=False).tolist()
    labels = rank_labels(scores)

    fig, ax = plt.subplots(figsize=(9, 8), subplot_kw={"polar": True})
    for r in scores.itertuples():
        vals = [getattr(r, d) for d in dims]
        ax.plot(angles + angles[:1], vals + vals[:1], color=REPO_COLORS[r.repo], linewidth=2.2,
                marker="o", label=labels[r.repo])
        ax.fill(angles + angles[:1], vals + vals[:1], color=REPO_COLORS[r.repo], alpha=0.10)
    ax.plot(np.linspace(0, 2 * np.pi, 200), [0] * 200, color="grey", linestyle="--", linewidth=1)
    ax.set_xticks(angles)
    ax.set_xticklabels([f"{dimension_label(d)}\n(weight {DIMENSION_WEIGHTS[d]:.0%})" for d in dims])
    ax.set_ylim(-1.6, 1.6)
    ax.set_yticks([-1, 0, 1])
    ax.set_yticklabels(["-1 SD", "avg", "+1 SD"], color="grey")
    ax.set_rlabel_position(60)                     # put ring labels in an empty gap
    ax.set_title("Health dimension profile, ranked healthiest to least healthy\n"
                 "subscore = mean signed z-score of the dimension's metrics (outward = healthier)",
                 pad=30)
    ax.legend(title="Rank, repo (composite score)", loc="upper center",
              bbox_to_anchor=(0.5, -0.08), ncol=1, frameon=False)
    path = PLOTS_DIR / "dimension_profile.png"
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return path


def plot_monthly_trends(monthly, scores):
    """Small multiples from mart_repo_monthly: one panel per metric, one line per repo."""
    panels = [
        ("commits",                   "Commits",                     "commits / month", False),
        ("active_contributors",       "Active PR authors (non-bot)", "people / month", False),
        ("prs_opened",                "PRs opened",                  "PRs / month", False),
        ("prs_merged",                "PRs merged",                  "PRs / month", False),
        ("prs_closed_unmerged",       "PRs closed without merge",    "PRs / month", False),
        ("merge_ratio",               "Merge ratio",                 "merged / (merged + closed unmerged)", False),
        ("median_first_review_hours", "Median time to first review", "hours (log scale)", True),
        ("median_merge_hours",        "Median time to merge",        "hours (log scale)", True),
    ]
    labels = rank_labels(scores)
    fig, axes = plt.subplots(4, 2, figsize=(14, 15), sharex=True)
    partial = monthly.loc[monthly.is_partial_month, "month_start"].min()

    for ax, (col, title, unit, log) in zip(axes.flat, panels):
        for repo in scores.repo:
            g = monthly[monthly.repo == repo]
            y = g[col].where(g[col] > 0) if log else g[col]
            ax.plot(g.month_start, y, color=REPO_COLORS[repo], linewidth=1.8, label=labels[repo])
        if log:
            ax.set_yscale("log")
        if pd.notna(partial):
            ax.axvspan(partial, partial + pd.offsets.MonthEnd(1), color="grey", alpha=0.15, lw=0)
        ax.set_title(title)
        ax.set_ylabel(unit)
        ax.grid(alpha=0.3)
    for ax in axes[-1]:
        ax.set_xlabel("month")
        ax.tick_params(axis="x", rotation=45)

    handles, names = axes.flat[0].get_legend_handles_labels()
    fig.legend(handles, names, loc="upper center", ncol=3, frameon=False, bbox_to_anchor=(0.5, 0.965))
    fig.suptitle(f"Monthly activity, {monthly.month_start.min():%b %Y} to "
                 f"{monthly.month_start.max():%b %Y} (grey band = current partial month)",
                 fontsize=15, y=0.99)
    fig.tight_layout(rect=(0, 0, 1, 0.95))
    path = PLOTS_DIR / "monthly_trends.png"
    fig.savefig(path, dpi=150)
    plt.close(fig)
    return path


def plot_relationship(monthly, inputs, scores, filename="review_vs_merge_bubbles.png",
                      period="", diamond_label="whole-window value (diamond)"):
    """Bubble chart: x = review time, y = merge ratio, size = active authors, color = repo.

    `inputs` supplies the diamonds (one per repo) and needs the columns
    median_hours_to_first_review, merge_rate and active_contributors_per_month.
    """
    labels = rank_labels(scores)
    complete = monthly[~monthly.is_partial_month & monthly.median_first_review_hours.gt(0)
                       & monthly.merge_ratio.notna()]
    size = lambda people: 15 + 6 * people          # bubble area per active PR author

    fig, ax = plt.subplots(figsize=(11, 7.5))
    for repo in scores.repo:
        g = complete[complete.repo == repo]
        ax.scatter(g.median_first_review_hours, g.merge_ratio, s=size(g.active_contributors),
                   color=REPO_COLORS[repo], alpha=0.35, edgecolor="none")
        overall = inputs.loc[repo]
        ax.scatter(overall.median_hours_to_first_review, overall.merge_rate,
                   s=size(overall.active_contributors_per_month), color=REPO_COLORS[repo],
                   edgecolor="black", linewidth=1.5, marker="D", zorder=3)
    # legend: fixed-size color keys (in rank order), then a bubble-size key
    handles = [plt.Line2D([], [], marker="o", linestyle="", markersize=10, alpha=0.6,
                          color=REPO_COLORS[repo], label=labels[repo]) for repo in scores.repo]
    handles += [ax.scatter([], [], s=size(people), color="grey", alpha=0.4,
                           label=f"{people} active PR authors") for people in (5, 25, 75)]
    handles.append(ax.scatter([], [], s=80, color="white", edgecolor="black", marker="D",
                              label=diamond_label))

    ax.set_xscale("log")
    ax.set_xlabel("Median time to first review that month (hours, log scale)")
    ax.set_ylabel("Merge ratio that month (merged / (merged + closed unmerged))")
    ax.set_title(f"Faster reviews vs. merge ratio, one bubble per repo-month{period}\n"
                 "bubble size = active non-bot PR authors that month; color = repo")
    ax.grid(alpha=0.3, which="both")
    ax.legend(handles=handles, loc="center left", bbox_to_anchor=(1.01, 0.5), frameon=False,
              labelspacing=1.2)
    path = PLOTS_DIR / filename
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return path


def plot_contributor_load(monthly, scores):
    """Bubble chart: x = active PR authors, y = merge ratio, size = PRs opened, color = repo.

    One point per complete repo-month, plus a least-squares trend line per repo. Each
    repo's Spearman rank correlation between authors and merge ratio is shown in the
    legend (rank-based, so a few extreme months can't dominate it).
    """
    labels = rank_labels(scores)
    points = monthly[~monthly.is_partial_month & monthly.merge_ratio.notna()]
    size = lambda prs: 10 + 2 * prs                 # bubble area per PR opened

    fig, ax = plt.subplots(figsize=(11, 7.5))
    handles = []
    for repo in scores.repo:
        g = points[points.repo == repo]
        color = REPO_COLORS[repo]
        ax.scatter(g.active_contributors, g.merge_ratio, s=size(g.prs_opened),
                   color=color, alpha=0.35, edgecolor="none")
        slope, intercept = np.polyfit(g.active_contributors, g.merge_ratio, 1)
        xs = np.linspace(g.active_contributors.min(), g.active_contributors.max(), 200)
        ys = intercept + slope * xs
        valid = (ys >= 0) & (ys <= 1)               # a merge ratio only exists in [0, 1]
        ax.plot(xs[valid], ys[valid], color=color, linewidth=2.5)
        rho = g.active_contributors.corr(g.merge_ratio, method="spearman")
        handles.append(plt.Line2D([], [], color=color, marker="o", markersize=9, linewidth=2.5,
                                  label=f"{labels[repo]}, Spearman ρ = {rho:+.2f}"))
    handles += [ax.scatter([], [], s=size(prs), color="grey", alpha=0.4,
                           label=f"{prs} PRs opened that month") for prs in (10, 50, 150)]

    ax.set_xlabel("Active non-bot PR authors that month (people)")
    ax.set_ylabel("Merge ratio that month (merged / (merged + closed unmerged))")
    ax.set_title("Busier contributor months, lower merge ratio: one bubble per repo-month\n"
                 "bubble size = PRs opened that month; color = repo; line = linear trend per repo")
    ax.set_ylim(-0.03, 1.05)
    ax.grid(alpha=0.3)
    ax.legend(handles=handles, loc="center left", bbox_to_anchor=(1.01, 0.5), frameon=False,
              labelspacing=1.2)
    path = PLOTS_DIR / "contributors_vs_merge_ratio.png"
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return path


def pre_cutoff_inputs(con):
    """Whole-period diamonds for the pre-October bubble chart, from data before COMPARISON_CUTOFF.

    Same definitions as mart_health_inputs (human-authored PRs only), restricted to
    events before the cutoff. Durations come precomputed from fct_pull_requests; only
    medians, counts and averages are taken here.
    """
    cutoff = f"timestamptz '{COMPARISON_CUTOFF} 00:00:00+00'"
    return query(con, f"""
        with prs as (
            select
                repo,
                median(hours_to_first_review) filter (where first_review_at < {cutoff})
                    as median_hours_to_first_review,
                count(*) filter (where merged_at < {cutoff})
                  / nullif(count(*) filter (where merged_at < {cutoff}
                                           or (is_closed_unmerged and closed_at < {cutoff})), 0)
                    as merge_rate
            from fct_pull_requests
            where not author_is_bot and created_at < {cutoff}
            group by repo
        ),
        authors as (
            select repo, avg(active_contributors) as active_contributors_per_month
            from mart_repo_monthly
            where month_start < date '{COMPARISON_CUTOFF}'
            group by repo
        )
        select * from prs join authors using (repo)
    """).set_index("repo")


@task
@log_to_file("plot")
def plot():
    logger = get_run_logger()
    con = duckdb.connect(str(DATABASE), read_only=True)
    try:
        scores = query(con, "select * from health_scores order by rank")
        monthly = query(con, "select * from mart_repo_monthly order by repo, month_start")
        inputs = query(con, "select * from mart_health_inputs").set_index("repo")
        pre_inputs = pre_cutoff_inputs(con)
    finally:
        con.close()
    monthly["month_start"] = pd.to_datetime(monthly.month_start)
    pre_monthly = monthly[monthly.month_start < pd.Timestamp(COMPARISON_CUTOFF)]
    last_month = (pd.Timestamp(COMPARISON_CUTOFF) - pd.offsets.MonthBegin(1)).strftime("%b %Y")

    PLOTS_DIR.mkdir(exist_ok=True)
    paths = [
        plot_dimension_profile(scores),
        plot_monthly_trends(monthly, scores),
        plot_relationship(monthly, inputs, scores),
        # Same chart, before dask's maintainer stall (Oct 2026 on), so the three repos are
        # compared over a period when all of them were active. Supplementary: scoring and
        # the other plots still use the full open-ended window.
        plot_relationship(pre_monthly, pre_inputs, scores,
                          filename="review_vs_merge_bubbles_pre_oct2026.png",
                          period=f" (Jan 2024 – {last_month} only)",
                          diamond_label=f"Jan 2024 – {last_month} value (diamond)"),
        plot_contributor_load(monthly, scores),
    ]
    for p in paths:
        logger.info(f"Saved {p}")
    logger.info(f"Task 6 complete: {len(paths)} plots in {PLOTS_DIR}/")
    return [str(p) for p in paths]


@task
@log_to_file("api_odometer")
def record_api_usage(summary):
    """Helper task (after Task 2): save this flow run's API odometer to DuckDB.

    Task 1 must never write to DuckDB, so it only returns its counts; this task appends
    them to api_odometer (one row per API x repo x endpoint, keyed by flow run) and logs
    the request history of every recorded run, e.g. full vs. incremental, REST vs. GraphQL.
    """
    logger = get_run_logger()
    run_id = str(flow_run.id or f"local-{datetime.now(timezone.utc):%Y%m%dT%H%M%S}")
    usage = pd.DataFrame(summary["odometer"],
                         columns=["api", "repo", "endpoint", "requests", "graphql_points"])
    usage.insert(0, "flow_run_id", run_id)
    usage["ingest_mode"] = summary["ingest_mode"]
    usage["run_type"] = summary["run_type"]
    usage["extract_seconds"] = summary["extract_seconds"]

    con = duckdb.connect(str(DATABASE))
    try:
        con.execute("""
            create table if not exists api_odometer (
                flow_run_id VARCHAR, recorded_at TIMESTAMP, api VARCHAR, repo VARCHAR,
                endpoint VARCHAR, requests INTEGER, graphql_points INTEGER,
                ingest_mode VARCHAR, run_type VARCHAR, extract_seconds DOUBLE
            )
        """)
        con.register("usage_df", usage)
        con.execute("delete from api_odometer where flow_run_id = ?", [run_id])   # retry-safe
        con.execute("insert into api_odometer by name "
                    "select *, current_timestamp as recorded_at from usage_df")
        con.unregister("usage_df")
        history = query(con, """
            select flow_run_id, min(recorded_at) as recorded_at, any_value(run_type) as run_type,
                   any_value(ingest_mode) as ingest_mode,
                   sum(requests) filter (where api = 'rest')    as rest_requests,
                   sum(requests) filter (where api = 'graphql') as graphql_requests,
                   sum(requests) as total_requests, sum(graphql_points) as graphql_points,
                   round(any_value(extract_seconds) / 60, 1) as extract_minutes
            from api_odometer group by flow_run_id order by recorded_at
        """)
    finally:
        con.close()
    logger.info(f"Recorded {usage.requests.sum()} requests for flow run {run_id} in api_odometer")
    logger.info("API odometer history (every recorded flow run):\n"
                + history.fillna(0).to_string(index=False))
    return int(usage.requests.sum())


@flow(name="qtr7bs-package-health-flow")
def main(ingest_mode: Literal["graphql", "rest"] = "graphql"):
    """ingest_mode picks how PRs and reviews are fetched: GraphQL (default, minutes) or
    the original REST path (one reviews call per PR, ~1 hour), kept for comparison."""
    os.chdir(PROJECT_DIR)        # relative paths (data/, logs/, plots/, dbt's DuckDB path)
                                 # resolve to the repo, wherever the flow is launched from
    ODOMETER.reset()             # the odometer counts this flow run only
    summary = extract(ingest_mode)
    load()
    record_api_usage(summary)
    build_test()
    validate()
    model()
    plot()
    get_run_logger().info(f"API odometer: {summary['total_requests']} GitHub API requests this "
                          f"flow run ({summary['rest_requests']} REST, "
                          f"{summary['graphql_requests']} GraphQL)")


if __name__ == "__main__":
    import sys
    # python main.py          -> PRs/reviews via GraphQL (default)
    # python main.py rest     -> original REST path, for the REST vs. GraphQL comparison
    main(ingest_mode=sys.argv[1] if len(sys.argv) > 1 else "graphql")
