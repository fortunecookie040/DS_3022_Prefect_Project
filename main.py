# put your prefect flow here (6 core tasks)
# please give your main @flow a name identifiable with your computing ID, such as the example below
# this will help instructional staff find and review your logs in the prefect server.

from prefect import flow, task, get_run_logger
from pathlib import Path
from contextlib import contextmanager
import httpx, json, os, time, duckdb, logging, shutil, functools
from datetime import datetime, timezone
from dbt.cli.main import dbtRunner


START_DATE = "2024-01-01T00:00:00Z"
RAW_DIR = Path("data/raw")
DATABASE = Path("packages.duckdb")
LOG_DIR = Path("logs")
DBT_DIR = Path(__file__).parent      # dbt_project.yml + profiles.yml live in the repo root
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

REQUEST_COUNT = {"total": 0}        # API odometer: every HTTP call to GitHub this run

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

def get_page(url, headers, max_tries=5):
    tries = 0
    while True:
        try:
            REQUEST_COUNT["total"] += 1
            response = httpx.get(url, headers=headers, timeout=60)
        except httpx.TransportError:
            tries += 1
            if tries >= max_tries:
                raise
            time.sleep(2 ** tries)
            continue

        if response.status_code in (403, 429):
            remaining = response.headers.get("X-RateLimit-Remaining")
            reset = response.headers.get("X-RateLimit-Reset")
            if remaining == "0" and reset:
                wait = max(1, int(reset) - int(time.time())) + 5
            elif response.headers.get("Retry-After", "").isdigit():
                wait = int(response.headers["Retry-After"])
            else:
                response.raise_for_status()
            time.sleep(wait)
            continue

        if response.status_code >= 500:
            tries += 1
            if tries >= max_tries:
                response.raise_for_status()
            time.sleep(2 ** tries)
            continue

        response.raise_for_status()

        # sleep BEFORE the budget hits zero, not after a 403
        remaining = int(response.headers.get("X-RateLimit-Remaining", 100))
        reset = int(response.headers.get("X-RateLimit-Reset", 0))
        if remaining <= 5:
            time.sleep(max(1, reset - int(time.time())) + 5)
        return response


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


def save_page(filename, repo, endpoint, page, url, data, next_url, pr_number=None):
    filename.parent.mkdir(parents=True, exist_ok=True)
    result = {
        "repo": repo,
        "endpoint": endpoint,
        "url": url,
        "page": page,
        "fetched_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "next_url": next_url,
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

        response = get_page(url, headers)
        data = response.json()

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

@task(retries=3, retry_delay_seconds=[30, 60, 120])
@log_to_file("extract")
def extract():
    # talks to github and writes files
    # It never writes to DuckDB. Its job is to get every page of every endpoint onto disk, without exceeding the rate limit and without losing work if the run is interrupted.
    logger = get_run_logger()
    REQUEST_COUNT["total"] = 0

    token = os.environ.get("TOKEN")

    if not token:
        raise ValueError("Please set the TOKEN environment variable to your GitHub PAT first.")

    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json"
    }

    marker = RAW_DIR / "_LOADED"
    if marker.exists():
        logger.info("Found _LOADED: everything on disk is in DuckDB -> starting fresh "
                    "(deleting data/raw, fetching only records newer than _load_state)")
        shutil.rmtree(RAW_DIR)
    elif RAW_DIR.exists():
        logger.info("No _LOADED: last run did not finish loading -> resuming, "
                    "reusing pages already on disk")
    watermarks = read_watermarks()
    logger.info(f"Mode: {'incremental' if watermarks else 'full window'} run")
    requests_by_repo = {}

    for repo in REPOS:
        logger.info(f"Starting extraction for {repo}")
        repo_start = REQUEST_COUNT["total"]

        base_url = f"{API_BASE}/repos/{repo}"

        # Download commits
        commits_since = watermarks.get((repo, "commits"), START_DATE)
        commits_url = (
            f"{base_url}/commits?since={commits_since}&per_page=100"
        )
        get_all_pages(repo, "commits", commits_url, headers)

        # Download pull requests
        pulls_cutoff = watermarks.get((repo, "pulls"), START_DATE)
        pulls_url = (
            f"{base_url}/pulls?state=all&sort=updated"
            f"&direction=desc&per_page=100"
        )
        get_all_pages(repo, "pulls", pulls_url, headers,
                      stop_field="updated_at", stop_before=pulls_cutoff)

        # Download reviews for each PR since January 2024
        pull_requests = get_pull_requests(repo, updated_since=pulls_cutoff)
        review_total = 0

        for pr in pull_requests:
            number = pr["number"]
            reviews_url = (
                f"{base_url}/pulls/{number}/reviews?per_page=100"
            )

            review_total += get_all_pages(
                repo, "pr_reviews", reviews_url,
                headers, pr_number=number
            )
        logger.info(f"{repo} pr_reviews: {review_total} records across {len(pull_requests)} PRs")

        # Download releases
        releases_url = f"{base_url}/releases?per_page=100"
        get_all_pages(repo, "releases", releases_url, headers)

        requests_by_repo[repo] = REQUEST_COUNT["total"] - repo_start
        logger.info(f"Finished extraction for {repo} ({requests_by_repo[repo]} API requests)")

    logger.info(f"Total API requests this run: {REQUEST_COUNT['total']}")
    logger.info("Task 1 extraction complete!")
    return {"requests_by_repo": requests_by_repo, "total_requests": REQUEST_COUNT["total"]}

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
                repo, endpoint = saved["repo"], saved["endpoint"]
                fetched_at, records = saved["fetched_at"], saved["data"]
            except (json.JSONDecodeError, KeyError) as e:
                raise ValueError(f"Could not parse landing file {filename}: {e}") from e

            if repo not in REPOS:
                raise ValueError(f"Unknown repo in {filename}: {repo}")
            if endpoint not in ENDPOINT_TABLES:
                raise ValueError(f"Unknown endpoint in {filename}: {endpoint}")
            if not isinstance(records, list):
                raise ValueError(f"Expected a list in 'data' of {filename}")

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
        result = dbtRunner().invoke([
            "build",
            "--project-dir", str(DBT_DIR),
            "--profiles-dir", str(DBT_DIR),
        ])

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

@task
def validate():
    pass

@task 
def model():
    pass

@task 
def plot():
    pass

@flow(name="qtr7bs-package-health-flow")
def main():
    extract()
    # load()
    # build_test()
    # validate()
    # model()
    # plot()


if __name__ == "__main__":
    main()
