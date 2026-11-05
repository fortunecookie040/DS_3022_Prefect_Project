# Prefect Deployments

All previous in-class examples of Prefect flows have run (executed) on student laptops, while their logging and output has been visible through the SDS Prefect server https://sds-prefect.pods.uvarc.io/.

However, an advanced feature of Prefect is that it can run scheduled or "detached" flows on the server itself. 

Here's how to set that up and enable it:

## Blocks / Secrets

1. Keep your entire flow script and all related files in a single repository. You will need the repo URL as part of the deployment.
2. Any private ENV variables your script depends on (such as your `GH_PAT`) must first be published to the Prefect Server as "Blocks" (i.e. a feature that can store secret variables you can call within your code).
   1. Go to https://sds-prefect.pods.uvarc.io/v2/blocks
   2. Select "+ New storage block"
   3. Select "Secret" (a generic secret), then "Create"
   4. Give the block a unique **name**, using your computing ID, such as `nem2p-pat` (lowercase letters, numbers, and dashes only). The value should be of the "password" type and contain the actual value of your PAT.
   5. Click "Save".
3. To use a Prefect Block within your flow, use this syntax:

    ```
    from prefect.blocks.system import Secret

    my_secret = Secret.load("nem2p-pat")

    # Access the stored secret in a reusable object
    secret = my_secret.get()
    ```

## Deployments

See the [reference example](https://gist.github.com/nmagee/177f916afefbc7ae5bcfa7e213ca5dd9) for all details and setup, but the crux is found in the `__main__` handler at the bottom:

```
if __name__ == "__main__":
    # runs midnight Eastern on the 28th of every month
    <flow_function_name>.from_source(
        source=GIT_URL,
        entrypoint="main.py:<flow_function_name>",
    ).deploy(
        name="package-health",
        work_pool_name=WORK_POOL,
        schedule=Cron("0 0 28 * *", timezone="America/New_York"),
        job_variables={
            "cpu_request": "2",
            "cpu_limit": "2",
            "memory_request": "2Gi",
            "memory_limit": "4Gi",
        },
    )
```

In the reference example a GitHub Gist repository was used, but any normal GitHub repo will work, using the `https://` prefix with the `.git` suffix.

### Private repositories

If your repo is private, the Prefect worker needs your PAT to clone it. A PAT with no scopes can only read public repositories, so generate a new one that can also read private repositories, and store it in your Secret block in place of the old one:

1. Go to GitHub → Settings → Developer settings → Personal access tokens → **Tokens (classic)** → "Generate new token (classic)".
2. Set an expiration that lasts past the end of the semester (60 days).
3. Check the **`repo`** scope. No other scopes are needed.
4. Copy the token and update the value of your Secret block (e.g. `nem2p-pat`).

A fine-grained token also works if you prefer narrower access: grant it access to your project repository with **Contents: Read-only** permission.

Instead of passing `GIT_URL` as a plain string, wrap it in a `GitRepository` object and pass your Secret block as the credential:

```
from prefect.blocks.system import Secret
from prefect.runner.storage import GitRepository

# be sure to use your PAT block below
if __name__ == "__main__":
    <flow_function_name>.from_source(
        source=GitRepository(
            url=GIT_URL,
            branch="main",
            credentials={"access_token": Secret.load("nem2p-pat")},
        ),
        entrypoint="main.py:<flow_function_name>",
    ).deploy(
        ...  # same deploy() arguments as above
    )
```

Pass the loaded `Secret` block itself, not `Secret.load(...).get()`. The deployment then stores only a reference to the block, and the worker fetches the token at run time, so the PAT is never written into the deployment's configuration.

The entrypoint should be the filename+primary flow function name, like this:

    main.py:package_health_flow

Keep the following values as written above:

- `WORK_POOL = "uvasds-k8s-pool"`
- `job_variables` and its values

To publish your deployment once written (and the entire code including your GIT_URL is correct and its content up-to-date), run this command from your project, pointing at the flow's entrypoint:

    prefect deploy ./main.py:<flow_function_name> \
        --name nem2p-deployment \
        --pool uvasds-k8s-pool

## Test your deployment

You should see your deployment listed in https://sds-prefect.pods.uvarc.io/v2/deployments

Select it to see details and any previous runs. Use the blue "Run" button in the upper-right to trigger a run, which you can then follow like any other run.

**For this project, trigger your deployment manually once.** The first scheduled run may not fall before the due date, so a manual run from the "Run" button is what you submit. Copy its log into `DEPLOYMENTS.md` along with your deployment command.

See the [NYC-Taxi-Monthly](https://sds-prefect.pods.uvarc.io/v2/deployments/deployment/987975de-1d75-48d3-810e-251d9e077b16?tab=Runs) sample deployment and try running it for yourself.

The savvy BSDS student will recognize that some outputs such as plots will not be available since they are built into the remote environment executing the code. This is okay ;^)

(In production the plots would be copied off to remote storage like S3, etc.)
---

# My deployment (qtr7bs)

**Flow:** `main.py:main` (`qtr7bs-package-health-flow`) · **Deployment:** `qtr7bs-deployment` · **Work pool:** `uvasds-k8s-pool` · **Schedule:** `0 0 28 * *`, America/New_York (midnight Eastern on the 28th of every month)

The deployment is defined in [`deploy.py`](deploy.py):

- **Code.** The worker clones this private repo with `GitRepository(..., credentials={"access_token": Secret.load("qtr7bs-pat")})`. Only a reference to the block is stored in the deployment, never the token itself.
- **Token.** The flow gets its GitHub token from the same block. `github_token()` in `main.py` uses the `TOKEN` environment variable when it exists and otherwise falls back to `Secret.load("qtr7bs-pat")`, which is what happens on the worker pod.
- **Job variables.** `job_variables` are exactly the CPU and memory values above, and nothing else. The class worker image (`ghcr.io/uvads/prefect-k8s-worker:3.9.0-python3.12`) already includes prefect, dbt, duckdb, pandas, matplotlib and httpx, in a read-only `site-packages`. My first attempt also passed `EXTRA_PIP_PACKAGES` to install the pinned versions from `requirements.txt`, and the pod failed (see below), so that was removed.
- **Flow parameter.** `ingest_mode="graphql"`, so the scheduled run fetches PRs and reviews through GraphQL and takes minutes, not about an hour.

**Incremental vs. full on the server.** Each worker pod starts from a fresh clone with no `packages.duckdb` and no `data/raw/`, so `_load_state` is empty and the scheduled run fetches the full window. The same code runs incrementally anywhere the database persists, as it does on my laptop. In production, `packages.duckdb` would live on persistent storage (a volume, S3, or MotherDuck) so the monthly run could be incremental there too. The same applies to the plots (see above).

## Deployment command

Run from the project root after pushing the latest code:

```bash
python deploy.py
```

Equivalent CLI form:

```bash
prefect deploy ./main.py:main --name qtr7bs-deployment --pool uvasds-k8s-pool
```

## Log of a manually triggered run

Triggered with the **Run → Quick run** button in the Prefect UI on 2026-11-05 (UTC).

| | |
| --- | --- |
| Flow run | `gorgeous-basilisk` (`5782152e-829e-4b44-a4c2-76dbb0272e69`) |
| Deployment | `qtr7bs-package-health-flow/qtr7bs-deployment`, work pool `uvasds-k8s-pool` |
| Result | **Completed**: 04:26:13 → 04:29:41 UTC (3.5 min) |
| Mode | Full-window GraphQL run (fresh pod, no `packages.duckdb`): **266 API requests** (81 REST + 185 GraphQL) |
| Checks | Task 2 loaded 7,566 commits, 8,650 PRs, 30,493 reviews and 204 releases. Task 3: 47/47 dbt models and tests passed. Task 4: all completeness checks passed. Tasks 5–6 completed. |

The record and request counts match my local full GraphQL run exactly. The only warning is the expected one for dask's October 2026, which had no maintainer activity (see the README).

**The first attempt failed, and here is why.** The first manual run (`provocative-tamarin`) had `EXTRA_PIP_PACKAGES` in its job variables, so the pod tried to replace the image's preinstalled packages with my pinned versions and stopped before the flow started:

```
error: failed to remove file `/usr/local/lib/python3.12/site-packages/matplotlib-3.11.2.dist-info/INSTALLER`: Read-only file system (os error 30)
```

I removed the extra packages from `deploy.py`, so `job_variables` is now exactly the instructor's values, then republished with `python deploy.py` and cancelled the stuck run. The run below used the image's own packages (for example matplotlib 3.11.2) with no code changes.

**Key lines:**

```
2026-11-05 04:26:00 | INFO    | Worker 'KubernetesWorker 59b8229a-1097-400e-afd7-b4547b8611e4' submitting flow run '5782152e-829e-4b44-a4c2-76dbb0272e69'
2026-11-05 04:26:07 | INFO    | Deployment step 'git_clone' completed successfully
2026-11-05 04:26:13 | INFO    | Beginning flow run 'gorgeous-basilisk' for flow 'qtr7bs-package-health-flow'
2026-11-05 04:26:13 | INFO    | Mode: full window run, PRs/reviews via GRAPHQL, 3 repos fetched in parallel
2026-11-05 04:26:41 | INFO    | Finished extraction for plotly/plotly.py in 28s (49 API requests so far this run)
2026-11-05 04:26:49 | INFO    | Finished extraction for dask/dask in 36s (44 API requests so far this run)
2026-11-05 04:29:00 | INFO    | Finished extraction for scikit-learn/scikit-learn in 167s (173 API requests so far this run)
2026-11-05 04:29:00 | INFO    | Total API requests this run: 266 (81 REST + 185 GraphQL) in 2.8 min [full window, graphql]
2026-11-05 04:29:26 | INFO    | raw_commits: 0 rows before -> 7566 rows after (+7566)
2026-11-05 04:29:26 | INFO    | raw_pull_requests: 0 rows before -> 8650 rows after (+8650)
2026-11-05 04:29:26 | INFO    | raw_pr_reviews: 0 rows before -> 30493 rows after (+30493)
2026-11-05 04:29:26 | INFO    | raw_releases: 0 rows before -> 204 rows after (+204)
2026-11-05 04:29:27 | INFO    | Task 2 complete: 266 files loaded, created data/raw/_LOADED
2026-11-05 04:29:27 | INFO    | Recorded 266 requests for flow run 5782152e-829e-4b44-a4c2-76dbb0272e69 in api_odometer
2026-11-05 04:29:35 | INFO    | Task 3 complete: 47 dbt models/tests passed
2026-11-05 04:29:35 | WARNING |   WARN dask/dask: 0 commits AND 0 merged PRs in 2026-10-01 -> real inactivity, not missing data (kept as 0)
2026-11-05 04:29:35 | INFO    | Task 4 complete: all completeness checks passed
2026-11-05 04:29:36 | INFO    | Task 5 complete: wrote health_scores (3 rows) and health_metric_scores (30 rows) to packages.duckdb
2026-11-05 04:29:41 | INFO    | Task 6 complete: 5 plots in plots/
2026-11-05 04:29:41 | INFO    | API odometer: 266 GitHub API requests this flow run (81 REST, 185 GraphQL)
2026-11-05 04:29:42 | INFO    | Finished in state Completed()
```

<details>
<summary>Full log (253 lines, from the Prefect server)</summary>

```
2026-11-05 04:26:00 | INFO    | Worker 'KubernetesWorker 59b8229a-1097-400e-afd7-b4547b8611e4' submitting flow run '5782152e-829e-4b44-a4c2-76dbb0272e69'
2026-11-05 04:26:00 | INFO    | Creating Kubernetes job...
2026-11-05 04:26:00 | INFO    | Kubernetes job 'gorgeous-basilisk-j6j9n' created in namespace 'uvasds-services'
2026-11-05 04:26:00 | INFO    | Flow run 'gorgeous-basilisk' submitted to infrastructure
2026-11-05 04:26:07 | INFO    | Running 1 deployment pull step(s)
2026-11-05 04:26:07 | INFO    | Executing deployment step: git_clone
2026-11-05 04:26:07 | INFO    | Deployment step 'git_clone' completed successfully
2026-11-05 04:26:08 | INFO    | All deployment steps completed successfully
2026-11-05 04:26:13 | INFO    | Beginning flow run 'gorgeous-basilisk' for flow 'qtr7bs-package-health-flow'
2026-11-05 04:26:13 | INFO    | Mode: full window run, PRs/reviews via GRAPHQL, 3 repos fetched in parallel
2026-11-05 04:26:13 | INFO    | Starting extraction for scikit-learn/scikit-learn (graphql PRs/reviews)
2026-11-05 04:26:13 | INFO    | Starting extraction for plotly/plotly.py (graphql PRs/reviews)
2026-11-05 04:26:13 | INFO    | Starting extraction for dask/dask (graphql PRs/reviews)
2026-11-05 04:26:19 | INFO    | dask/dask commits: 1371 records
2026-11-05 04:26:24 | INFO    | plotly/plotly.py commits: 2831 records
2026-11-05 04:26:28 | INFO    | scikit-learn/scikit-learn commits: 3364 records
2026-11-05 04:26:41 | INFO    | plotly/plotly.py pulls (GraphQL): 850 records (690 created in window)
2026-11-05 04:26:41 | INFO    | plotly/plotly.py pr_reviews (GraphQL): 1751 records
2026-11-05 04:26:41 | INFO    | plotly/plotly.py releases: 109 records
2026-11-05 04:26:41 | INFO    | Finished extraction for plotly/plotly.py in 28s (49 API requests so far this run)
2026-11-05 04:26:41 | INFO    | Finished in state Completed()
2026-11-05 04:26:49 | INFO    | dask/dask pulls (GraphQL): 1450 records (1233 created in window)
2026-11-05 04:26:49 | INFO    | dask/dask pr_reviews (GraphQL): 2774 records
2026-11-05 04:26:49 | INFO    | dask/dask releases: 44 records
2026-11-05 04:26:49 | INFO    | Finished extraction for dask/dask in 36s (44 API requests so far this run)
2026-11-05 04:26:49 | INFO    | Finished in state Completed()
2026-11-05 04:29:00 | INFO    | scikit-learn/scikit-learn pulls (GraphQL): 6350 records (5229 created in window)
2026-11-05 04:29:00 | INFO    | scikit-learn/scikit-learn pr_reviews (GraphQL): 25968 records
2026-11-05 04:29:00 | INFO    | scikit-learn/scikit-learn releases: 51 records
2026-11-05 04:29:00 | INFO    | Finished extraction for scikit-learn/scikit-learn in 167s (173 API requests so far this run)
2026-11-05 04:29:00 | INFO    | Finished in state Completed()
2026-11-05 04:29:00 | INFO    | API odometer (requests sent to GitHub this run, retries included):
  graphql dask/dask                   graphql_pulls        29 requests, 29 points
  graphql plotly/plotly.py            graphql_pulls        17 requests, 17 points
  graphql plotly/plotly.py            graphql_reviews       1 requests, 1 points
  graphql scikit-learn/scikit-learn   graphql_pulls       127 requests, 127 points
  graphql scikit-learn/scikit-learn   graphql_reviews      11 requests, 11 points
  rest    dask/dask                   commits              14 requests
  rest    dask/dask                   releases              1 requests
  rest    plotly/plotly.py            commits              29 requests
  rest    plotly/plotly.py            releases              2 requests
  rest    scikit-learn/scikit-learn   commits              34 requests
  rest    scikit-learn/scikit-learn   releases              1 requests
2026-11-05 04:29:00 | INFO    |   scikit-learn/scikit-learn: 173 requests, 167s
2026-11-05 04:29:00 | INFO    |   dask/dask: 44 requests, 36s
2026-11-05 04:29:00 | INFO    |   plotly/plotly.py: 49 requests, 28s
2026-11-05 04:29:00 | INFO    | Total API requests this run: 266 (81 REST + 185 GraphQL) in 2.8 min [full window, graphql]
2026-11-05 04:29:00 | INFO    | Task 1 extraction complete!
2026-11-05 04:29:00 | INFO    | Finished in state Completed()
2026-11-05 04:29:26 | INFO    | raw_commits: 0 rows before -> 7566 rows after (+7566)
2026-11-05 04:29:26 | INFO    | raw_pull_requests: 0 rows before -> 8650 rows after (+8650)
2026-11-05 04:29:26 | INFO    | raw_pr_reviews: 0 rows before -> 30493 rows after (+30493)
2026-11-05 04:29:26 | INFO    | raw_releases: 0 rows before -> 204 rows after (+204)
2026-11-05 04:29:27 | INFO    | dask/dask commits: 1371 records read from data/raw
2026-11-05 04:29:27 | INFO    | dask/dask pr_reviews: 2774 records read from data/raw
2026-11-05 04:29:27 | INFO    | dask/dask pulls: 1450 records read from data/raw
2026-11-05 04:29:27 | INFO    | dask/dask releases: 44 records read from data/raw
2026-11-05 04:29:27 | INFO    | plotly/plotly.py commits: 2831 records read from data/raw
2026-11-05 04:29:27 | INFO    | plotly/plotly.py pr_reviews: 1751 records read from data/raw
2026-11-05 04:29:27 | INFO    | plotly/plotly.py pulls: 850 records read from data/raw
2026-11-05 04:29:27 | INFO    | plotly/plotly.py releases: 109 records read from data/raw
2026-11-05 04:29:27 | INFO    | scikit-learn/scikit-learn commits: 3364 records read from data/raw
2026-11-05 04:29:27 | INFO    | scikit-learn/scikit-learn pr_reviews: 25968 records read from data/raw
2026-11-05 04:29:27 | INFO    | scikit-learn/scikit-learn pulls: 6350 records read from data/raw
2026-11-05 04:29:27 | INFO    | scikit-learn/scikit-learn releases: 51 records read from data/raw
2026-11-05 04:29:27 | INFO    | Task 2 complete: 266 files loaded, created data/raw/_LOADED
2026-11-05 04:29:27 | INFO    | Finished in state Completed()
2026-11-05 04:29:27 | INFO    | Recorded 266 requests for flow run 5782152e-829e-4b44-a4c2-76dbb0272e69 in api_odometer
2026-11-05 04:29:27 | INFO    | API odometer history (every recorded flow run):
                         flow_run_id                recorded_at    run_type ingest_mode  rest_requests  graphql_requests  total_requests  graphql_points  extract_minutes
5782152e-829e-4b44-a4c2-76dbb0272e69 2026-11-05 04:29:27.506374 full window     graphql           81.0             185.0           266.0           185.0              2.8
2026-11-05 04:29:27 | INFO    | Finished in state Completed()
2026-11-05 04:29:35 | INFO    | model stg_commits: success
2026-11-05 04:29:35 | INFO    | model stg_pr_reviews: success
2026-11-05 04:29:35 | INFO    | model stg_pull_requests: success
2026-11-05 04:29:35 | INFO    | model stg_releases: success
2026-11-05 04:29:35 | INFO    | test not_null_stg_commits_commit_key: pass
2026-11-05 04:29:35 | INFO    | test not_null_stg_commits_committed_at: pass
2026-11-05 04:29:35 | INFO    | test not_null_stg_commits_repo: pass
2026-11-05 04:29:35 | INFO    | test not_null_stg_commits_sha: pass
2026-11-05 04:29:35 | INFO    | test unique_stg_commits_commit_key: pass
2026-11-05 04:29:35 | INFO    | test accepted_values_stg_pr_reviews_state__APPROVED__CHANGES_REQUESTED__COMMENTED__DISMISSED: pass
2026-11-05 04:29:35 | INFO    | test not_null_stg_pr_reviews_pr_key: pass
2026-11-05 04:29:35 | INFO    | test not_null_stg_pr_reviews_pr_number: pass
2026-11-05 04:29:35 | INFO    | test not_null_stg_pr_reviews_review_id: pass
2026-11-05 04:29:35 | INFO    | test not_null_stg_pr_reviews_review_key: pass
2026-11-05 04:29:35 | INFO    | test not_null_stg_pr_reviews_state: pass
2026-11-05 04:29:35 | INFO    | test not_null_stg_pr_reviews_submitted_at: pass
2026-11-05 04:29:35 | INFO    | test unique_stg_pr_reviews_review_key: pass
2026-11-05 04:29:35 | INFO    | test accepted_values_stg_pull_requests_state__open__closed: pass
2026-11-05 04:29:35 | INFO    | test assert_pr_closed_after_created: pass
2026-11-05 04:29:35 | INFO    | test assert_review_after_pr_created: pass
2026-11-05 04:29:35 | INFO    | test not_null_stg_pull_requests_created_at: pass
2026-11-05 04:29:35 | INFO    | test not_null_stg_pull_requests_pr_key: pass
2026-11-05 04:29:35 | INFO    | test not_null_stg_pull_requests_pr_number: pass
2026-11-05 04:29:35 | INFO    | test not_null_stg_pull_requests_repo: pass
2026-11-05 04:29:35 | INFO    | test not_null_stg_pull_requests_state: pass
2026-11-05 04:29:35 | INFO    | test relationships_stg_pr_reviews_pr_key__pr_key__ref_stg_pull_requests_: pass
2026-11-05 04:29:35 | INFO    | test unique_stg_pull_requests_pr_key: pass
2026-11-05 04:29:35 | INFO    | test not_null_stg_releases_published_at: pass
2026-11-05 04:29:35 | INFO    | test not_null_stg_releases_release_id: pass
2026-11-05 04:29:35 | INFO    | test not_null_stg_releases_release_key: pass
2026-11-05 04:29:35 | INFO    | test unique_stg_releases_release_key: pass
2026-11-05 04:29:35 | INFO    | model fct_pull_requests: success
2026-11-05 04:29:35 | INFO    | model fct_releases: success
2026-11-05 04:29:35 | INFO    | test accepted_values_fct_pull_requests_state__open__closed: pass
2026-11-05 04:29:35 | INFO    | test not_null_fct_pull_requests_pr_key: pass
2026-11-05 04:29:35 | INFO    | test not_null_fct_pull_requests_repo: pass
2026-11-05 04:29:35 | INFO    | test unique_fct_pull_requests_pr_key: pass
2026-11-05 04:29:35 | INFO    | test not_null_fct_releases_release_key: pass
2026-11-05 04:29:35 | INFO    | test unique_fct_releases_release_key: pass
2026-11-05 04:29:35 | INFO    | model mart_repo_monthly: success
2026-11-05 04:29:35 | INFO    | test not_null_mart_repo_monthly_month_start: pass
2026-11-05 04:29:35 | INFO    | test not_null_mart_repo_monthly_repo: pass
2026-11-05 04:29:35 | INFO    | test not_null_mart_repo_monthly_repo_month: pass
2026-11-05 04:29:35 | INFO    | test unique_mart_repo_monthly_repo_month: pass
2026-11-05 04:29:35 | INFO    | model mart_health_inputs: success
2026-11-05 04:29:35 | INFO    | test not_null_mart_health_inputs_repo: pass
2026-11-05 04:29:35 | INFO    | test unique_mart_health_inputs_repo: pass
2026-11-05 04:29:35 | INFO    | Task 3 complete: 47 dbt models/tests passed
2026-11-05 04:29:35 | INFO    | Finished in state Completed()
2026-11-05 04:29:35 | INFO    | Check 1: every distinct (repo, id) in data/raw/ appears in its raw table
2026-11-05 04:29:35 | INFO    |   OK   dask/dask commits: 1371 distinct landed records, 1371 in raw_commits
2026-11-05 04:29:35 | INFO    |   OK   plotly/plotly.py commits: 2831 distinct landed records, 2831 in raw_commits
2026-11-05 04:29:35 | INFO    |   OK   scikit-learn/scikit-learn commits: 3364 distinct landed records, 3364 in raw_commits
2026-11-05 04:29:35 | INFO    |   OK   dask/dask pulls: 1450 distinct landed records, 1450 in raw_pull_requests
2026-11-05 04:29:35 | INFO    |   OK   plotly/plotly.py pulls: 850 distinct landed records, 850 in raw_pull_requests
2026-11-05 04:29:35 | INFO    |   OK   scikit-learn/scikit-learn pulls: 6350 distinct landed records, 6350 in raw_pull_requests
2026-11-05 04:29:35 | INFO    |   OK   dask/dask pr_reviews: 2774 distinct landed records, 2774 in raw_pr_reviews
2026-11-05 04:29:35 | INFO    |   OK   plotly/plotly.py pr_reviews: 1751 distinct landed records, 1751 in raw_pr_reviews
2026-11-05 04:29:35 | INFO    |   OK   scikit-learn/scikit-learn pr_reviews: 25968 distinct landed records, 25968 in raw_pr_reviews
2026-11-05 04:29:35 | INFO    |   OK   dask/dask releases: 44 distinct landed records, 44 in raw_releases
2026-11-05 04:29:35 | INFO    |   OK   plotly/plotly.py releases: 109 distinct landed records, 109 in raw_releases
2026-11-05 04:29:35 | INFO    |   OK   scikit-learn/scikit-learn releases: 51 distinct landed records, 51 in raw_releases
2026-11-05 04:29:35 | INFO    | Check 2: raw row count = staging row count (+ rows staging intentionally drops)
2026-11-05 04:29:35 | INFO    |   OK   dask/dask raw_commits: 1371 raw - 0 dropped = 1371, stg_commits has 1371
2026-11-05 04:29:35 | INFO    |   OK   plotly/plotly.py raw_commits: 2831 raw - 0 dropped = 2831, stg_commits has 2831
2026-11-05 04:29:35 | INFO    |   OK   scikit-learn/scikit-learn raw_commits: 3364 raw - 0 dropped = 3364, stg_commits has 3364
2026-11-05 04:29:35 | INFO    |   OK   dask/dask raw_pull_requests: 1450 raw - 0 dropped = 1450, stg_pull_requests has 1450
2026-11-05 04:29:35 | INFO    |   OK   plotly/plotly.py raw_pull_requests: 850 raw - 0 dropped = 850, stg_pull_requests has 850
2026-11-05 04:29:35 | INFO    |   OK   scikit-learn/scikit-learn raw_pull_requests: 6350 raw - 0 dropped = 6350, stg_pull_requests has 6350
2026-11-05 04:29:35 | INFO    |   OK   dask/dask raw_pr_reviews: 2774 raw - 0 dropped = 2774, stg_pr_reviews has 2774
2026-11-05 04:29:35 | INFO    |   OK   plotly/plotly.py raw_pr_reviews: 1751 raw - 0 dropped = 1751, stg_pr_reviews has 1751
2026-11-05 04:29:35 | INFO    |   OK   scikit-learn/scikit-learn raw_pr_reviews: 25968 raw - 0 dropped = 25968, stg_pr_reviews has 25968
2026-11-05 04:29:35 | INFO    |   OK   dask/dask raw_releases: 44 raw - 0 dropped = 44, stg_releases has 44
2026-11-05 04:29:35 | INFO    |   OK   plotly/plotly.py raw_releases: 109 raw - 0 dropped = 109, stg_releases has 109
2026-11-05 04:29:35 | INFO    |   OK   scikit-learn/scikit-learn raw_releases: 51 raw - 0 dropped = 51, stg_releases has 51
2026-11-05 04:29:35 | INFO    | Check 3: every repo has commits in every complete month of the window
2026-11-05 04:29:35 | INFO    |   OK   dask/dask: 33/34 complete months have commits
2026-11-05 04:29:35 | WARNING |   WARN dask/dask: 0 commits AND 0 merged PRs in 2026-10-01 -> real inactivity, not missing data (kept as 0)
2026-11-05 04:29:35 | INFO    |   OK   plotly/plotly.py: 34/34 complete months have commits
2026-11-05 04:29:35 | INFO    |   OK   scikit-learn/scikit-learn: 34/34 complete months have commits
2026-11-05 04:29:35 | INFO    | Outliers 1: merged PRs with hours_to_merge > 8760 (365 days)
2026-11-05 04:29:35 | INFO    |   dask/dask: 0 of 884 merged PRs flagged
2026-11-05 04:29:35 | INFO    |   plotly/plotly.py: 5 of 471 merged PRs flagged
2026-11-05 04:29:35 | INFO    |     #5232 by robertoffmoura: 492 days (opened 2025-06-17, merged 2026-10-22)
2026-11-05 04:29:35 | INFO    |     #5311 by robertoffmoura: 427 days (opened 2025-08-08, merged 2026-10-09)
2026-11-05 04:29:35 | INFO    |     #5262 by robertoffmoura: 426 days (opened 2025-07-03, merged 2026-09-02)
2026-11-05 04:29:35 | INFO    |     #5310 by robertoffmoura: 426 days (opened 2025-08-07, merged 2026-10-07)
2026-11-05 04:29:35 | INFO    |     #5285 by robertoffmoura: 409 days (opened 2025-07-18, merged 2026-08-31)
2026-11-05 04:29:35 | INFO    |   scikit-learn/scikit-learn: 24 of 3363 merged PRs flagged
2026-11-05 04:29:35 | INFO    |     #28464 by marikabergengren: 777 days (opened 2024-02-19, merged 2026-04-06)
2026-11-05 04:29:35 | INFO    |     #29026 by hchau630: 694 days (opened 2024-05-15, merged 2026-04-09)
2026-11-05 04:29:35 | INFO    |     #28760 by jeremiedbb: 687 days (opened 2024-04-03, merged 2026-02-19)
2026-11-05 04:29:35 | INFO    |     #29532 by blitchj: 627 days (opened 2024-07-21, merged 2026-04-09)
2026-11-05 04:29:35 | INFO    |     #29499 by ArturoAmorQ: 626 days (opened 2024-07-16, merged 2026-04-03)
2026-11-05 04:29:35 | INFO    |     #29641 by snath-xoc: 571 days (opened 2024-08-08, merged 2026-03-03)
2026-11-05 04:29:35 | INFO    |     #28043 by StefanieSenger: 541 days (opened 2024-01-02, merged 2025-06-26)
2026-11-05 04:29:35 | INFO    |     #29661 by EmilyXinyi: 512 days (opened 2024-08-12, merged 2026-01-06)
2026-11-05 04:29:35 | INFO    |     #30869 by adrinjalali: 501 days (opened 2025-02-20, merged 2026-07-06)
2026-11-05 04:29:35 | INFO    |     #29155 by agitter: 492 days (opened 2024-06-01, merged 2025-10-06)
2026-11-05 04:29:35 | INFO    |     #31413 by adrinjalali: 488 days (opened 2025-05-22, merged 2026-09-22)
2026-11-05 04:29:35 | INFO    |     #30859 by adrinjalali: 454 days (opened 2025-02-19, merged 2026-05-19)
2026-11-05 04:29:35 | INFO    |     #28971 by OmarManzoor: 450 days (opened 2024-05-07, merged 2025-07-31)
2026-11-05 04:29:35 | INFO    |     #29097 by lorentzenchr: 445 days (opened 2024-05-24, merged 2025-08-12)
2026-11-05 04:29:35 | INFO    |     #28755 by marijavlajic: 442 days (opened 2024-04-02, merged 2025-06-18)
2026-11-05 04:29:35 | INFO    |     #29307 by maf-rnmourao: 440 days (opened 2024-06-19, merged 2025-09-02)
2026-11-05 04:29:35 | INFO    |     #30508 by lucyleeow: 432 days (opened 2024-12-19, merged 2026-02-23)
2026-11-05 04:29:35 | INFO    |     #29022 by deepyaman: 413 days (opened 2024-05-14, merged 2025-07-01)
2026-11-05 04:29:35 | INFO    |     #32042 by lorentzenchr: 407 days (opened 2025-08-28, merged 2026-10-09)
2026-11-05 04:29:35 | INFO    |     #31127 by lorentzenchr: 400 days (opened 2025-04-01, merged 2026-05-07)
2026-11-05 04:29:35 | INFO    |     #29310 by icfaust: 400 days (opened 2024-06-20, merged 2025-07-25)
2026-11-05 04:29:35 | INFO    |     #28559 by myenugula: 391 days (opened 2024-03-01, merged 2025-03-27)
2026-11-05 04:29:35 | INFO    |     #30134 by SuccessMoses: 379 days (opened 2024-10-22, merged 2025-11-05)
2026-11-05 04:29:35 | INFO    |     #30751 by snath-xoc: 369 days (opened 2025-02-02, merged 2026-02-06)
2026-11-05 04:29:35 | INFO    | Outliers 2: weeks with commits > Q3 + 3 x IQR of that repo's weekly commits
2026-11-05 04:29:35 | INFO    |   dask/dask: cutoff 46.2 commits/week, 2 of 130 active weeks flagged
2026-11-05 04:29:35 | INFO    |     week of 2024-01-08: 77 commits (0 by bots)
2026-11-05 04:29:35 | INFO    |     week of 2024-01-15: 75 commits (0 by bots)
2026-11-05 04:29:35 | INFO    |   plotly/plotly.py: cutoff 93.0 commits/week, 1 of 145 active weeks flagged
2026-11-05 04:29:35 | INFO    |     week of 2024-10-21: 98 commits (0 by bots)
2026-11-05 04:29:35 | INFO    |   scikit-learn/scikit-learn: cutoff 53.0 commits/week, 1 of 149 active weeks flagged
2026-11-05 04:29:35 | INFO    |     week of 2025-10-06: 83 commits (0 by bots)
2026-11-05 04:29:35 | INFO    | Outlier decision: KEEP all flagged records. They are real events (long-lived PRs that eventually merged, release or refactor pushes), not data errors. Task 5 scores medians, shares and slopes, which a handful of extreme values barely move, so capping or dropping would hide real behaviour without changing the ranking.
2026-11-05 04:29:35 | INFO    | Descriptive statistics (human-authored PRs; durations in hours; open and never-reviewed PRs are NULL and fall out of the medians):
                     repo  median_first_review_h  p90_first_review_h  median_merge_h  p90_merge_h  commits_per_week  releases_per_year
                dask/dask                   11.5               223.1            12.3        223.2               9.2               15.5
         plotly/plotly.py                   16.3               698.6            41.6      1,130.4              19.0               10.2
scikit-learn/scikit-learn                   13.0               277.7            36.1        843.1              22.7                5.3
2026-11-05 04:29:35 | INFO    | Task 4 complete: all completeness checks passed
2026-11-05 04:29:35 | INFO    | Finished in state Completed()
2026-11-05 04:29:36 | INFO    | Scored metrics (from mart_health_inputs):
2026-11-05 04:29:36 | INFO    |   [Activity] commit_trend_pct (higher is healthier): Slope of monthly commits over complete months / average monthly commits (fractional change per month; the trend component)
2026-11-05 04:29:36 | INFO    |   [Responsiveness] median_hours_to_first_review (lower is healthier): Median hours from PR open to first review by someone other than the author or a bot
2026-11-05 04:29:36 | INFO    |   [Responsiveness] pct_reviewed_within_7d (higher is healthier): Share of human PRs (opened 7+ days before run time) reviewed within 168 hours
2026-11-05 04:29:36 | INFO    |   [Throughput] merge_rate (higher is healthier): Merged / (merged + closed unmerged) human PRs
2026-11-05 04:29:36 | INFO    |   [Throughput] median_hours_to_merge (lower is healthier): Median hours from PR open to merge (merged human PRs only)
2026-11-05 04:29:36 | INFO    |   [Backlog] pct_open_stale (lower is healthier): Share of open human PRs with no update in 180+ days
2026-11-05 04:29:36 | INFO    |   [Release cadence] releases_per_year (higher is healthier): Published releases in the window / (window days / 365.25)
2026-11-05 04:29:36 | INFO    |   [Release cadence] release_gap_cv (lower is healthier): Coefficient of variation of days between releases (lower = more regular)
2026-11-05 04:29:36 | INFO    |   [Contributor base] top3_merge_share (lower is healthier): Share of merged human PRs authored by the 3 most prolific authors (concentration; a share, so it doesn't grow with project size)
2026-11-05 04:29:36 | INFO    |   [Contributor base] contributor_trend_pct (higher is healthier): Slope of monthly active PR authors / average monthly active PR authors (fractional change per month; a second trend component)
2026-11-05 04:29:36 | INFO    | Raw metric values:
repo                          dask/dask  plotly/plotly.py  scikit-learn/scikit-learn
commit_trend_pct                 -0.075            -0.006                     -0.009
median_hours_to_first_review     11.531            16.293                     12.973
pct_reviewed_within_7d            0.559             0.578                      0.639
merge_rate                        0.874             0.814                      0.693
median_hours_to_merge            12.296            41.648                     36.099
pct_open_stale                    0.214             0.183                      0.342
releases_per_year                15.465            10.193                      5.272
release_gap_cv                    0.943             0.686                      0.658
top3_merge_share                  0.592             0.597                      0.309
contributor_trend_pct             0.031             0.036                      0.021
2026-11-05 04:29:36 | INFO    | Signed z-scores (positive = healthier than the 3-repo average):
repo                          dask/dask  plotly/plotly.py  scikit-learn/scikit-learn
commit_trend_pct                  -1.41             +0.76                      +0.66
median_hours_to_first_review      +1.04             -1.35                      +0.31
pct_reviewed_within_7d            -0.96             -0.42                      +1.38
merge_rate                        +1.07             +0.27                      -1.34
median_hours_to_merge             +1.39             -0.91                      -0.48
pct_open_stale                    +0.47             +0.92                      -1.39
releases_per_year                 +1.24             -0.03                      -1.21
release_gap_cv                    -1.41             +0.59                      +0.81
top3_merge_share                  -0.69             -0.73                      +1.41
contributor_trend_pct             +0.25             +1.08                      -1.33
2026-11-05 04:29:36 | INFO    | Weights: Responsiveness 25%, Contributor base 20%, Throughput 20%, Backlog 15%, Activity 10%, Release cadence 10%
2026-11-05 04:29:36 | INFO    | Health ranking (score = weighted sum of dimension subscores):
                     repo  rank  responsiveness  contributor_base  throughput  backlog  activity  release_cadence  score
                dask/dask     1          +0.039            -0.216      +1.230   +0.471    -1.413           -0.085 +0.133
         plotly/plotly.py     2          -0.885            +0.175      -0.323   +0.919    +0.755           +0.283 -0.009
scikit-learn/scikit-learn     3          +0.847            +0.041      -0.907   -1.390    +0.658           -0.198 -0.124
2026-11-05 04:29:36 | INFO    | Task 5 complete: wrote health_scores (3 rows) and health_metric_scores (30 rows) to packages.duckdb
2026-11-05 04:29:36 | INFO    | Finished in state Completed()
2026-11-05 04:29:41 | INFO    | Saved plots/dimension_profile.png
2026-11-05 04:29:41 | INFO    | Saved plots/monthly_trends.png
2026-11-05 04:29:41 | INFO    | Saved plots/review_vs_merge_bubbles.png
2026-11-05 04:29:41 | INFO    | Saved plots/review_vs_merge_bubbles_pre_oct2026.png
2026-11-05 04:29:41 | INFO    | Saved plots/contributors_vs_merge_ratio.png
2026-11-05 04:29:41 | INFO    | Task 6 complete: 5 plots in plots/
2026-11-05 04:29:41 | INFO    | Finished in state Completed()
2026-11-05 04:29:41 | INFO    | API odometer: 266 GitHub API requests this flow run (81 REST, 185 GraphQL)
2026-11-05 04:29:42 | INFO    | Finished in state Completed()
```

</details>
