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
- **Job variables.** `job_variables` keeps the CPU and memory values above unchanged. It adds `env.EXTRA_PIP_PACKAGES` (the pinned packages from `requirements.txt`) so the pod installs dbt, duckdb, pandas, etc. at startup, and it keeps the pool's default `HOME=/home/prefect`.
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

Triggered with the **Run** button in the Prefect UI (https://sds-prefect.pods.uvarc.io/v2/deployments).

```
TODO: paste the flow-run log here (Prefect UI → flow run → Logs, or `prefect flow-run logs <flow-run-id>`)
```
