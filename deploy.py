"""Publish the package-health flow as a Prefect deployment on the SDS Prefect server.

Run once from the project root (after pushing the code to GitHub):

    python deploy.py

The worker pod clones this (private) repo using the PAT stored in the Prefect Secret
block below, and the flow reads the same block for its GitHub API calls (main.py falls
back to it when no TOKEN environment variable exists). The schedule runs the flow at
midnight Eastern on the 28th of every month.
"""
from prefect.blocks.system import Secret
from prefect.runner.storage import GitRepository
from prefect.schedules import Cron

from main import main, SECRET_BLOCK

GIT_URL = "https://github.com/fortunecookie040/DS_3022_Prefect_Project.git"   # private repo
WORK_POOL = "uvasds-k8s-pool"
DEPLOYMENT_NAME = "qtr7bs-deployment"

# No extra packages are installed in the pod: the class worker image
# (ghcr.io/uvads/prefect-k8s-worker) already ships prefect, dbt, duckdb, pandas, etc. in a
# read-only site-packages, so pip-installing pinned versions there fails.

if __name__ == "__main__":
    main.from_source(
        source=GitRepository(
            url=GIT_URL,
            branch="main",
            credentials={"access_token": Secret.load(SECRET_BLOCK)},   # a reference, not the value
        ),
        entrypoint="main.py:main",
    ).deploy(
        name=DEPLOYMENT_NAME,
        work_pool_name=WORK_POOL,
        schedule=Cron("0 0 28 * *", timezone="America/New_York"),
        parameters={"ingest_mode": "graphql"},
        job_variables={
            "cpu_request": "2",
            "cpu_limit": "2",
            "memory_request": "2Gi",
            "memory_limit": "4Gi",
        },
    )
