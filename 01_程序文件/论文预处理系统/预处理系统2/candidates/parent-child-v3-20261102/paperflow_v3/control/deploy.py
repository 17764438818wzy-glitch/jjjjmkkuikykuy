"""Create the two candidate deployments when explicitly invoked by an operator."""

import argparse
import os
from pathlib import Path

from prefect.deployments.runner import RunnerDeployment

from .flows import paper_order, parent_order


def create_deployments(work_pool):
    root = str(Path(__file__).resolve().parents[2])
    parent = RunnerDeployment.from_flow(parent_order, name="parent", work_pool_name=work_pool,
                                        job_variables={"working_dir": root}).apply(
        work_pool_name=work_pool)
    paper = RunnerDeployment.from_flow(paper_order, name="paper", work_pool_name=work_pool,
                                       concurrency_limit=None,
                                       job_variables={"working_dir": root}).apply(
        work_pool_name=work_pool)
    return parent, paper


def main():
    parser = argparse.ArgumentParser(description="Create candidate Paperflow v3 Prefect deployments")
    parser.add_argument("--pool", required=True, help="existing Prefect process work pool")
    args = parser.parse_args()
    os.chdir(Path(__file__).resolve().parents[2])
    parent, paper = create_deployments(args.pool)
    print("PAPERFLOW_V3_PARENT_DEPLOYMENT_ID=" + str(parent))
    print("PAPERFLOW_V3_CHILD_DEPLOYMENT_ID=" + str(paper))


if __name__ == "__main__":
    main()
