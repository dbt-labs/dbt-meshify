import shutil
from pathlib import Path

from dbt_meshify.dbt import Dbt

dbt = Dbt()


def setup_test_project(src_project_path, dest_project_path):
    src_path = Path(src_project_path)
    dest_path = Path(dest_project_path)

    if dest_path.exists():
        shutil.rmtree(dest_path)

    shutil.copytree(src_path, dest_path)
    dbt.invoke(directory=dest_project_path, runner_args=["deps"])
    dbt.invoke(directory=dest_project_path, runner_args=["seed"])
    dbt.invoke(directory=dest_project_path, runner_args=["run"])


def teardown_test_project(dest_project_path):
    dest_path = Path(dest_project_path)
    shutil.rmtree(dest_path)
