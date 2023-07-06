from pathlib import Path

import pytest
from click.testing import CliRunner

from dbt_meshify.dbt import Dbt
from dbt_meshify.dbt_projects import DbtProject
from dbt_meshify.main import group
from tests.dbt_project_utils import setup_test_project, teardown_test_project

src_path_string = "test-projects/split/split_proj"
dest_path_string = "test-projects/split/temp_proj"
proj_path = Path(dest_path_string)
dbt = Dbt()

# this test should encapsulate the following:
# 1. group is created in the project with proper yml
# 2. public models also have contracts
# since we handle the creation and validation of yml in other tests, this shouldn't need many fixtures


@pytest.mark.parametrize(
    "select,expected_public_contracted_models",
    [
        (
            "+orders",
            ["orders"],
        ),
    ],
    ids=["1"],
)
def test_group_command(select, expected_public_contracted_models):
    setup_test_project(src_path_string, dest_path_string)
    dbt.invoke(directory=proj_path, runner_args=["deps"])
    dbt.invoke(directory=proj_path, runner_args=["seed"])
    dbt.invoke(directory=proj_path, runner_args=["run"])
    runner = CliRunner()
    result = runner.invoke(
        group,
        [
            "test_group",
            "--owner-name",
            "Teenage Mutant Jinja Turtles",
            "--select",
            select,
            "--project-path",
            dest_path_string,
        ],
    )
    assert result.exit_code == 0
    project = DbtProject.from_directory(proj_path)
    # ensure that the correct set of public models is created
    public_contracted_models = [
        model.name
        for model_key, model in project.models.items()
        if model.access == "public" and model.config.contract.enforced
    ]
    assert public_contracted_models == expected_public_contracted_models
    teardown_test_project(dest_path_string)
