from pathlib import Path

import pytest
from click.testing import CliRunner

from dbt_meshify.dbt import Dbt
from dbt_meshify.dbt_projects import DbtProject
from dbt_meshify.main import group

proj_path_string = "test-projects/split/split_proj"
proj_path = Path(proj_path_string)
dbt = Dbt()

# this test should encapsulate the following:
# 1. group is created in the project with proper yml
# 2. public models also have contracts
# since we handle the creation and validation of yml in other tests, this shouldn't need many fixtures


@pytest.mark.parametrize(
    "select,expected_public_contract_models",
    [
        (
            "+orders",
            ["orders"],
        ),
    ],
    ids=["1"],
)
def test_group_command(select, expected_public_contract_models):
    dbt.seed(proj_path)
    dbt.run(proj_path)
    runner = CliRunner()
    result = runner.invoke(
        group,
        [
            "test_group",
            "--select",
            select,
            "--project-path",
            proj_path_string,
        ],
    )
    assert result.exit_code == 0
    project = DbtProject(proj_path)
    # ensure that the correct set of punblic models is created
    public_models = project.select_resources(
        select="group:test_group,config.access:public", output_key="name"
    )
    assert public_models == expected_public_contract_models
    contracted_models = project.select_resources(
        select="group:test_group,config.access:public", output_key="contract"
    )
    assert max([contract["enforced"] for contract in contracted_models]) == True
