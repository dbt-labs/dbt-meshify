from pathlib import Path

import pytest
import yaml
from click.testing import CliRunner

from dbt_meshify.dbt import Dbt
from dbt_meshify.main import cli
from tests.dbt_project_utils import setup_test_project, teardown_test_project

from ..sql_and_yml_fixtures import (
    expected_contract_yml_all_col,
    expected_contract_yml_no_col,
    expected_contract_yml_no_entry,
    expected_contract_yml_one_col,
    expected_contract_yml_other_model,
    expected_contract_yml_sequential,
    model_yml_all_col,
    model_yml_no_col_no_version,
    model_yml_one_col,
    model_yml_other_model,
)

# TODO: Make a copy before executing!!!1!
proj_path = Path("test-projects/source-hack/src_proj_a")
dest_project = Path("test-projects/source-hack/testing/src_proj_a")
dbt = Dbt()


@pytest.fixture
def project():
    setup_test_project(proj_path, dest_project)
    yield
    teardown_test_project(dest_project.parent)


@pytest.mark.parametrize(
    "start_yml,end_yml",
    [
        (model_yml_no_col_no_version, expected_contract_yml_no_col),
        (model_yml_one_col, expected_contract_yml_one_col),
        (model_yml_all_col, expected_contract_yml_all_col),
        (None, expected_contract_yml_no_entry),
        (model_yml_other_model, expected_contract_yml_other_model),
    ],
    ids=["1", "2", "3", "4", "5"],
)
def test_add_contract_to_yml(start_yml, end_yml, project):
    yml_file = dest_project / "models" / "_models.yml"
    yml_file.parent.mkdir(parents=True, exist_ok=True)
    runner = CliRunner()
    # do a dbt run to create the duckdb
    # and enable the docs generate command to work
    dbt.invoke(directory=dest_project, runner_args=["run"])
    # only create file if start_yml is not None
    # in situations where models don't have a patch path, there isn't a yml file to read from
    if start_yml:
        yml_file.touch()
        start_yml_content = yaml.safe_load(start_yml)
        with open(yml_file, "w+") as f:
            yaml.safe_dump(start_yml_content, f, sort_keys=False)
    result = runner.invoke(
        cli,
        [
            "operation",
            "add-contract",
            "--select",
            "shared_model",
            "--project-path",
            str(dest_project),
        ],
    )
    assert result.exit_code == 0
    # reset the read path to the default in the logic
    with open(yml_file, "r") as f:
        actual = yaml.safe_load(f)

    assert actual == yaml.safe_load(end_yml)


@pytest.mark.parametrize(
    "start_yml,end_yml",
    [
        (None, expected_contract_yml_sequential),
    ],
    ids=["1"],
)
def test_add_sequential_contracts_to_yml(start_yml, end_yml, project):
    yml_file = dest_project / "models" / "_models.yml"
    runner = CliRunner()
    # do a dbt run to create the duckdb
    # and enable the docs generate command to work
    dbt.invoke(directory=dest_project, runner_args=["run"])

    result = runner.invoke(
        cli,
        [
            "operation",
            "add-contract",
            "--select",
            "shared_model",
            "--project-path",
            str(dest_project),
        ],
    )

    assert result.exit_code == 0
    result2 = runner.invoke(
        cli,
        [
            "operation",
            "add-contract",
            "--select",
            "new_model",
            "--project-path",
            str(dest_project),
        ],
    )
    assert result2.exit_code == 0
    # reset the read path to the default in the logic
    with open(yml_file, "r") as f:
        actual = yaml.safe_load(f)

    assert actual == yaml.safe_load(end_yml)


@pytest.mark.parametrize(
    "start_yml,end_yml,read_catalog",
    [
        (model_yml_no_col_no_version, expected_contract_yml_no_col, True),
        (model_yml_no_col_no_version, expected_contract_yml_no_col, False),
    ],
    ids=["1", "2"],
)
def test_add_contract_read_catalog(start_yml, end_yml, read_catalog, caplog, project):
    yml_file = dest_project / "models" / "_models.yml"
    yml_file.parent.mkdir(parents=True, exist_ok=True)
    runner = CliRunner()
    # do a dbt run to create the duckdb
    # and enable the docs generate command to work
    dbt.invoke(directory=dest_project, runner_args=["run"])
    # only create file if start_yml is not None
    # in situations where models don't have a patch path, there isn't a yml file to read from
    if start_yml:
        yml_file.touch()
        start_yml_content = yaml.safe_load(start_yml)
        with open(yml_file, "w+") as f:
            yaml.safe_dump(start_yml_content, f, sort_keys=False)
    args = [
        "operation",
        "add-contract",
        "--select",
        "shared_model",
        "--project-path",
        str(dest_project),
    ]
    if read_catalog:
        args.append("--read-catalog")
    result = runner.invoke(cli, args)

    assert result.exit_code == 0
    # reset the read path to the default in the logic
    with open(yml_file, "r") as f:
        actual = yaml.safe_load(f)

    if read_catalog:
        assert "Reading catalog from" in caplog.text
        assert "Generating catalog with dbt docs generate..." not in caplog.text
    else:
        assert "Reading catalog from" not in caplog.text
        assert "Generating catalog with dbt docs generate..." in caplog.text

    assert actual == yaml.safe_load(end_yml)
