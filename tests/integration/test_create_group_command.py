from pathlib import Path

import pytest
import yaml
from click.testing import CliRunner

from dbt_meshify.main import cli
from tests.dbt_project_utils import setup_test_project, teardown_test_project
from tests.unit.test_add_group_and_access_to_model_yml import (
    expected_model_yml_multiple_models_multi_select,
    model_yml_multiple_models,
    model_yml_shared_model,
)
from tests.unit.test_add_group_to_yml import (
    expected_group_yml_existing_groups,
    expected_group_yml_no_group,
    group_yml_empty_file,
    group_yml_existing_groups,
    group_yml_group_predefined,
)

source_path = Path("test-projects/source-hack/src_proj_a")
proj_path = Path("test-projects/source-hack/testing/src_proj_a")


@pytest.fixture
def project():
    setup_test_project(source_path, proj_path)
    yield
    teardown_test_project(proj_path.parent)


@pytest.mark.parametrize(
    "model_yml,select,start_group_yml,end_group_yml",
    [
        (
            model_yml_shared_model,
            "shared_model",
            group_yml_empty_file,
            expected_group_yml_no_group,
        ),
        (
            model_yml_shared_model,
            "shared_model",
            group_yml_existing_groups,
            expected_group_yml_existing_groups,
        ),
        (
            model_yml_shared_model,
            "shared_model",
            group_yml_group_predefined,
            expected_group_yml_no_group,
        ),
        (model_yml_shared_model, "", group_yml_empty_file, expected_group_yml_no_group),
    ],
    ids=["1", "2", "3", "4"],
)
def test_create_group_command(model_yml, select, start_group_yml, end_group_yml, project):
    group_yml_file = proj_path / "models" / "_groups.yml"
    model_yml_file = proj_path / "models" / "_models.yml"

    group_yml_file.parent.mkdir(parents=True, exist_ok=True)
    model_yml_file.parent.mkdir(parents=True, exist_ok=True)

    start_yml_content = yaml.safe_load(model_yml)
    with open(model_yml_file, "w") as f:
        yaml.safe_dump(start_yml_content, f, sort_keys=False)

    start_group_yml_content = yaml.safe_load(start_group_yml)
    with open(group_yml_file, "w") as f:
        yaml.safe_dump(start_group_yml_content, f, sort_keys=False)

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "operation",
            "create-group",
            "test_group",
            "--select",
            select,
            "--project-path",
            str(proj_path),
            "--owner-name",
            "Shaina Fake",
            "--owner-email",
            "fake@example.com",
        ],
    )

    with open(group_yml_file, "r") as f:
        actual = yaml.safe_load(f)

    assert result.exit_code == 0
    assert actual == yaml.safe_load(end_group_yml)


@pytest.mark.parametrize(
    "start_model_yml, end_model_yml, start_group_yml, end_group_yml",
    [
        (
            model_yml_multiple_models,
            expected_model_yml_multiple_models_multi_select,
            group_yml_empty_file,
            expected_group_yml_no_group,
        ),
    ],
    ids=["1"],
)
def test_create_group_command_multi_select(
    start_model_yml, end_model_yml, start_group_yml, end_group_yml, project
):
    group_yml_file = proj_path / "models" / "_groups.yml"
    model_yml_file = proj_path / "models" / "_models.yml"
    other_model_file = proj_path / "models" / "other_model.sql"

    group_yml_file.parent.mkdir(parents=True, exist_ok=True)
    model_yml_file.parent.mkdir(parents=True, exist_ok=True)
    other_model_file.parent.mkdir(parents=True, exist_ok=True)

    start_yml_content = yaml.safe_load(start_model_yml)
    with open(model_yml_file, "w") as f:
        yaml.safe_dump(start_yml_content, f, sort_keys=False)

    start_group_yml_content = yaml.safe_load(start_group_yml)
    with open(group_yml_file, "w") as f:
        yaml.safe_dump(start_group_yml_content, f, sort_keys=False)

    with open(other_model_file, "w") as f:
        f.write("select 1 as test")

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "operation",
            "create-group",
            "test_group",
            "--select",
            "shared_model",
            "other_model",
            "--project-path",
            str(proj_path),
            "--owner-name",
            "Shaina Fake",
            "--owner-email",
            "fake@example.com",
        ],
    )

    with open(group_yml_file, "r") as f:
        actual_group_yml = yaml.safe_load(f)
    with open(model_yml_file, "r") as f:
        actual_model_yml = yaml.safe_load(f)

    assert result.exit_code == 0
    assert actual_group_yml == yaml.safe_load(end_group_yml)
    assert actual_model_yml == yaml.safe_load(end_model_yml)


@pytest.mark.parametrize(
    "name,email,end_group_yml",
    [
        ("Tony Legitman", None, expected_group_yml_no_group),
        (None, "tony@notacop.org", expected_group_yml_no_group),
        ("Tony Legitman", "tony@notacop.org", expected_group_yml_no_group),
    ],
    ids=["1", "2", "3"],
)
def test_group_owner_properties(name, email, end_group_yml, project):
    group_yml_file = proj_path / "models" / "_groups.yml"
    model_yml_file = proj_path / "models" / "_models.yml"

    group_yml_file.parent.mkdir(parents=True, exist_ok=True)
    model_yml_file.parent.mkdir(parents=True, exist_ok=True)

    start_yml_content = yaml.safe_load(model_yml_shared_model)
    with open(model_yml_file, "w") as f:
        yaml.safe_dump(start_yml_content, f, sort_keys=False)

    start_group_yml_content = yaml.safe_load(group_yml_empty_file)
    with open(group_yml_file, "w") as f:
        yaml.safe_dump(start_group_yml_content, f, sort_keys=False)

    args = [
        "operation",
        "create-group",
        "test_group",
        "--select",
        "shared_model",
        "--project-path",
        str(proj_path),
    ]

    if name:
        args += ["--owner-name", name]

    if email:
        args += ["--owner-email", email]

    runner = CliRunner()
    result = runner.invoke(cli, args)

    with open(group_yml_file, "r") as f:
        actual = yaml.safe_load(f)

    assert result.exit_code == 0

    end_group_content = yaml.safe_load(end_group_yml)
    if name:
        end_group_content["groups"][0]["owner"]["name"] = name
    else:
        del end_group_content["groups"][0]["owner"]["name"]

    if email:
        end_group_content["groups"][0]["owner"]["email"] = email
    else:
        del end_group_content["groups"][0]["owner"]["email"]

    assert actual == end_group_content


def test_command_raises_exception_invalid_paths():
    """Verify that proving an invalid project path raises the correct error."""
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "operation",
            "create-group",
            "test_group",
            "--owner-name",
            "Johnny Spells",
            "--select",
            "shared_model",
            "other_model",
            "--project-path",
            "tests",
        ],
    )

    assert result.exit_code != 0
    assert "does not contain a dbt project" in result.stdout
