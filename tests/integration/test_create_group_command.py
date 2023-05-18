from pathlib import Path

import pytest
import yaml
from click.testing import CliRunner

from dbt_meshify.main import create_group
from tests.unit.test_add_group_and_access_to_model_yml import model_yml_shared_model
from tests.unit.test_add_group_to_yml import (
    expected_group_yml_existing_groups,
    expected_group_yml_no_group,
    group_yml_empty_file,
    group_yml_existing_groups,
    group_yml_group_predefined,
)

proj_path_string = "test-projects/source-hack/src_proj_a"
proj_path = Path(proj_path_string)


@pytest.mark.parametrize(
    "model_yml,start_group_yml,end_group_yml",
    [
        (model_yml_shared_model, group_yml_empty_file, expected_group_yml_no_group),
        (model_yml_shared_model, group_yml_existing_groups, expected_group_yml_existing_groups),
        (model_yml_shared_model, group_yml_group_predefined, expected_group_yml_no_group),
    ],
    ids=["1", "2", "3"],
)
def test_create_group_command(model_yml, start_group_yml, end_group_yml):
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
        create_group,
        [
            "test_group",
            "--select",
            "shared_model",
            "--project-path",
            proj_path_string,
            "--owner",
            "name",
            "Shaina Fake",
            "--owner",
            "email",
            "fake@example.com",
        ],
    )

    with open(group_yml_file, "r") as f:
        actual = yaml.safe_load(f)

    group_yml_file.unlink()
    model_yml_file.unlink()

    assert result.exit_code == 0
    assert actual == yaml.safe_load(end_group_yml)


@pytest.mark.parametrize(
    "name,email,end_group_yml",
    [
        ("Tony Legitman", None, expected_group_yml_no_group),
        (None, "tony@notacop.org", expected_group_yml_no_group),
        ("Tony Legitman", "tony@notacop.org", expected_group_yml_no_group),
    ],
    ids=["1", "2", "3"],
)
def test_group_group_owner_properties(name, email, end_group_yml):
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

    args = ["test_group", "--select", "shared_model", "--project-path", proj_path_string]

    if name:
        args += ["--owner", "name", name]

    if email:
        args += ["--owner", "email", email]

    runner = CliRunner()
    result = runner.invoke(create_group, args)

    with open(group_yml_file, "r") as f:
        actual = yaml.safe_load(f)

    group_yml_file.unlink()
    model_yml_file.unlink()

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
