from pathlib import Path

import pytest
from click.testing import CliRunner

from dbt_meshify.dbt_projects import DbtProject
from dbt_meshify.main import cli
from tests.dbt_project_utils import setup_test_project, teardown_test_project

src_path_string = "test-projects/split/split_proj"
dest_path_string = "test-projects/split/temp_proj"
proj_path = Path(dest_path_string)


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
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "group",
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
    project = DbtProject.from_directory(proj_path, read_catalog=False)
    # ensure that the correct set of public models is created
    public_contracted_models = [
        model.name
        for _, model in project.models.items()
        if model.access == "protected" and model.config.contract.enforced
    ]
    assert public_contracted_models == expected_public_contracted_models
    teardown_test_project(dest_path_string)


def test_command_raises_exception_invalid_paths():
    """Verify that proving an invalid project path raises the correct error."""
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "group",
            "test_group",
            "--owner-name",
            "Teenage Mutant Jinja Turtles",
            "--select",
            "foo",
            "--project-path",
            "tests",
        ],
    )

    assert result.exit_code != 0
    assert "does not contain a dbt project" in result.stdout


def test_read_catalog_group():
    """Verify that proving an invalid project path raises the correct error."""
    setup_test_project(src_path_string, dest_path_string)
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "group",
            "test_group",
            "--owner-name",
            "Teenage Mutant Jinja Turtles",
            "--select",
            "foo",
            "--project-path",
            "tests",
            "--read-catalog",
        ],
    )

    assert result.exit_code != 0
    assert "dbt docs generate" not in result.stdout
    teardown_test_project(dest_path_string)


def test_group_name_group_file():
    """Verify that providing a group yaml path results in a group yml being created at the appropriate path."""
    setup_test_project(src_path_string, dest_path_string)
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "group",
            "test_group",
            "--owner-name",
            "Teenage Mutant Jinja Turtles",
            "--select",
            "+order",
            "--project-path",
            dest_path_string,
            "--group-yml-path",
            str(Path(dest_path_string) / "models" / "test_groups.yml"),
        ],
    )

    assert result.exit_code == 0
    assert (Path(dest_path_string) / "models" / "test_groups.yml").exists()
    teardown_test_project(dest_path_string)


def test_group_raises_exception_ambiguous_group_file():
    """The group command raises an exception if there are multiple group files present and `--group-yml-path` is not specified."""
    setup_test_project(src_path_string, dest_path_string)

    for index in range(2):
        group_yaml = Path(dest_path_string) / "models" / f"groups_{index}.yml"
        with open(group_yaml, "w") as file:
            file.write(f"groups:\n - name: group_{index}\n   owner:\n     email: foo@example.com")

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "group",
            "test_group",
            "--owner-name",
            "Teenage Mutant Jinja Turtles",
            "--select",
            "+order",
            "--project-path",
            dest_path_string,
        ],
    )

    assert result.exit_code != 0
    assert "Unable to pick which group YAML" in result.stdout
    teardown_test_project(dest_path_string)


def test_defining_group_yaml_disambiguates_group_file():
    """The group command will not raise an exception if there are multiple group files present and `--group-yml-path` is specified."""
    setup_test_project(src_path_string, dest_path_string)

    for index in range(2):
        group_yaml = Path(dest_path_string) / "models" / f"groups_{index}.yml"
        with open(group_yaml, "w") as file:
            file.write(f"groups:\n - name: group_{index}\n   owner:\n     email: foo@example.com")

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "group",
            "test_group",
            "--owner-name",
            "Teenage Mutant Jinja Turtles",
            "--select",
            "+order",
            "--project-path",
            dest_path_string,
            "--group-yml-path",
            str(Path(dest_path_string) / "models" / "test_groups.yml"),
        ],
    )

    assert result.exit_code == 0
    assert (Path(dest_path_string) / "models" / "test_groups.yml").exists()
    teardown_test_project(dest_path_string)
