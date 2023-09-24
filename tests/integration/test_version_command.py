import shutil
from pathlib import Path

import pytest
import yaml
from click.testing import CliRunner
from loguru import logger

from dbt_meshify.main import cli
from tests.dbt_project_utils import setup_test_project, teardown_test_project

from ..sql_and_yml_fixtures import (
    expected_versioned_model_yml_increment_prerelease_version,
    expected_versioned_model_yml_increment_prerelease_version_with_second_prerelease,
    expected_versioned_model_yml_increment_version_defined_in,
    expected_versioned_model_yml_increment_version_no_prerelease,
    expected_versioned_model_yml_increment_version_with_prerelease,
    expected_versioned_model_yml_no_version,
    expected_versioned_model_yml_no_yml,
    model_yml_increment_version,
    model_yml_no_col_no_version,
    model_yml_string_version,
)

source_path = Path("test-projects/source-hack/src_proj_a")
proj_path = Path("test-projects/source-hack/testing/src_proj_a")


@pytest.fixture
def project():
    setup_test_project(source_path, proj_path)
    yield
    teardown_test_project(proj_path.parent)


@pytest.mark.parametrize(
    "start_yml,end_yml,start_files,expected_files,command_options",
    [
        (
            model_yml_no_col_no_version,
            expected_versioned_model_yml_no_version,
            ["shared_model.sql"],
            ["shared_model_v1.sql"],
            [],
        ),
        (
            None,
            expected_versioned_model_yml_no_yml,
            ["shared_model.sql"],
            ["shared_model_v1.sql"],
            [],
        ),
    ],
    ids=["1", "2"],
)
def test_add_version_version_in_yml(
    start_yml, end_yml, start_files, expected_files, command_options, project
):
    yml_file = proj_path / "models" / "_models.yml"
    yml_file.parent.mkdir(parents=True, exist_ok=True)
    runner = CliRunner()
    # only create file if start_yml is not None
    # in situations where models don't have a patch path, there isn't a yml file to read from
    if start_yml:
        yml_file.touch()
        start_yml_content = yaml.safe_load(start_yml)
        with open(yml_file, "w+") as f:
            yaml.safe_dump(start_yml_content, f, sort_keys=False)
    base_command = [
        "operation",
        "add-version",
        "--select",
        "shared_model",
        "--project-path",
        str(proj_path),
    ]
    base_command.extend(command_options)
    result = runner.invoke(cli, base_command)
    assert result.exit_code == 0
    # reset the read path to the default in the logic
    with open(yml_file, "r") as f:
        actual = yaml.safe_load(f)
    for file in expected_files:
        path = proj_path / "models" / file
        try:
            assert path.is_file()
        except Exception:
            logger.exception(f"File {file} not found")

    assert actual == yaml.safe_load(end_yml)


@pytest.mark.parametrize(
    "start_yml,end_yml,start_files,expected_files,command_options",
    [
        (
            model_yml_increment_version,
            expected_versioned_model_yml_increment_version_no_prerelease,
            ["shared_model.sql"],
            ["shared_model_v1.sql", "shared_model_v2.sql"],
            [],
        ),
        (
            model_yml_increment_version,
            expected_versioned_model_yml_increment_version_with_prerelease,
            ["shared_model.sql"],
            ["shared_model_v1.sql", "shared_model_v2.sql"],
            ["--prerelease"],
        ),
        (
            model_yml_increment_version,
            expected_versioned_model_yml_increment_version_defined_in,
            ["shared_model.sql"],
            ["shared_model_v1.sql", "daves_model.sql"],
            ["--defined-in", "daves_model"],
        ),
        (
            expected_versioned_model_yml_increment_version_with_prerelease,
            expected_versioned_model_yml_increment_prerelease_version_with_second_prerelease,
            ["shared_model_v1.sql", "shared_model_v2.sql"],
            ["shared_model_v1.sql", "shared_model_v2.sql", "shared_model_v3.sql"],
            ["--prerelease"],
        ),
        (
            expected_versioned_model_yml_increment_version_with_prerelease,
            expected_versioned_model_yml_increment_prerelease_version,
            ["shared_model_v1.sql", "shared_model_v2.sql"],
            ["shared_model_v1.sql", "shared_model_v2.sql", "shared_model_v3.sql"],
            [],
        ),
    ],
    ids=["1", "2", "3", "4", "5"],
)
def test_add_version_version_in_yml_fails_when_versions_present(
    start_yml, end_yml, start_files, expected_files, command_options, project
):
    yml_file = proj_path / "models" / "_models.yml"
    yml_file.parent.mkdir(parents=True, exist_ok=True)
    runner = CliRunner()
    # only create file if start_yml is not None
    # in situations where models don't have a patch path, there isn't a yml file to read from
    if start_yml:
        yml_file.touch()
        start_yml_content = yaml.safe_load(start_yml)
        with open(yml_file, "w+") as f:
            yaml.safe_dump(start_yml_content, f, sort_keys=False)
    base_command = [
        "operation",
        "add-version",
        "--select",
        "shared_model",
        "--project-path",
        str(proj_path),
    ]
    base_command.extend(command_options)
    result = runner.invoke(cli, base_command)
    assert result.exit_code != 0


@pytest.mark.parametrize(
    "start_yml,end_yml,start_files,expected_files,command_options",
    [
        (
            model_yml_no_col_no_version,
            expected_versioned_model_yml_no_version,
            ["shared_model.sql"],
            ["shared_model_v1.sql"],
            [],
        ),
        (
            None,
            expected_versioned_model_yml_no_yml,
            ["shared_model.sql"],
            ["shared_model_v1.sql"],
            [],
        ),
    ],
    ids=["1", "2"],
)
def test_bump_version_fails_when_no_versions_present(
    start_yml, end_yml, start_files, expected_files, command_options, project
):
    yml_file = proj_path / "models" / "_models.yml"
    yml_file.parent.mkdir(parents=True, exist_ok=True)
    runner = CliRunner()
    # only create file if start_yml is not None
    # in situations where models don't have a patch path, there isn't a yml file to read from
    if start_yml:
        yml_file.touch()
        start_yml_content = yaml.safe_load(start_yml)
        with open(yml_file, "w+") as f:
            yaml.safe_dump(start_yml_content, f, sort_keys=False)
    base_command = [
        "operation",
        "bump-version",
        "--select",
        "shared_model",
        "--project-path",
        str(proj_path),
    ]
    base_command.extend(command_options)
    result = runner.invoke(cli, base_command)
    assert result.exit_code != 0


@pytest.mark.parametrize(
    "start_yml,end_yml,start_files,expected_files,command_options",
    [
        (
            model_yml_increment_version,
            expected_versioned_model_yml_increment_version_no_prerelease,
            ["shared_model.sql"],
            ["shared_model_v1.sql", "shared_model_v2.sql"],
            [],
        ),
        (
            model_yml_increment_version,
            expected_versioned_model_yml_increment_version_with_prerelease,
            ["shared_model.sql"],
            ["shared_model_v1.sql", "shared_model_v2.sql"],
            ["--prerelease"],
        ),
        (
            model_yml_increment_version,
            expected_versioned_model_yml_increment_version_defined_in,
            ["shared_model.sql"],
            ["shared_model_v1.sql", "daves_model.sql"],
            ["--defined-in", "daves_model"],
        ),
        (
            expected_versioned_model_yml_increment_version_with_prerelease,
            expected_versioned_model_yml_increment_prerelease_version_with_second_prerelease,
            ["shared_model_v1.sql", "shared_model_v2.sql"],
            ["shared_model_v1.sql", "shared_model_v2.sql", "shared_model_v3.sql"],
            ["--prerelease"],
        ),
        (
            expected_versioned_model_yml_increment_version_with_prerelease,
            expected_versioned_model_yml_increment_prerelease_version,
            ["shared_model_v1.sql", "shared_model_v2.sql"],
            ["shared_model_v1.sql", "shared_model_v2.sql", "shared_model_v3.sql"],
            [],
        ),
    ],
    ids=["1", "2", "3", "4", "5"],
)
def test_bump_version_in_yml(
    start_yml, end_yml, start_files, expected_files, command_options, project
):
    yml_file = proj_path / "models" / "_models.yml"
    yml_file.parent.mkdir(parents=True, exist_ok=True)
    runner = CliRunner()
    # only create file if start_yml is not None
    # in situations where models don't have a patch path, there isn't a yml file to read from
    if start_yml:
        yml_file.touch()
        start_yml_content = yaml.safe_load(start_yml)
        with open(yml_file, "w+") as f:
            yaml.safe_dump(start_yml_content, f, sort_keys=False)
    base_command = [
        "operation",
        "bump-version",
        "--select",
        "shared_model",
        "--project-path",
        str(proj_path),
    ]
    base_command.extend(command_options)
    result = runner.invoke(cli, base_command)
    print(result.stdout)
    assert result.exit_code == 0
    # reset the read path to the default in the logic
    with open(yml_file, "r") as f:
        actual = yaml.safe_load(f)
    for file in expected_files:
        path = proj_path / "models" / file
        try:
            assert path.is_file()
        except Exception:
            logger.exception(f"File {file} not found")

    assert actual == yaml.safe_load(end_yml)


@pytest.mark.parametrize(
    "start_yml,end_yml,expected_files,command_options",
    [
        (
            model_yml_increment_version,
            expected_versioned_model_yml_increment_version_with_prerelease,
            ["shared_model_v1.sql", "shared_model_v2.sql"],
            ["--prerelease"],
        )
    ],
)
def test_bump_version_prerelease_in_yml(
    start_yml, end_yml, expected_files, command_options, project
):
    # Create the original versioned model
    shutil.move(
        Path(proj_path / "models" / "shared_model.sql"),
        Path(proj_path / "models" / "shared_model_v1.sql"),
    )
    yml_file = proj_path / "models" / "_models.yml"
    yml_file.parent.mkdir(parents=True, exist_ok=True)
    runner = CliRunner()
    # only create file if start_yml is not None
    # in situations where models don't have a patch path, there isn't a yml file to read from
    if start_yml:
        yml_file.touch()
        start_yml_content = yaml.safe_load(start_yml)
        with open(yml_file, "w+") as f:
            yaml.safe_dump(start_yml_content, f, sort_keys=False)
    base_command = [
        "operation",
        "bump-version",
        "--select",
        "shared_model",
        "--project-path",
        str(proj_path),
    ]
    base_command.extend(command_options)
    result = runner.invoke(cli, base_command)
    print(result.stdout)
    assert result.exit_code == 0
    # reset the read path to the default in the logic
    with open(yml_file, "r") as f:
        actual = yaml.safe_load(f)

    for file in expected_files:
        path = proj_path / "models" / file
        try:
            assert path.is_file()
        except Exception:
            logger.exception(f"File {file} not found")

    assert actual == yaml.safe_load(end_yml)


@pytest.mark.parametrize(
    "start_yml,start_files",
    [
        (
            model_yml_string_version,
            ["shared_model.sql"],
        ),
    ],
    ids=["1"],
)
def test_add_version_to_invalid_yml(start_yml, start_files, project):
    yml_file = proj_path / "models" / "_models.yml"

    yml_file.parent.mkdir(parents=True, exist_ok=True)
    runner = CliRunner()
    # only create file if start_yml is not None
    # in situations where models don't have a patch path, there isn't a yml file to read from
    if start_yml:
        yml_file.touch()
        start_yml_content = yaml.safe_load(start_yml)
        with open(yml_file, "w+") as f:
            yaml.safe_dump(start_yml_content, f, sort_keys=False)
    base_command = [
        "operation",
        "add-version",
        "--select",
        "shared_model",
        "--project-path",
        str(proj_path),
    ]
    result = runner.invoke(cli, base_command, catch_exceptions=True)
    assert result.exit_code == 1


def test_command_raises_exception_invalid_paths():
    """Verify that proving an invalid project path raises the correct error."""
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "operation",
            "add-version",
            "--select",
            "shared_model",
            "--project-path",
            "tests",
        ],
    )

    assert result.exit_code != 0
    assert "does not contain a dbt project" in result.stdout


def test_version_no_model_yaml(project):
    """Verify that the version command running in default mode adds versions and creates a second version, too"""

    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["--debug", "version", "--select", "shared_model", "--project-path", proj_path],
    )

    assert result.exit_code == 0

    assert (proj_path / "models" / "shared_model_v1.sql").exists()
    assert (proj_path / "models" / "shared_model_v2.sql").exists()
