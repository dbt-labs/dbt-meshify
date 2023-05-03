from click.testing import CliRunner
import yaml
from pathlib import Path
from dbt_meshify.main import version
import pytest
from ..fixtures import (
    shared_model_sql,
    model_yml_no_col_no_version,
    expected_versioned_model_yml_no_version,
    model_yml_increment_version,
    expected_versioned_model_yml_increment_version_no_prerelease,
    expected_versioned_model_yml_increment_version_with_prerelease,
    expected_versioned_model_yml_increment_version_defined_in,
    expected_versioned_model_yml_no_yml
)

proj_path_string = "test-projects/source-hack/src_proj_a"
proj_path = Path(proj_path_string)

def reset_project():
    model_path = proj_path / "models" / "shared_model.sql"
    if not model_path.is_file():
        with model_path.open("w+") as f:
            f.write(shared_model_sql)

@pytest.mark.parametrize(
    "start_yml,end_yml,expected_files",
    [
        (model_yml_no_col_no_version, expected_versioned_model_yml_no_version,["shared_model_v1.sql"]),
        (model_yml_increment_version, expected_versioned_model_yml_increment_version_no_prerelease,["shared_model.sql, shared_model_v2.sql"]),
        (model_yml_increment_version, expected_versioned_model_yml_increment_version_with_prerelease,["shared_model.sql", "shared_model_v2.sql"]),
        (model_yml_increment_version, expected_versioned_model_yml_increment_version_defined_in,["shared_model.sql", "daves_model.sql"]),
        (None, expected_versioned_model_yml_no_yml, ["shared_model_v1.sql"]),
    ],
    ids=["1", "2", "3", "4", "5"],
)

def test_add_version_to_yml(start_yml, end_yml, expected_files):
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
    result = runner.invoke(
        version, ["--select", "shared_model", "--project-path", proj_path_string]
    )
    # import pdb; pdb.set_trace()
    assert result.exit_code == 0
    # reset the read path to the default in the logic
    with open(yml_file, "r") as f:
        actual = yaml.safe_load(f)
    for file in expected_files:
        path = proj_path / "models" / file
        try:
            assert path.is_file()
        except:
            print(f"File {file} not found")
        path.unlink()
    yml_file.unlink()
    reset_project()
    assert actual == yaml.safe_load(end_yml)
