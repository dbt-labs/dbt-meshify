from pathlib import Path

import pytest
import yaml
from click.testing import CliRunner

from dbt_meshify.main import add_contract

from ..fixtures import (
    expected_yml_all_col,
    expected_yml_no_col,
    expected_yml_no_entry,
    expected_yml_one_col,
    expected_yml_other_model,
    model_yml_all_col,
    model_yml_no_col,
    model_yml_one_col,
    model_yml_other_model,
)

proj_path_string = "test-projects/source-hack/src_proj_a"
proj_path = Path(proj_path_string)


@pytest.mark.parametrize(
    "start_yml,end_yml",
    [
        (model_yml_no_col, expected_yml_no_col),
        (model_yml_one_col, expected_yml_one_col),
        (model_yml_all_col, expected_yml_all_col),
        (None, expected_yml_no_entry),
        (model_yml_other_model, expected_yml_other_model),
    ],
    ids=["1", "2", "3", "4", "5"],
)
def test_add_contract_to_yml(start_yml, end_yml):
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
        add_contract, ["--select", "shared_model", "--project-path", proj_path_string]
    )
    assert result.exit_code == 0
    # reset the read path to the default in the logic
    with open(yml_file, "r") as f:
        actual = yaml.safe_load(f)
    yml_file.unlink()
    assert actual == yaml.safe_load(end_yml)
