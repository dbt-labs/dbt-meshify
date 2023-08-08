from pathlib import Path

import pytest
import yaml
from click.testing import CliRunner

from dbt_meshify.main import cli
from tests.dbt_project_utils import setup_test_project, teardown_test_project

src_project_path = "test-projects/split/split_proj"
dest_project_path = "test-projects/split/temp_proj"


@pytest.fixture
def project():
    setup_test_project(src_project_path, dest_project_path)
    yield
    teardown_test_project(dest_project_path)


class TestSplitCommand:
    def test_split_one_model(self, project):
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "split",
                "my_new_project",
                "--project-path",
                dest_project_path,
                "--select",
                "stg_orders",
            ],
        )

        assert result.exit_code == 0
        assert (
            Path(dest_project_path) / "my_new_project" / "models" / "staging" / "stg_orders.sql"
        ).exists()
        x_proj_ref = "{{ ref('my_new_project', 'stg_orders') }}"
        child_sql = (Path(dest_project_path) / "models" / "marts" / "orders.sql").read_text()
        assert x_proj_ref in child_sql

    def test_split_one_model_one_source(self, project):
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "split",
                "my_new_project",
                "--project-path",
                dest_project_path,
                "--select",
                "+stg_orders",
            ],
        )

        assert result.exit_code == 0
        assert (
            Path(dest_project_path) / "my_new_project" / "models" / "staging" / "stg_orders.sql"
        ).exists()
        assert (
            Path(dest_project_path) / "my_new_project" / "models" / "staging" / "__sources.yml"
        ).exists()
        source_yml = yaml.safe_load(
            (Path(dest_project_path) / "models" / "staging" / "__sources.yml").read_text()
        )
        new_source_yml = yaml.safe_load(
            (
                Path(dest_project_path) / "my_new_project" / "models" / "staging" / "__sources.yml"
            ).read_text()
        )

        assert len(source_yml["sources"][0]["tables"]) == 5
        assert len(new_source_yml["sources"][0]["tables"]) == 1

        x_proj_ref = "{{ ref('my_new_project', 'stg_orders') }}"
        child_sql = (Path(dest_project_path) / "models" / "marts" / "orders.sql").read_text()
        assert x_proj_ref in child_sql

    def test_split_one_model_one_source_custom_macro(self, project):
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "split",
                "my_new_project",
                "--project-path",
                dest_project_path,
                "--select",
                "+stg_order_items",
            ],
        )

        assert result.exit_code == 0
        # moved model
        assert (
            Path(dest_project_path)
            / "my_new_project"
            / "models"
            / "staging"
            / "stg_order_items.sql"
        ).exists()
        # moved source
        assert (
            Path(dest_project_path) / "my_new_project" / "models" / "staging" / "__sources.yml"
        ).exists()
        # copied custom macro
        assert (
            Path(dest_project_path) / "my_new_project" / "macros" / "cents_to_dollars.sql"
        ).exists()

        # assert only one source moved
        source_yml = yaml.safe_load(
            (Path(dest_project_path) / "models" / "staging" / "__sources.yml").read_text()
        )
        new_source_yml = yaml.safe_load(
            (
                Path(dest_project_path) / "my_new_project" / "models" / "staging" / "__sources.yml"
            ).read_text()
        )
        assert len(source_yml["sources"][0]["tables"]) == 5
        assert len(new_source_yml["sources"][0]["tables"]) == 1

        # assert cross project ref in child model
        x_proj_ref = "{{ ref('my_new_project', 'stg_order_items') }}"
        child_sql = (Path(dest_project_path) / "models" / "marts" / "orders.sql").read_text()
        assert x_proj_ref in child_sql

    def test_split_one_model_create_path(self, project):
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "split",
                "my_new_project",
                "--project-path",
                dest_project_path,
                "--select",
                "stg_orders",
                "--create-path",
                "test-projects/split/ham_sandwich",
            ],
        )

        assert result.exit_code == 0
        assert (
            Path("test-projects/split/ham_sandwich") / "models" / "staging" / "stg_orders.sql"
        ).exists()
        x_proj_ref = "{{ ref('my_new_project', 'stg_orders') }}"
        child_sql = (Path(dest_project_path) / "models" / "marts" / "orders.sql").read_text()
        assert x_proj_ref in child_sql

        teardown_test_project("test-projects/split/ham_sandwich")
