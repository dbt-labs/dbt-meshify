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
                "customers",
            ],
        )

        assert result.exit_code == 0
        assert (
            Path(dest_project_path) / "my_new_project" / "models" / "marts" / "customers.sql"
        ).exists()
        x_proj_ref = "{{ ref('split_proj', 'stg_customers') }}"
        child_sql = (
            Path(dest_project_path) / "my_new_project" / "models" / "marts" / "customers.sql"
        ).read_text()
        assert x_proj_ref in child_sql

        # Copied a referenced docs block
        assert (Path(dest_project_path) / "my_new_project" / "models" / "docs.md").exists()
        assert (
            "customer_id"
            in (Path(dest_project_path) / "my_new_project" / "models" / "docs.md").read_text()
        )

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

        # Confirm that we did not bring over an unreferenced dollars_to_cents macro
        assert (
            "dollars_to_cents"
            not in (
                Path(dest_project_path) / "my_new_project" / "macros" / "cents_to_dollars.sql"
            ).read_text()
        )

        # copied custom macro parents too!
        assert (
            Path(dest_project_path) / "my_new_project" / "macros" / "type_numeric.sql"
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
                "+stg_orders",
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

    def test_split_child_project(self):
        """
        Test that splitting out a project downstream of the base project splits as expected
        """
        setup_test_project(src_project_path, dest_project_path)
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "split",
                "my_new_project",
                "--project-path",
                dest_project_path,
                "--select",
                "+stg_orders+",
            ],
        )

        assert result.exit_code == 0
        # selected model is moved to subdirectory
        assert (
            Path(dest_project_path) / "my_new_project" / "models" / "staging" / "stg_orders.sql"
        ).exists()
        x_proj_ref = "{{ ref('split_proj', 'stg_order_items') }}"
        child_sql = (
            Path(dest_project_path) / "my_new_project" / "models" / "marts" / "orders.sql"
        ).read_text()
        # split model is updated to reference the parent project
        assert x_proj_ref in child_sql
        # dependencies.yml is moved to subdirectory, not the parent project
        assert (Path(dest_project_path) / "my_new_project" / "dependencies.yml").exists()
        assert not (Path(dest_project_path) / "dependencies.yml").exists()
        teardown_test_project(dest_project_path)

    def test_split_project_cycle(self):
        """
        Test that splitting out a project downstream of the base project splits as expected
        """
        setup_test_project(src_project_path, dest_project_path)
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "split",
                "my_new_project",
                "--project-path",
                dest_project_path,
                "--select",
                "orders",
            ],
        )

        assert result.exit_code == 1

        teardown_test_project(dest_project_path)

    def test_split_public_leaf_nodes(self, project):
        """
        asserts that the split command will split out a model that is a leaf node mark it as public
        """
        setup_test_project(src_project_path, dest_project_path)
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "split",
                "my_new_project",
                "--project-path",
                dest_project_path,
                "--select",
                "+leaf_node",
            ],
        )

        assert result.exit_code == 0
        assert (
            Path(dest_project_path) / "my_new_project" / "models" / "marts" / "leaf_node.sql"
        ).exists()
        # this is lazy, but this should be the only model in that yml file
        assert (
            "access: public"
            in (
                Path(dest_project_path) / "my_new_project" / "models" / "marts" / "__models.yml"
            ).read_text()
        )

    def test_split_upstream_multiple_boundary_parents(self):
        """
        Test that splitting out a project downstream of the base project splits as expected
        """
        setup_test_project(src_project_path, dest_project_path)
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
                "+stg_order_items",
            ],
        )

        assert result.exit_code == 0
        # selected model is moved to subdirectory
        assert (
            Path(dest_project_path) / "my_new_project" / "models" / "staging" / "stg_orders.sql"
        ).exists()
        x_proj_ref_1 = "{{ ref('my_new_project', 'stg_orders') }}"
        x_proj_ref_2 = "{{ ref('my_new_project', 'stg_order_items') }}"
        child_sql = (Path(dest_project_path) / "models" / "marts" / "orders.sql").read_text()
        # downstream model has all cross project refs updated
        assert x_proj_ref_1 in child_sql
        assert x_proj_ref_2 in child_sql

        teardown_test_project(dest_project_path)

    def test_split_downstream_multiple_boundary_parents(self):
        """
        Test that splitting out a project downstream of the base project splits as expected
        """
        setup_test_project(src_project_path, dest_project_path)
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "split",
                "my_new_project",
                "--project-path",
                dest_project_path,
                "--select",
                "orders+",
            ],
        )

        assert result.exit_code == 0
        # selected model is moved to subdirectory
        assert (
            Path(dest_project_path) / "my_new_project" / "models" / "marts" / "orders.sql"
        ).exists()
        x_proj_ref_1 = "{{ ref('split_proj', 'stg_orders') }}"
        x_proj_ref_2 = "{{ ref('split_proj', 'stg_order_items') }}"
        x_proj_ref_3 = "{{ ref('split_proj', 'stg_products') }}"
        x_proj_ref_4 = "{{ ref('split_proj', 'stg_locations') }}"
        x_proj_ref_5 = "{{ ref('split_proj', 'stg_supplies') }}"
        child_sql = (
            Path(dest_project_path) / "my_new_project" / "models" / "marts" / "orders.sql"
        ).read_text()
        # downstream model has all cross project refs updated
        assert x_proj_ref_1 in child_sql
        assert x_proj_ref_2 in child_sql
        assert x_proj_ref_3 in child_sql
        assert x_proj_ref_4 in child_sql
        assert x_proj_ref_5 in child_sql

        teardown_test_project(dest_project_path)


def test_command_raises_exception_invalid_paths():
    """Verify that proving an invalid project path raises the correct error."""
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "split",
            "my_new_project",
            "--project-path",
            "tests",
            "--select",
            "orders+",
        ],
    )

    assert result.exit_code != 0
    assert "does not contain a dbt project" in result.stdout
