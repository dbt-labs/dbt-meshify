from pathlib import Path

import pytest
import yaml
from click.testing import CliRunner

from dbt_meshify.main import cli
from tests.dbt_project_utils import setup_test_project, teardown_test_project

src_project_path = "test-projects/split/split_proj"
dest_project_path = "test-projects/split/temp_proj"
project_path: Path = Path(dest_project_path) / "my_new_project"


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
        assert (project_path / "models" / "marts" / "customers.sql").exists()
        x_proj_ref = "{{ ref('split_proj', 'stg_customers') }}"
        child_sql = (project_path / "models" / "marts" / "customers.sql").read_text()
        assert x_proj_ref in child_sql

        # Copied a referenced docs block
        assert (project_path / "models" / "docs.md").exists()
        assert "customer_id" in (project_path / "models" / "docs.md").read_text()

        # Updated references in a moved macro
        redirect_path = project_path / "macros" / "redirect.sql"
        assert (redirect_path).exists()
        assert "ref('split_proj', 'orders')" in redirect_path.read_text()

        # Starter project assets exist.
        assert (project_path / "analyses").exists()
        assert (project_path / "macros").exists()
        assert (project_path / "seeds").exists()
        assert (project_path / "snapshots").exists()
        assert (project_path / "tests").exists()

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
        assert (project_path / "models" / "staging" / "stg_orders.sql").exists()
        assert (project_path / "models" / "staging" / "__sources.yml").exists()
        source_yml = yaml.safe_load(
            (Path(dest_project_path) / "models" / "staging" / "__sources.yml").read_text()
        )
        new_source_yml = yaml.safe_load(
            (project_path / "models" / "staging" / "__sources.yml").read_text()
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
        assert (project_path / "models" / "staging" / "__sources.yml").exists()
        # copied custom macro
        assert (project_path / "macros" / "cents_to_dollars.sql").exists()

        # Confirm that we did not bring over an unreferenced dollars_to_cents macro
        assert (
            "dollars_to_cents"
            not in (project_path / "macros" / "cents_to_dollars.sql").read_text()
        )

        # copied custom macro parents too!
        assert (project_path / "macros" / "type_numeric.sql").exists()

        # assert only one source moved
        source_yml = yaml.safe_load(
            (Path(dest_project_path) / "models" / "staging" / "__sources.yml").read_text()
        )
        new_source_yml = yaml.safe_load(
            (project_path / "models" / "staging" / "__sources.yml").read_text()
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
        assert (project_path / "models" / "staging" / "stg_orders.sql").exists()
        x_proj_ref = "{{ ref('split_proj', 'stg_order_items') }}"
        child_sql = (project_path / "models" / "marts" / "orders.sql").read_text()
        # split model is updated to reference the parent project
        assert x_proj_ref in child_sql
        # dependencies.yml is moved to subdirectory, not the parent project
        assert (project_path / "dependencies.yml").exists()
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
        assert (project_path / "models" / "marts" / "leaf_node.sql").exists()
        # this is lazy, but this should be the only model in that yml file
        assert "access: public" in (project_path / "models" / "marts" / "__models.yml").read_text()

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
        assert (project_path / "models" / "staging" / "stg_orders.sql").exists()
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
        assert (project_path / "models" / "marts" / "orders.sql").exists()
        x_proj_ref_1 = "{{ ref('split_proj', 'stg_orders') }}"
        x_proj_ref_2 = "{{ ref('split_proj', 'stg_order_items') }}"
        x_proj_ref_3 = "{{ ref('split_proj', 'stg_products') }}"
        x_proj_ref_4 = "{{ ref('split_proj', 'stg_locations') }}"
        x_proj_ref_5 = "{{ ref('split_proj', 'stg_supplies') }}"
        child_sql = (project_path / "models" / "marts" / "orders.sql").read_text()
        # downstream model has all cross project refs updated
        assert x_proj_ref_1 in child_sql
        assert x_proj_ref_2 in child_sql
        assert x_proj_ref_3 in child_sql
        assert x_proj_ref_4 in child_sql
        assert x_proj_ref_5 in child_sql

        teardown_test_project(dest_project_path)

    def test_split_updates_exposure_depends_on(self):
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
                "+orders",
            ],
        )

        assert result.exit_code == 0
        # selected model is moved to subdirectory
        assert (project_path / "models" / "marts" / "orders.sql").exists()
        old_ref = "ref('orders')"
        x_proj_ref = "ref('my_new_project', 'orders')"
        exposure_yml = yaml.safe_load(
            (Path(dest_project_path) / "models" / "marts" / "__exposures.yml").read_text()
        )
        exposure = [
            exposure
            for exposure in exposure_yml["exposures"]
            if exposure["name"] == "crazy_growth_dashboard"
        ][0]
        assert x_proj_ref in exposure["depends_on"]
        assert old_ref not in exposure["depends_on"]

        teardown_test_project(dest_project_path)

    def test_split_updates_semantic_model_model(self):
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
                "+orders",
            ],
        )

        assert result.exit_code == 0
        # selected model is moved to subdirectory
        assert (project_path / "models" / "marts" / "orders.sql").exists()
        old_ref = "ref('orders')"
        x_proj_ref = "ref('my_new_project', 'orders')"
        sm_yml = yaml.safe_load(
            (Path(dest_project_path) / "models" / "marts" / "__semantic_models.yml").read_text()
        )
        sm = [sm for sm in sm_yml["semantic_models"] if sm["name"] == "orders_semantic_model"][0]
        assert x_proj_ref == sm["model"]
        assert old_ref != sm["model"]

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
