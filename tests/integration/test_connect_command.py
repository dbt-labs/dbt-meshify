from pathlib import Path

import pytest
from click.testing import CliRunner

from dbt_meshify.main import connect
from tests.dbt_project_utils import setup_test_project, teardown_test_project

producer_project_path = "test-projects/source-hack/src_proj_a"
source_consumer_project_path = "test-projects/source-hack/src_proj_b"
package_consumer_project_path = "test-projects/source-hack/dest_proj_a"
copy_producer_project_path = producer_project_path + "_copy"
copy_source_consumer_project_path = source_consumer_project_path + "_copy"
copy_package_consumer_project_path = package_consumer_project_path + "_copy"


@pytest.fixture
def producer_project():
    setup_test_project(producer_project_path, copy_producer_project_path)
    setup_test_project(source_consumer_project_path, copy_source_consumer_project_path)
    setup_test_project(package_consumer_project_path, copy_package_consumer_project_path)
    # yield to the test. We'll come back here after the test returns.
    yield

    teardown_test_project(copy_producer_project_path)
    teardown_test_project(copy_source_consumer_project_path)
    teardown_test_project(copy_package_consumer_project_path)


class TestConnectCommand:
    def test_connect_source_hack(self, producer_project):
        runner = CliRunner()
        result = runner.invoke(
            connect,
            ["--project-paths", copy_producer_project_path, copy_source_consumer_project_path],
        )

        assert result.exit_code == 0

        # assert that the source is replaced with a ref
        x_proj_ref = "{{ ref('src_proj_a', 'shared_model') }}"
        source_func = "{{ source('src_proj_a', 'shared_model') }}"
        child_sql = (
            Path(copy_source_consumer_project_path) / "models" / "downstream_model.sql"
        ).read_text()
        assert x_proj_ref in child_sql
        assert source_func not in child_sql

        # assert that the source was deleted
        # may want to add some nuance in the future for cases where we delete a single source out of a set
        assert not (
            Path(copy_source_consumer_project_path) / "models" / "staging" / "_sources.yml"
        ).exists()

        # assert that the dependecies yml was created with a pointer to the upstream project
        assert (
            "src_proj_a"
            in (Path(copy_source_consumer_project_path) / "dependencies.yml").read_text()
        )

    def test_connect_package(self, producer_project):
        runner = CliRunner()
        result = runner.invoke(
            connect,
            ["--project-paths", copy_producer_project_path, copy_package_consumer_project_path],
        )

        assert result.exit_code == 0

        # assert that the source is replaced with a ref
        x_proj_ref = "{{ ref('src_proj_a', 'shared_model') }}"
        child_sql = (
            Path(copy_package_consumer_project_path) / "models" / "downstream_model.sql"
        ).read_text()
        assert x_proj_ref in child_sql

        # assert that the dependecies yml was created with a pointer to the upstream project
        assert (
            "src_proj_a"
            in (Path(copy_package_consumer_project_path) / "dependencies.yml").read_text()
        )

    def test_packages_dir_exclude_packages(self):
        # copy all three packages into a subdirectory to ensure no repeat project names in one directory
        subdir_producer_project = "test-projects/source-hack/subdir/src_proj_a"
        subdir_source_consumer_project = "test-projects/source-hack/subdir/src_proj_b"
        subdir_package_consumer_project = "test-projects/source-hack/subdir/dest_proj_a"
        setup_test_project(producer_project_path, subdir_producer_project)
        setup_test_project(source_consumer_project_path, subdir_source_consumer_project)
        setup_test_project(package_consumer_project_path, subdir_package_consumer_project)
        runner = CliRunner()
        result = runner.invoke(
            connect,
            [
                "--projects-dir",
                "test-projects/source-hack/subdir",
                "--exclude-projects",
                "src_proj_b",
            ],
        )

        assert result.exit_code == 0

        # assert that the source is replaced with a ref
        x_proj_ref = "{{ ref('src_proj_a', 'shared_model') }}"
        child_sql = (
            Path(subdir_package_consumer_project) / "models" / "downstream_model.sql"
        ).read_text()
        assert x_proj_ref in child_sql

        # assert that the dependecies yml was created with a pointer to the upstream project
        assert (
            "src_proj_a"
            in (Path(subdir_package_consumer_project) / "dependencies.yml").read_text()
        )

        teardown_test_project(subdir_producer_project)
        teardown_test_project(subdir_package_consumer_project)
        teardown_test_project(subdir_source_consumer_project)
