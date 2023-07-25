from pathlib import Path

import yaml
from click.testing import CliRunner

from dbt_meshify.main import connect
from tests.dbt_project_utils import setup_test_project, teardown_test_project

producer_project = "test-projects/source-hack/src_proj_a"
source_consumer_project = "test-projects/source-hack/src_proj_b"
package_consumer_project = "test-projects/source-hack/dest_proj_a"


class TestConnectCommand:
    def test_connect_source_hack(self):
        copy_producer_project = producer_project + "_copy"
        copy_source_consumer_project = source_consumer_project + "_copy"
        setup_test_project(producer_project, copy_producer_project)
        setup_test_project(source_consumer_project, copy_source_consumer_project)
        runner = CliRunner()
        result = runner.invoke(
            connect,
            ["--project-paths", copy_producer_project, copy_source_consumer_project],
        )

        assert result.exit_code == 0

        # assert that the source is replaced with a ref
        x_proj_ref = "{{ ref('src_proj_a', 'shared_model') }}"
        source_func = "{{ source('src_proj_a', 'shared_model') }}"
        child_sql = (
            Path(copy_source_consumer_project) / "models" / "downstream_model.sql"
        ).read_text()
        assert x_proj_ref in child_sql
        assert source_func not in child_sql

        # assert that the source was deleted
        # may want to add some nuance in the future for cases where we delete a single source out of a set
        assert not (
            Path(copy_source_consumer_project) / "models" / "staging" / "_sources.yml"
        ).exists()

        # assert that the dependecies yml was created with a pointer to the upstream project
        assert (
            "src_proj_a" in (Path(copy_source_consumer_project) / "dependencies.yml").read_text()
        )

        teardown_test_project(copy_producer_project)
        teardown_test_project(copy_source_consumer_project)

    def test_connect_package(self):
        copy_producer_project = producer_project + "_copy"
        copy_package_consumer_project = package_consumer_project + "_copy"
        setup_test_project(producer_project, copy_producer_project)
        setup_test_project(package_consumer_project, copy_package_consumer_project)
        runner = CliRunner()
        result = runner.invoke(
            connect,
            ["--project-paths", copy_producer_project, copy_package_consumer_project],
        )

        assert result.exit_code == 0

        # assert that the source is replaced with a ref
        x_proj_ref = "{{ ref('src_proj_a', 'shared_model') }}"
        child_sql = (
            Path(copy_package_consumer_project) / "models" / "downstream_model.sql"
        ).read_text()
        assert x_proj_ref in child_sql

        # assert that the dependecies yml was created with a pointer to the upstream project
        assert (
            "src_proj_a" in (Path(copy_package_consumer_project) / "dependencies.yml").read_text()
        )

        teardown_test_project(copy_producer_project)
        teardown_test_project(copy_package_consumer_project)
