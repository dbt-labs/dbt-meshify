from unittest.mock import MagicMock

import pytest
import yaml

from dbt_meshify.storage.file_content_editors import (
    DbtMeshFileEditor,
    ResourceFileEditor,
)
from dbt_meshify.utilities.versioner import ModelVersioner

from ..dbt_project_fixtures import model, project  # noqa: F401
from ..sql_and_yml_fixtures import (
    expected_versioned_model_yml_increment_version_defined_in,
    expected_versioned_model_yml_increment_version_no_prerelease,
    expected_versioned_model_yml_increment_version_with_prerelease,
    expected_versioned_model_yml_no_version,
    expected_versioned_model_yml_no_yml,
    model_yml_increment_version,
    model_yml_no_col_no_version,
)

meshify = DbtMeshFileEditor()
model_name = "shared_model"


@pytest.fixture
def file_manager():
    input = read_yml(model_yml_increment_version)
    file_manager = MagicMock()
    file_manager.read_file.return_value = input
    return file_manager


def read_yml(yml_str):
    return yaml.safe_load(yml_str)


class TestAddContractToYML:
    def test_add_version_to_model_yml_no_yml(self, model, project):  # noqa: F811
        input = {}
        file_manager = MagicMock()
        file_manager.read_file.return_value = input

        versioner = ModelVersioner(project=project, file_manager=file_manager)
        changes = list(versioner.generate_version(model))
        yml_dict = ResourceFileEditor.update_resource(properties=input, change=changes[0])

        assert yml_dict == read_yml(expected_versioned_model_yml_no_yml)

    def test_add_version_to_model_yml_no_version(self, model, project):  # noqa: F811
        input = read_yml(model_yml_no_col_no_version)
        file_manager = MagicMock()
        file_manager.read_file.return_value = input

        versioner = ModelVersioner(project=project, file_manager=file_manager)
        changes = list(versioner.generate_version(model))
        yml_dict = ResourceFileEditor.update_resource(properties=input, change=changes[0])

        assert yml_dict == read_yml(expected_versioned_model_yml_no_version)

    def test_add_version_to_model_yml_increment_version_no_prerelease(
        self, model, project, file_manager  # noqa: F811
    ):
        versioner = ModelVersioner(project=project, file_manager=file_manager)
        changes = list(versioner.generate_version(model))
        yml_dict = ResourceFileEditor.update_resource(
            properties=read_yml(model_yml_increment_version), change=changes[0]
        )

        assert yml_dict == read_yml(expected_versioned_model_yml_increment_version_no_prerelease)

    def test_add_version_to_model_yml_increment_version_with_prerelease(
        self, model, project, file_manager  # noqa: F811
    ):
        versioner = ModelVersioner(project=project, file_manager=file_manager)
        changes = list(versioner.generate_version(model, prerelease=True))
        yml_dict = ResourceFileEditor.update_resource(
            properties=read_yml(model_yml_increment_version), change=changes[0]
        )

        assert yml_dict == read_yml(expected_versioned_model_yml_increment_version_with_prerelease)

    def test_add_version_to_model_yml_increment_version_defined_in(
        self, model, project, file_manager  # noqa: F811
    ):
        versioner = ModelVersioner(project=project, file_manager=file_manager)
        changes = list(versioner.generate_version(model, defined_in="daves_model"))
        yml_dict = ResourceFileEditor.update_resource(
            properties=read_yml(model_yml_increment_version), change=changes[0]
        )

        assert yml_dict == read_yml(expected_versioned_model_yml_increment_version_defined_in)
