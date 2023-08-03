from pathlib import Path
from unittest.mock import MagicMock

import pytest
from dbt.contracts.files import FileHash
from dbt.contracts.graph.nodes import ModelNode, NodeType

from dbt_meshify.change import ResourceChange
from dbt_meshify.storage.file_content_editors import (
    DbtMeshFileEditor,
    ResourceFileEditor,
)
from dbt_meshify.utilities.contractor import Contractor

from ..dbt_project_fixtures import shared_model_catalog_entry
from ..sql_and_yml_fixtures import (
    expected_contract_yml_all_col,
    expected_contract_yml_no_col,
    expected_contract_yml_no_entry,
    expected_contract_yml_one_col,
    expected_contract_yml_one_col_one_test,
    expected_contract_yml_other_model,
    model_yml_all_col,
    model_yml_no_col_no_version,
    model_yml_one_col,
    model_yml_one_col_one_test,
    model_yml_other_model,
)
from . import read_yml

meshify = DbtMeshFileEditor()
catalog_entry = shared_model_catalog_entry
model_name = "shared_model"


@pytest.fixture
def project():
    project = MagicMock()
    project.resolve_patch_path.return_value = Path(".")
    project.get_catalog_entry.return_value = shared_model_catalog_entry
    return project


@pytest.fixture
def model() -> ModelNode:
    return ModelNode(
        database=None,
        resource_type=NodeType.Model,
        checksum=FileHash("foo", "foo"),
        schema="foo",
        name=model_name,
        package_name="foo",
        path="models/_models.yml",
        original_file_path=f"models/{model_name}.sql",
        unique_id="model.foo.foo",
        fqn=["foo", "foo"],
        alias="foo",
    )


@pytest.fixture
def change(project, model) -> ResourceChange:
    contractor = Contractor(project=project)
    return contractor.generate_contract(model)


class TestAddContractToYML:
    def test_add_contract_to_yml_no_col(self, change):
        yml_dict = ResourceFileEditor.update_resource(
            read_yml(model_yml_no_col_no_version), change
        )
        assert yml_dict == read_yml(expected_contract_yml_no_col)

    def test_add_contract_to_yml_one_col(self, change):
        yml_dict = ResourceFileEditor.update_resource(read_yml(model_yml_one_col), change)
        assert yml_dict == read_yml(expected_contract_yml_one_col)

    def test_add_contract_to_yml_one_col_one_test(self, change):
        yml_dict = ResourceFileEditor.update_resource(read_yml(model_yml_one_col_one_test), change)
        assert yml_dict == read_yml(expected_contract_yml_one_col_one_test)

    def test_add_contract_to_yml_all_col(self, change):
        yml_dict = ResourceFileEditor.update_resource(read_yml(model_yml_all_col), change)
        assert yml_dict == read_yml(expected_contract_yml_all_col)

    def test_add_contract_to_yml_no_entry(self, change):
        yml_dict = ResourceFileEditor.update_resource({}, change)
        assert yml_dict == read_yml(expected_contract_yml_no_entry)

    def test_add_contract_to_yml_other_model(self, change):
        yml_dict = ResourceFileEditor.update_resource(read_yml(model_yml_other_model), change)
        assert yml_dict == read_yml(expected_contract_yml_other_model)
