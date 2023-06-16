import yaml
from dbt.node_types import NodeType

from dbt_meshify.storage.yaml_editors import DbtMeshYmlEditor

from ..fixtures import (
    expeceted_remainder_yml__source_yml_multiple_tables,
    expected_remainder_yml__model_yml_other_model,
    expected_remove_model_yml__model_yml_no_col_no_version,
    expected_remove_model_yml__model_yml_one_col,
    expected_remove_model_yml__model_yml_other_model,
    expected_remove_source_yml__default,
    model_yml_no_col_no_version,
    model_yml_one_col,
    model_yml_other_model,
    source_yml_multiple_tables,
    source_yml_one_table,
)

meshify = DbtMeshYmlEditor()
model_name = "shared_model"
source_name = "test_source"
source_table_name = "table"


def read_yml(yml_str):
    return yaml.safe_load(yml_str)


class TestRemoveResourceYml:
    def test_remove_model_yml_simple(self):
        resource_yml, full_yml = meshify.get_yml_entry(
            resource_name=model_name,
            full_yml=read_yml(model_yml_no_col_no_version),
            resource_type=NodeType.Model,
            source_name=None,
        )
        assert resource_yml == read_yml(expected_remove_model_yml__model_yml_no_col_no_version)
        assert full_yml == None

    def test_remove_model_yml_simple_with_description(self):
        resource_yml, full_yml = meshify.get_yml_entry(
            resource_name=model_name,
            full_yml=read_yml(model_yml_one_col),
            resource_type=NodeType.Model,
            source_name=None,
        )
        assert resource_yml == read_yml(expected_remove_model_yml__model_yml_one_col)
        assert full_yml == None

    def test_remove_model_yml_other_model(self):
        resource_yml, full_yml = meshify.get_yml_entry(
            resource_name=model_name,
            full_yml=read_yml(model_yml_other_model),
            resource_type=NodeType.Model,
            source_name=None,
        )
        assert resource_yml == read_yml(expected_remove_model_yml__model_yml_other_model)
        assert full_yml == read_yml(expected_remainder_yml__model_yml_other_model)

    def test_remove_source_yml_one_table(self):
        resource_yml, full_yml = meshify.get_yml_entry(
            resource_name=source_table_name,
            full_yml=read_yml(source_yml_one_table),
            resource_type=NodeType.Source,
            source_name=source_name,
        )
        assert resource_yml == read_yml(expected_remove_source_yml__default)
        assert full_yml == None

    def test_remove_source_yml_multiple_table(self):
        resource_yml, full_yml = meshify.get_yml_entry(
            resource_name=source_table_name,
            full_yml=read_yml(source_yml_multiple_tables),
            resource_type=NodeType.Source,
            source_name=source_name,
        )
        assert resource_yml == read_yml(expected_remove_source_yml__default)
        assert full_yml == read_yml(expeceted_remainder_yml__source_yml_multiple_tables)
