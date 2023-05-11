import pytest
from dbt.contracts.graph.nodes import Group
from dbt.contracts.graph.unparsed import Owner
from dbt.node_types import NodeType

from dbt_meshify.storage.yaml_editors import DbtMeshYmlEditor
from tests.fixtures import (
    expected_group_yml_no_group,
    group_yml_empty_file,
    group_yml_no_empty_group,
    expected_group_yml_existing_groups,
    group_yml_existing_groups,
    group_yml_group_predefined,
)
from tests.unit import read_yml

meshify = DbtMeshYmlEditor()
model_name = "shared_model"


class TestAddGroupToYML:
    @pytest.fixture
    def owner(self) -> Owner:
        return Owner(name="Shaina Fake", email="fake@example.com")

    @pytest.fixture
    def new_group(self, owner: Owner) -> Group:
        return Group(
            name="test_group",
            owner=owner,
            package_name="test_package",
            original_file_path="fake_path",
            unique_id=f"group.test_package.test_group",
            path="models/fake_path",
            resource_type=NodeType.Group,
        )

    def test_adds_groups_to_empty_file(self, new_group: Group):
        yml_dict = meshify.add_group_to_yml(
            group=new_group, full_yml_dict=read_yml(group_yml_empty_file)
        )
        assert yml_dict == read_yml(expected_group_yml_no_group)

    def test_adds_groups_to_empty_group_list(self, new_group: Group):
        yml_dict = meshify.add_group_to_yml(
            group=new_group, full_yml_dict=read_yml(group_yml_no_empty_group)
        )
        assert yml_dict == read_yml(expected_group_yml_no_group)

    def test_adds_groups_to_existing_list_of_groups(self, new_group: Group):
        yml_dict = meshify.add_group_to_yml(
            group=new_group, full_yml_dict=read_yml(group_yml_existing_groups)
        )
        assert yml_dict == read_yml(expected_group_yml_existing_groups)

    def test_adds_groups_updates_predefined_group(self, new_group: Group):
        yml_dict = meshify.add_group_to_yml(
            group=new_group, full_yml_dict=read_yml(group_yml_group_predefined)
        )
        assert yml_dict == read_yml(expected_group_yml_no_group)
