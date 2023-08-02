import pathlib

import pytest
from dbt.contracts.graph.nodes import Group
from dbt.contracts.graph.unparsed import Owner
from dbt.node_types import NodeType

from dbt_meshify.change import Change, EntityType, Operation, ResourceChange
from dbt_meshify.storage.file_content_editors import (
    DbtMeshFileEditor,
    ResourceFileEditor,
)
from tests.unit import read_yml

meshify = DbtMeshFileEditor()

group_yml_empty_file = """"""

expected_group_yml_no_group = """
groups:
  - name: test_group
    owner:
      name: Shaina Fake
      email: fake@example.com
"""

group_yml_existing_groups = """
groups:
  - name: other_group
    owner:
      name: Ted Real
      email: real@example.com
"""

expected_group_yml_existing_groups = """
groups:
  - name: other_group
    owner:
      name: Ted Real
      email: real@example.com
  - name: test_group
    owner:
      name: Shaina Fake
      email: fake@example.com
"""

group_yml_group_predefined = """
groups:
  - name: test_group
    owner:
      name: Ted Real
      email: real@example.com
"""


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
            unique_id="group.test_package.test_group",
            path="models/fake_path",
            resource_type=NodeType.Group,
        )

    @pytest.fixture
    def change(self, new_group: Group) -> Change:
        return ResourceChange(
            operation=Operation.Add,
            entity_type=EntityType.Group,
            identifier=new_group.name,
            path=pathlib.Path(new_group.path),
            data={
                "name": new_group.name,
                "owner": new_group.owner.to_dict(),
            },
        )

    def test_adds_groups_to_empty_file(self, change: ResourceChange):
        yml_dict = ResourceFileEditor.update_resource({}, change)
        assert yml_dict == read_yml(expected_group_yml_no_group)

    def test_adds_groups_to_existing_list_of_groups(self, change: ResourceChange):
        yml_dict = ResourceFileEditor.update_resource(read_yml(group_yml_existing_groups), change)
        assert yml_dict == read_yml(expected_group_yml_existing_groups)

    def test_adds_groups_updates_predefined_group(self, change: ResourceChange):
        yml_dict = ResourceFileEditor.update_resource(read_yml(group_yml_group_predefined), change)
        assert yml_dict == read_yml(expected_group_yml_no_group)
