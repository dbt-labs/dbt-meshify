import pytest
from dbt.contracts.graph.nodes import Group
from dbt.contracts.graph.unparsed import Owner
from dbt.node_types import AccessType, NodeType

from dbt_meshify.storage.file_content_editors import DbtMeshFileEditor
from tests.unit import read_yml

meshify = DbtMeshFileEditor()
model_name = "shared_model"

model_yml_empty_file = """"""

model_yml_model_missing = """
models:
 - name: unrelated_model
"""

model_yml_shared_model = """
models:
  - name: shared_model
"""


expected_model_yml_shared_model = """
models:
  - name: shared_model
    access: public
    group: test_group
"""

model_yml_shared_model_with_group = """
models:
  - name: shared_model
    access: private
    group: old_group
"""


model_yml_multiple_models = """
models:
  - name: shared_model
  - name: other_model
  - name: other_other_model
"""

expected_model_yml_multiple_models = """
models:
  - name: shared_model
    access: public
    group: test_group
  - name: other_model
  - name: other_other_model
"""

expected_model_yml_multiple_models_multi_select = """
models:
  - name: shared_model
    access: public
    group: test_group
  - name: other_model
    access: public
    group: test_group
  - name: other_other_model
"""


class TestAddGroupToModelYML:
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

    def test_adds_group_to_model_file(self, new_group: Group):
        yml_dict = meshify.add_group_and_access_to_model_yml(
            model_name=model_name,
            group=new_group,
            access_type=AccessType.Public,
            models_yml=read_yml(model_yml_shared_model),
        )
        assert yml_dict == read_yml(expected_model_yml_shared_model)

    def test_adds_group_overwrites_existing_groups(self, new_group: Group):
        yml_dict = meshify.add_group_and_access_to_model_yml(
            model_name=model_name,
            group=new_group,
            access_type=AccessType.Public,
            models_yml=read_yml(model_yml_shared_model_with_group),
        )
        assert yml_dict == read_yml(expected_model_yml_shared_model)

    def test_preserves_existing_models(self, new_group: Group):
        yml_dict = meshify.add_group_and_access_to_model_yml(
            model_name=model_name,
            group=new_group,
            access_type=AccessType.Public,
            models_yml=read_yml(model_yml_multiple_models),
        )
        assert yml_dict == read_yml(expected_model_yml_multiple_models)
