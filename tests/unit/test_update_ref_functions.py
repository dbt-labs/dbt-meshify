import yaml
from dbt.node_types import NodeType

from dbt_meshify.storage.file_content_editors import DbtMeshFileEditor

meshify = DbtMeshFileEditor()
upstream_project_name = "upstream_project"
upstream_model_name = "my_table"

simple_model_sql = """
select * from {{ ref('my_table') }}
"""

expected_simple_model_sql = """
select * from {{ ref('upstream_project', 'my_table') }}
"""


def read_yml(yml_str):
    return yaml.safe_load(yml_str)


class TestRemoveResourceYml:
    def test_update_ref_function__basic(self):
        updated_sql = meshify.update_sql_refs(
            model_sql=simple_model_sql,
            model_name=upstream_model_name,
            project_name=upstream_project_name,
        )
        assert updated_sql == expected_simple_model_sql
