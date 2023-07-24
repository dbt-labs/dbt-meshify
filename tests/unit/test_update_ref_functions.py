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

simple_model_python = """
def model(dbt, session):

    my_sql_model_df = dbt.ref('my_table')

    return my_sql_model_df
"""

expected_simple_model_python = """
def model(dbt, session):

    my_sql_model_df = dbt.ref('upstream_project', 'my_table')

    return my_sql_model_df
"""


def read_yml(yml_str):
    return yaml.safe_load(yml_str)


class TestRemoveResourceYml:
    def test_update_sql_ref_function__basic(self):
        updated_sql = meshify.update_refs__sql(
            model_code=simple_model_sql,
            model_name=upstream_model_name,
            project_name=upstream_project_name,
        )
        assert updated_sql == expected_simple_model_sql

    def test_update_python_ref_function__basic(self):
        updated_python = meshify.update_refs__python(
            model_code=simple_model_python,
            model_name=upstream_model_name,
            project_name=upstream_project_name,
        )
        assert updated_python == expected_simple_model_python
