from unittest.mock import MagicMock

import yaml

from dbt_meshify.utilities.references import ReferenceUpdater

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
        reference_updater = ReferenceUpdater(project=MagicMock())
        updated_sql = reference_updater.update_refs__sql(
            model_code=simple_model_sql,
            model_name=upstream_model_name,
            project_name=upstream_project_name,
        )
        assert updated_sql == expected_simple_model_sql

    def test_update_python_ref_function__basic(self):
        reference_updater = ReferenceUpdater(project=MagicMock())
        updated_python = reference_updater.update_refs__python(
            model_code=simple_model_python,
            model_name=upstream_model_name,
            project_name=upstream_project_name,
        )
        assert updated_python == expected_simple_model_python
