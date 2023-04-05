import pytest
from dbt.test.util import (
    get_manifest,
    run_dbt,
    get_artifact
)
# whatever this command actually becomes
import dbt_meshify

models__project_a_shared_model = """
{{ config(materialized='table') }}

with source_data as (


    select 1 as id, 'grace' as colleague
    union all
    select 2 as id, 'dave' as colleague

)

select *
from source_data
"""

models__project_b_downstream_model_before = """
with 

upstream as (
    select * from {{ source('project_a', 'shared_model') }}
)

select * from upstream 
where colleague = 'grace'
"""

## need to make schema generic here
sources__project_b_before = """
version: 2 

sources:
  - name: project_a
    schema: dbt_dconnors 
    tables:
      - name: shared_model
"""

models__project_b_downstream_model_after = """
with 

upstream as (
    select * from {{ ref('project_a', 'shared_model') }}
)

select * from upstream 
where colleague = 'grace'
"""

class BaseProject:
    @pytest.fixture(scope="class")
    def get_manifest(self, project):
        results = run_dbt(["compile"])


class MergeSourceMetadataProjectA(BaseProject):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "shared_model.sql" : models__project_a_shared_model
        }

class MergeSourceMetadataProjectBBefore(BaseProject):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "downstream_model.sql" : models__project_b_downstream_model_before,
            "_sources.yml" : sources__project_b_before
        }


class BaseMergeSourceMetadataProjectBAfter(BaseProject):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "downstream_model.sql" : models__project_b_downstream_model_after,
        }
    
class TestMergeSourceMetadata:
    @pytest.fixture(scope="class")
    def test__merge_source_metadata(self):
        compiled_project_a = MergeSourceMetadataProjectA.compile_project()
        compiled_project_b_before = MergeSourceMetadataProjectBBefore.compile_project()
        compiled_project_b_after = BaseMergeSourceMetadataProjectBAfter.compile_project()

        merged_a, merged_b = dbt_meshify.merge(compiled_project_a, compiled_project_b_before)

        assert merged_a == compiled_project_a
        assert merged_b == compiled_project_b_after