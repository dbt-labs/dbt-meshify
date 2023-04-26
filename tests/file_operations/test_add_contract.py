from dbt.contracts.results import CatalogTable
from dbt_meshify.dbt_meshify import DbtMeshYmlEditor
import yaml

test_model_sql = """
select 
1 as id, 'blue' as color, true as is_cool_color
union all 
2 as id, 'cerulean' as color, true as is_cool_color
union all
3 as id, 'red' as color, false as is_cool_color
"""

test_model_catalog_entry = CatalogTable.from_dict(
    {
      "metadata" : {
        "type": "BASE TABLE",
        "schema": "dbt_dconnors",
        "name": "shared_model",
        "database": "postgres",
        "comment": "null",
        "owner": "daveconnors"
      },
      "columns" : {
        "id": { "type": "integer", "index": 1, "name": "id", "comment": "null" },
        "color": {
          "type": "text",
          "index": 2,
          "name": "color",
          "comment": "null"
        },
        "is_cool_color": {
          "type": "boolean",
          "index": 2,
          "name": "color",
          "comment": "null"
        }
      },
      "stats" : {
        "has_stats": {
          "id": "has_stats",
          "label": "Has Stats?",
          "value": False,
          "include": False,
          "description": "Indicates whether there are statistics for this table"
        }
      },
      "unique_id" : "model.src_proj_a.shared_model"
  }
)

model_yml_no_col = """
models:
  - name: test_model
    description: "this is a test model"
"""

model_yml_one_col = """
models:
  - name: test_model
    description: "this is a test model"
    columns:
      - name: id
        description: "this is the id column"
"""

model_yml_all_col = """
models:
  - name: test_model
    description: "this is a test model"
    columns:
      - name: id
        description: "this is the id column"
      - name: color
        description: "this is the color column"
      - name: is_cool_color
        description: "this is the is_cool_color column"
"""
expected_yml_no_col = """
models:
  - name: test_model
    config:
      contract:
        enforced: true
    description: "this is a test model"
    columns:
      - name: id
        data_type: integer
      - name: color
        data_type: text
      - name: is_cool_color
        data_type: boolean
"""

expected_yml_one_col = """
models:
  - name: test_model
    config:
      contract:
        enforced: true
    description: "this is a test model"
    columns:
      - name: id
        description: "this is the id column"
        data_type: integer
      - name: color
        data_type: text
      - name: is_cool_color
        data_type: boolean
"""

expected_yml_all_col = """
models:
  - name: test_model
    config:
      contract:
        enforced: true
    description: "this is a test model"
    columns:
      - name: id
        description: "this is the id column"
        data_type: integer
      - name: color
        description: "this is the color column"
        data_type: text
      - name: is_cool_color
        description: "this is the is_cool_color column"
        data_type: boolean
"""

expected_yml_no_entry = """
models:
  - name: test_model
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: integer
      - name: color
        data_type: text
      - name: is_cool_color
        data_type: boolean
"""

meshify = DbtMeshYmlEditor()
catalog_entry = test_model_catalog_entry
model_name = 'test_model'

def read_yml(yml_str):
    return yaml.safe_load(yml_str)

class TestAddContractToYML:
    
    def test_add_contract_to_yml_no_col(self):
        yml_dict = meshify.add_model_contract_to_yml(
            full_yml_dict=read_yml(model_yml_no_col), 
            model_catalog=catalog_entry, 
            model_name=model_name
        )
        assert yml_dict == read_yml(expected_yml_no_col)
        
    def test_add_contract_to_yml_one_col(self):
        yml_dict = meshify.add_model_contract_to_yml(
            full_yml_dict=read_yml(model_yml_one_col), 
            model_catalog=catalog_entry, 
            model_name=model_name
        )
        assert yml_dict == read_yml(expected_yml_one_col)

    def test_add_contract_to_yml_all_col(self):
        yml_dict = meshify.add_model_contract_to_yml(
            full_yml_dict=read_yml(model_yml_all_col), 
            model_catalog=catalog_entry, 
            model_name=model_name
        )
        assert yml_dict == read_yml(expected_yml_all_col)

    def test_add_contract_to_yml_no_entry(self):
        yml_dict = meshify.add_model_contract_to_yml(
            full_yml_dict=None, 
            model_catalog=catalog_entry, 
            model_name=model_name
        )
        assert yml_dict == read_yml(expected_yml_no_entry)