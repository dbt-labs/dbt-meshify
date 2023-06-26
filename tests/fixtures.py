from dbt.contracts.results import CatalogTable

shared_model_sql = """
with source_data as (


    select 1 as id, 'grace' as colleague
    union all
    select 2 as id, 'dave' as colleague

)

select *
from source_data
"""

shared_model_catalog_entry = CatalogTable.from_dict(
    {
        "metadata": {
            "type": "BASE TABLE",
            "schema": "main",
            "name": "shared_model",
            "database": "database",
            "comment": None,
            "owner": None,
        },
        "columns": {
            "ID": {"type": "INTEGER", "index": 1, "name": "id", "comment": None},
            "colleague": {"type": "VARCHAR", "index": 2, "name": "colleague", "comment": None},
        },
        "stats": {
            "has_stats": {
                "id": "has_stats",
                "label": "Has Stats?",
                "value": False,
                "include": False,
                "description": "Indicates whether there are statistics for this table",
            }
        },
        "unique_id": "model.src_proj_a.shared_model",
    }
)

model_yml_no_col_no_version = """
models:
  - name: shared_model
    description: "this is a test model"
"""

model_yml_other_model = """
models:
  - name: other_shared_model
    description: "this is a different test model"
  - name: shared_model
"""

model_yml_one_col = """
models:
  - name: shared_model
    description: "this is a test model"
    columns:
      - name: id
        description: "this is the id column"
"""

model_yml_one_col_one_test = """
models:
  - name: shared_model
    description: "this is a test model"
    columns:
      - name: id
        description: "this is the id column"
        tests:
          - unique
"""

model_yml_all_col = """
models:
  - name: shared_model
    description: "this is a test model"
    columns:
      - name: id
        description: "this is the id column"
      - name: colleague
        description: "this is the colleague column"
"""
expected_contract_yml_no_col = """
models:
  - name: shared_model
    config:
      contract:
        enforced: true
    description: "this is a test model"
    columns:
      - name: id
        data_type: integer
      - name: colleague
        data_type: varchar
"""

expected_contract_yml_one_col = """
models:
  - name: shared_model
    config:
      contract:
        enforced: true
    description: "this is a test model"
    columns:
      - name: id
        description: "this is the id column"
        data_type: integer
      - name: colleague
        data_type: varchar
"""

expected_contract_yml_one_col_one_test = """
models:
  - name: shared_model
    config:
      contract:
        enforced: true
    description: "this is a test model"
    columns:
      - name: id
        description: "this is the id column"
        data_type: integer
        tests:
          - unique
      - name: colleague
        data_type: varchar
"""

expected_contract_yml_all_col = """
models:
  - name: shared_model
    config:
      contract:
        enforced: true
    description: "this is a test model"
    columns:
      - name: id
        description: "this is the id column"
        data_type: integer
      - name: colleague
        description: "this is the colleague column"
        data_type: varchar
"""

expected_contract_yml_no_entry = """
models:
  - name: shared_model
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: integer
      - name: colleague
        data_type: varchar
"""

expected_contract_yml_other_model = """
models:
  - name: other_shared_model
    description: "this is a different test model"
  - name: shared_model
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: integer
      - name: colleague
        data_type: varchar
"""

expected_versioned_model_yml_no_yml = """
models:
  - name: shared_model
    latest_version: 1
    versions:
      - v: 1
"""

expected_versioned_model_yml_no_version = """
models:
  - name: shared_model
    latest_version: 1
    description: "this is a test model"
    versions:
      - v: 1
"""

model_yml_increment_version = """
models:
  - name: shared_model
    latest_version: 1
    description: "this is a test model"
    versions:
      - v: 1
"""

expected_versioned_model_yml_increment_version_no_prerelease = """
models:
  - name: shared_model
    latest_version: 2
    description: "this is a test model"
    versions:
      - v: 1
      - v: 2
"""

expected_versioned_model_yml_increment_version_with_prerelease = """
models:
  - name: shared_model
    latest_version: 1
    description: "this is a test model"
    versions:
      - v: 1
      - v: 2
"""

expected_versioned_model_yml_increment_version_defined_in = """
models:
  - name: shared_model
    latest_version: 2
    description: "this is a test model"
    versions:
      - v: 1
      - v: 2
        defined_in: daves_model
"""
