from dbt.contracts.results import CatalogTable
shared_model_catalog_entry = CatalogTable.from_dict(
    {
      "metadata": {
        "type": "BASE TABLE",
        "schema": "dbt_dconnors",
        "name": "shared_model",
        "database": "postgres",
        "comment": None,
        "owner": "daveconnors"
      },
      "columns": {
        "id": { "type": "integer", "index": 1, "name": "id", "comment": None },
        "colleague": {
          "type": "text",
          "index": 2,
          "name": "colleague",
          "comment": None
        }
      },
      "stats": {
        "has_stats": {
          "id": "has_stats",
          "label": "Has Stats?",
          "value": False,
          "include": False,
          "description": "Indicates whether there are statistics for this table"
        }
      },
      "unique_id": "model.src_proj_a.shared_model"
    }
)

model_yml_no_col = """
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
expected_yml_no_col = """
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
        data_type: text
"""

expected_yml_one_col = """
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
        data_type: text
"""

expected_yml_all_col = """
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
        data_type: text
"""

expected_yml_no_entry = """
models:
  - name: shared_model
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: integer
      - name: colleague
        data_type: text
"""

expected_yml_other_model = """
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
        data_type: text
"""