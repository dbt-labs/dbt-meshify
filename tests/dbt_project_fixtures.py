from dbt.contracts.results import CatalogTable

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
