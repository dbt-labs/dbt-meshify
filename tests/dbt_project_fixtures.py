from pathlib import Path
from unittest.mock import MagicMock

import pytest
from dbt.contracts.files import FileHash
from dbt.contracts.graph.nodes import ModelNode, NodeType
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


@pytest.fixture
def project():
    project = MagicMock()
    project.resolve_patch_path.return_value = Path(".")
    return project


@pytest.fixture
def model() -> ModelNode:
    return ModelNode(
        database=None,
        resource_type=NodeType.Model,
        checksum=FileHash("foo", "foo"),
        schema="foo",
        name="shared_model",
        package_name="foo",
        path="models/_models.yml",
        original_file_path="models/shared_model.sql",
        unique_id="model.foo.foo",
        fqn=["foo", "foo"],
        alias="foo",
    )
