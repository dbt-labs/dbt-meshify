from pathlib import Path

from dbt_meshify.change import EntityType, Operation, ResourceChange
from dbt_meshify.storage.file_content_editors import NamedList, ResourceFileEditor


class TestResourceFileEditor:
    def test_update_resource_adds_fields(self):
        """ResourceFileEditor adds new fields when new items are present in a Change."""
        properties = {"models": [{"name": "example"}]}
        change = ResourceChange(
            operation=Operation.Update,
            entity_type=EntityType.Model,
            identifier="example",
            path=Path("."),
            data={"description": "foobar"},
        )
        output = ResourceFileEditor.update_resource(properties, change)
        assert output["models"][0]["name"] == "example"
        assert output["models"][0]["description"] == "foobar"

    def test_update_resource_overwrites_existing_fields(self):
        """ResourceFileEditor overwrites existing fields"""
        properties = {"models": [{"name": "example", "description": "bogus"}]}
        change = ResourceChange(
            operation=Operation.Update,
            entity_type=EntityType.Model,
            identifier="example",
            path=Path("."),
            data={"description": "foobar"},
        )
        output = ResourceFileEditor.update_resource(properties, change)
        assert output["models"][0]["description"] == "foobar"

    def test_update_resource_preserves_nested_fields(self):
        """ResourceEditor does not overwrite fields nested in lists."""
        properties = {
            "models": [
                {
                    "name": "example",
                    "columns": [{"name": "column_one", "tests": ["unique"]}],
                }
            ]
        }
        change = ResourceChange(
            operation=Operation.Update,
            entity_type=EntityType.Model,
            identifier="example",
            path=Path("."),
            data={"columns": NamedList([{"name": "column_one", "data_type": "bogus"}])},
        )
        output = ResourceFileEditor.update_resource(properties, change)

        assert output["models"][0]["columns"][0]["data_type"] == "bogus"
        assert output["models"][0]["columns"][0]["tests"] == ["unique"]
