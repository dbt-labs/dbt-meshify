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

    def test_create_source_nested_resource(self):
        """ResourceEditor does not overwrite fields nested in lists."""
        properties = {
            "sources": [
                {
                    "name": "example",
                    "description": "foobar",
                }
            ]
        }
        change = ResourceChange(
            operation=Operation.Add,
            entity_type=EntityType.Source,
            identifier="example",
            path=Path("."),
            data={"tables": NamedList([{"name": "new_table", "description": "new_description"}])},
        )
        output = ResourceFileEditor.update_resource(properties, change)

        print(output)

        assert output["sources"][0]["description"] == "foobar"
        assert output["sources"][0]["tables"][0]["name"] == "new_table"

    def test_update_source_nested_resource(self):
        """ResourceEditor does not overwrite fields nested in lists."""
        properties = {
            "sources": [
                {
                    "name": "example",
                    "description": "foobar",
                    "tables": [
                        {"name": "new_table", "description": "old_description"},
                        {"name": "other", "description": "bogus"},
                    ],
                }
            ]
        }
        change = ResourceChange(
            operation=Operation.Add,
            entity_type=EntityType.Source,
            identifier="example",
            path=Path("."),
            data={"tables": NamedList([{"name": "new_table", "description": "new_description"}])},
        )
        output = ResourceFileEditor.update_resource(properties, change)

        print(output)

        assert output["sources"][0]["description"] == "foobar"
        assert output["sources"][0]["tables"][0]["description"] == "new_description"
