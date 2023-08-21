from typing import Any, Dict, List, Optional

from loguru import logger

from dbt_meshify.change import EntityType, FileChange, ResourceChange
from dbt_meshify.exceptions import FileEditorException
from dbt_meshify.storage.file_manager import RawFileManager, YAMLFileManager


class NamedList(dict):
    """An NamedList is a Dict generated from a list with an indexable value."""

    def __init__(self, source_list: Optional[List[Dict]] = None, index_field: str = "name"):
        self.index_field = index_field
        data = {}

        # Allow empty source lists
        if source_list is None:
            source_list = []

        for item in source_list:
            for key, value in item.items():
                if (
                    isinstance(value, list)
                    and len(value) > 0
                    and isinstance(value[0], dict)
                    and self.index_field in value[0]
                ):
                    item[key] = NamedList(value)

            data[item[self.index_field]] = item

        data = {item.get(self.index_field): item for item in source_list}
        super().__init__(data)

    def to_list(self) -> List[Dict]:
        """Create a List from an IndexableList"""
        output = []
        for _, item in self.items():
            for key, value in item.items():
                if isinstance(value, NamedList):
                    item[key] = value.to_list()

            output.append(item)
        return output


def filter_empty_dict_items(dict_to_filter: Dict[str, Any]):
    """Filters out empty dictionary items"""
    return {k: v for k, v in dict_to_filter.items() if v}


def format_resource(entity_type: EntityType, resource: Dict[str, Any]):
    """Process a resource to be written back to a file in a given order."""

    sort_order = None
    if entity_type == EntityType.Model:
        sort_order = [
            "name",
            "description",
            "latest_version",
            "access",
            "group",
            "config",
            "meta",
            "tests",
            "columns",
            "versions",
        ]

    if not sort_order:
        return resource

    return {key: resource[key] for key in sort_order if key in resource}


def safe_update(original: Dict[Any, Any], update: Dict[Any, Any]) -> Dict[Any, Any]:
    """Safely update a dictionary without squashing nesting dictionary values."""

    for key, value in update.items():
        if isinstance(value, dict) or isinstance(value, NamedList):
            original[key] = safe_update(
                original.get(key, NamedList() if isinstance(value, NamedList) else {}), value
            )
        elif value is None and key in original:
            del original[key]
        elif value is not None:
            original[key] = value
    return original


class RawFileEditor:
    """A class used to perform Raw operations on Files"""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

    @staticmethod
    def add(change: FileChange):
        """Add data to a new file."""

        if not change.path.parent.exists():
            change.path.parent.mkdir(parents=True, exist_ok=True)

        if change.data is None:
            return RawFileManager.touch_file(change.path)

        RawFileManager.write_file(path=change.path, content=change.data)

    @staticmethod
    def update(change: FileChange):
        """Update data to a new file."""

        if not change.path.exists():
            raise FileNotFoundError(f"Unable to find file {change.path}.")

        return RawFileEditor.add(change)

    @staticmethod
    def copy(change: FileChange):
        """Copy a file from one location to another."""
        if change.source is None:
            raise FileEditorException("None source value provided in Copy operation.")

        RawFileManager.copy_file(change.source, change.path)

    @staticmethod
    def move(change: FileChange):
        """Move a file from one location to another."""
        if change.source is None:
            raise FileEditorException("None source value provided in Copy operation.")

        RawFileManager.move_file(change.source, change.path)


class ResourceFileEditor:
    @staticmethod
    def update_resource(properties: Dict[Any, Any], change: ResourceChange) -> Dict:
        """Update an identified resource in a properties dictionary using a ResourceChange."""

        identifier = change.identifier if not change.source_name else change.source_name
        entities = NamedList(properties.get(change.entity_type.pluralize(), []))
        updated_entities = safe_update(entities.get(identifier, {}), change.data)
        entities[identifier] = format_resource(change.entity_type, updated_entities)

        properties[change.entity_type.pluralize()] = entities.to_list()
        if len(properties[change.entity_type.pluralize()]) == 0:
            del properties[change.entity_type.pluralize()]

        return properties

    @staticmethod
    def remove_resource(properties: Dict, change: ResourceChange) -> Dict:
        """Remove an identified resource in a properties dictionary using a ResourceChange."""

        entities = NamedList(properties.get(change.entity_type.pluralize(), []))

        if change.source_name:
            source_tables = entities[change.source_name].get("tables", {})
            del source_tables[change.identifier]
            entities[change.source_name]["tables"] = source_tables

            # Remove the source definition if the last table has been removed.
            if len(entities[change.source_name]["tables"]) == 0:
                del entities[change.source_name]

        else:
            del entities[change.identifier]

        properties[change.entity_type.pluralize()] = entities.to_list()

        if len(properties[change.entity_type.pluralize()]) == 0:
            del properties[change.entity_type.pluralize()]

        return properties

    def add(self, change: ResourceChange) -> None:
        """Add a Resource to a YAML file at a given path."""

        # Make the file if it does not exist.
        if not change.path.parent.exists():
            change.path.parent.mkdir(parents=True, exist_ok=True)

        if not change.path.exists():
            open(change.path, "w").close()

        properties = YAMLFileManager.read_file(change.path) or {}
        properties = self.update_resource(properties, change)
        YAMLFileManager.write_file(change.path, properties)

    def update(self, change: ResourceChange) -> None:
        """Update an existing Resource in a YAML file"""

        properties = YAMLFileManager.read_file(change.path)
        properties = self.update_resource(properties, change)

        # If there are no more resources in the file, remove it.
        if len(properties.keys()) == 0 or set(properties.keys()) == {"version"}:
            logger.debug("There are no resources remaining in {change.path}. Removing the file.")
            RawFileManager.delete_file(change.path)
            return

        YAMLFileManager.write_file(change.path, properties)

    def remove(self, change: ResourceChange) -> None:
        """Remove an existing resource from a YAML file"""
        properties = YAMLFileManager.read_file(change.path)
        properties = self.remove_resource(properties, change)

        # If there are no more resources in the file, remove it.
        if len(properties.keys()) == 0 or set(properties.keys()) == {"version"}:
            logger.debug("There are no resources remaining in {change.path}. Removing the file.")
            RawFileManager.delete_file(change.path)
            return

        YAMLFileManager.write_file(change.path, properties)
