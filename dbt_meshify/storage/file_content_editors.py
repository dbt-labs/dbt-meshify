import shutil
from collections import OrderedDict
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

from dbt.contracts.graph.nodes import Group, ManifestNode
from dbt.contracts.results import CatalogTable
from dbt.node_types import AccessType, NodeType
from loguru import logger

from dbt_meshify.change import Change, EntityType, FileChange, ResourceChange
from dbt_meshify.exceptions import FileEditorException, ModelFileNotFoundError
from dbt_meshify.storage.file_manager import DbtFileManager


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


def process_model_yml(model_yml: Dict[str, Any]):
    """Processes the yml contents to be written back to a file"""
    model_ordered_dict = OrderedDict.fromkeys(
        [
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
    )
    model_ordered_dict.update(model_yml)
    # remove any keys with None values
    return filter_empty_dict_items(model_ordered_dict)


def resources_yml_to_dict(
    resources_yml: Optional[Dict], resource_type: Union[NodeType, EntityType] = NodeType.Model
):
    """Converts a yml dict to a named dictionary for easier operation"""
    return (
        {resource["name"]: resource for resource in resources_yml[resource_type.pluralize()]}
        if resources_yml
        else {}
    )


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


class FileEditor:
    """A class used to update ChangeableResources"""

    def __init__(self, file_manager: Optional[DbtFileManager] = None) -> None:
        self.file_manager = file_manager


class RawFileEditor(FileEditor):
    """A class used to perform Raw operations on Files"""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

    def update(self, change: FileChange):
        """Update data to a new file."""

        if not change.path.exists():
            raise FileNotFoundError(f"Unable to find file {change.path}.")

        with open(change.path, "w") as file:
            if change.data:
                file.write(change.data)

    def copy(self, change: FileChange):
        """Copy a file from one location to another."""
        if change.source is None:
            raise FileEditorException("None source value provided in Copy operation.")

        shutil.copy(change.source, change.path)

    def move(self, change: FileChange):
        """Move a file from one location to another."""
        if change.source is None:
            raise FileEditorException("None source value provided in Copy operation.")

        shutil.move(change.source, change.path)


class ResourceFileEditor(FileEditor):
    def __init__(self, project_path: Path) -> None:
        self.file_manager: DbtFileManager = DbtFileManager(read_project_path=project_path)
        super().__init__(self.file_manager)

    @staticmethod
    def update_resource(properties: Dict[Any, Any], change: ResourceChange) -> Dict:
        entities = NamedList(properties.get(change.entity_type.pluralize(), []))

        if change.source_name:
            updated_entities = safe_update(
                entities.get(change.source_name, {}).get(change.identifier, {}), change.data
            )
            entities[change.source_name][change.identifier] = format_resource(
                change.entity_type, updated_entities
            )
        else:
            updated_entities = safe_update(entities.get(change.identifier, {}), change.data)
            entities[change.identifier] = format_resource(change.entity_type, updated_entities)

        properties[change.entity_type.pluralize()] = entities.to_list()
        return properties

    @staticmethod
    def remove_resource(properties: Dict, change: ResourceChange) -> Dict:
        entities = NamedList(properties.get(change.entity_type.pluralize(), []))
        print(entities)
        if change.source_name:
            source_tables = entities[change.source_name].get("tables", {})
            del source_tables[change.identifier]
            entities[change.source_name]["tables"] = source_tables
        else:
            del entities[change.identifier]
        print(entities)
        properties[change.entity_type.pluralize()] = entities.to_list()

        if len(properties[change.entity_type.pluralize()]) == 0:
            del properties[change.entity_type.pluralize()]

        return properties

    def __read_file(self, path: Path) -> Dict:
        """Read a properties yaml file"""

        properties = self.file_manager.read_file(path)
        if isinstance(properties, str):
            raise FileEditorException(
                f"Unexpected type returned when reading {path}. ({type(properties)})"
            )
        return properties

    def add(self, change: ResourceChange) -> None:
        """Add a Resource to a YAML file at a given path."""

        # Make the file if it does not exist.
        if not change.path.exists():
            open(change.path, "w").close()

        properties = self.__read_file(change.path) or {}
        properties = self.update_resource(properties, change)
        self.file_manager.write_file(change.path, properties)

    def update(self, change: ResourceChange) -> None:
        """Update an existing Resource in a YAML file"""

        properties = self.__read_file(change.path)
        properties = self.update_resource(properties, change)
        self.file_manager.write_file(change.path, properties)

    def remove(self, change: ResourceChange) -> None:
        """Remove an existing resource from a YAML file"""
        properties = self.__read_file(change.path)
        properties = self.remove_resource(properties, change)
        self.file_manager.write_file(change.path, properties)


class ProjectFileEditor(FileEditor):
    def add(self, change: Change) -> None:
        raise NotImplementedError

    def update(self, change: Change) -> None:
        raise NotImplementedError

    def remove(self, change: Change) -> None:
        raise NotImplementedError


class CodeFileEditor(FileEditor):
    def add(self, change: Change) -> None:
        raise NotImplementedError

    def update(self, change: Change) -> None:
        raise NotImplementedError

    def remove(self, change: Change) -> None:
        raise NotImplementedError


class DbtMeshFileEditor:
    """
    Class to operate on the contents of a dbt project's files
    to add the dbt mesh functionality
    includes editing yml entries and sql file contents
    """

    @staticmethod
    def add_access_to_model_yml(
        model_name: str, access_type: AccessType, models_yml: Dict[str, Any]
    ):
        """Add group and access configuration to a model's YAMl properties."""
        # parse the yml file into a dictionary with model names as keys
        models = resources_yml_to_dict(models_yml)
        model_yml = models.get(model_name) or {"name": model_name, "columns": [], "config": {}}

        model_yml.update({"access": access_type.value})
        models[model_name] = process_model_yml(model_yml)

        models_yml["models"] = list(models.values())
        return models_yml

    @staticmethod
    def add_group_to_model_yml(model_name: str, group: Group, models_yml: Dict[str, Any]):
        """Add group and access configuration to a model's YAMl properties."""
        # parse the yml file into a dictionary with model names as keys
        models = resources_yml_to_dict(models_yml)
        model_yml = models.get(model_name) or {"name": model_name, "columns": [], "config": {}}

        model_yml.update({"group": group.name})
        models[model_name] = process_model_yml(model_yml)

        models_yml["models"] = list(models.values())
        return models_yml

    def get_source_yml_entry(
        self, resource_name: str, full_yml: Dict[str, Any], source_name: str
    ) -> Tuple[Dict[str, Any], Optional[Dict[str, Any]]]:
        """
        Remove a single source entry from a source definition block, return source definition with
        single source entry and the remainder of the original
        """
        sources = resources_yml_to_dict(full_yml, NodeType.Source)
        source_definition = sources.get(source_name)
        tables = source_definition.get("tables", [])
        table = list(filter(lambda x: x["name"] == resource_name, tables))
        remaining_tables = list(filter(lambda x: x["name"] != resource_name, tables))
        resource_yml = source_definition.copy()
        resource_yml["tables"] = table
        source_definition["tables"] = remaining_tables
        sources[source_name] = source_definition
        if len(remaining_tables) == 0:
            return resource_yml, None

        full_yml["sources"] = list(sources.values())
        return resource_yml, full_yml

    def get_yml_entry(
        self,
        resource_name: str,
        full_yml: Dict[str, Any],
        resource_type: NodeType = NodeType.Model,
        source_name: Optional[str] = None,
    ) -> Tuple[Dict[str, Any], Optional[Dict[str, Any]]]:
        """Remove a single resource entry from a yml file, return the single entry and the remainder of the yml file"""
        # parse the yml file into a dictionary with model names as keys
        if resource_type == NodeType.Source:
            if not source_name:
                raise ValueError("Missing source name")
            return self.get_source_yml_entry(resource_name, full_yml, source_name)
        else:
            resources = resources_yml_to_dict(full_yml, resource_type)
            resource_yml = resources.pop(resource_name, None)
            if len(resources.keys()) == 0:
                return resource_yml, None
            else:
                full_yml[resource_type.pluralize()] = (
                    list(resources.values()) if len(resources) > 0 else None
                )
                return resource_yml, full_yml

    def add_entry_to_yml(
        self, resource_entry: Dict[str, Any], full_yml: Dict[str, Any], resource_type: NodeType
    ):
        """
        Adds a single resource yml entry to yml file
        """
        if not full_yml:
            full_yml = {resource_type.pluralize(): []}

        if resource_type != NodeType.Source or resource_entry["name"] not in [
            source["name"] for source in full_yml[resource_type.pluralize()]
        ]:
            full_yml[resource_type.pluralize()].append(resource_entry)
            return full_yml

        new_table = resource_entry["tables"][0]
        sources = {source["name"]: source for source in full_yml["sources"]}
        sources[resource_entry["name"]]["tables"].append(new_table)
        full_yml["sources"] = list(sources.values())
        return full_yml

    def get_latest_yml_defined_version(self, model_yml: Dict[str, Any]):
        """
        Returns the latest version defined in the yml file for a given model name
        the format of `model_yml` should be a single model yml entry
        if no versions, returns 0
        """
        model_yml_versions = model_yml.get("versions", [])
        try:
            return max([int(v.get("v")) for v in model_yml_versions]) if model_yml_versions else 0
        except ValueError:
            raise ValueError(
                f"Version not an integer, can't increment version for {model_yml.get('name')}"
            )

    def update_sql_refs(self, model_code: str, model_name: str, project_name: str):
        import re

        # pattern to search for ref() with optional spaces and either single or double quotes
        pattern = re.compile(r"{{\s*ref\s*\(\s*['\"]" + re.escape(model_name) + r"['\"]\s*\)\s*}}")

        # replacement string with the new format
        replacement = f"{{{{ ref('{project_name}', '{model_name}') }}}}"

        # perform replacement
        new_code = re.sub(pattern, replacement, model_code)

        return new_code

    def update_python_refs(self, model_code: str, model_name: str, project_name: str):
        import re

        # pattern to search for ref() with optional spaces and either single or double quotes
        pattern = re.compile(r"dbt\.ref\s*\(\s*['\"]" + re.escape(model_name) + r"['\"]\s*\)")

        # replacement string with the new format
        replacement = f"dbt.ref('{project_name}', '{model_name}')"

        # perform replacement
        new_code = re.sub(pattern, replacement, model_code)

        return new_code


class DbtMeshConstructor(DbtMeshFileEditor):
    def __init__(
        self, project_path: Path, node: ManifestNode, catalog: Optional[CatalogTable] = None
    ):
        self.project_path = project_path
        self.node = node
        self.model_catalog = catalog
        self.name = node.name
        self.file_manager = DbtFileManager(read_project_path=project_path)

    def get_patch_path(self) -> Path:
        """Returns the path to the yml file where the resource is defined or described"""
        if self.node.resource_type in [
            NodeType.Model,
            NodeType.Seed,
            NodeType.Snapshot,
            NodeType.Macro,
            NodeType.Test,
        ]:
            # find yml path for resources that are not defined
            yml_path = Path(self.node.patch_path.split("://")[1]) if self.node.patch_path else None
        else:
            yml_path = self.get_resource_path()

        # if the model doesn't have a patch path, create a new yml file in the models directory
        if not yml_path:
            resource_path = self.get_resource_path()

            if resource_path is None:
                # If this happens, then the model doesn't have a model file, either, which is cause for alarm.
                raise ModelFileNotFoundError(
                    f"Unable to locate the file defining {self.node.name}. Aborting"
                )

            filename = f"_{self.node.resource_type.pluralize()}.yml"
            yml_path = resource_path.parent / filename
            self.file_manager.write_file(yml_path, {})
        logger.info(f"Schema entry for {self.node.unique_id} written to {yml_path}")
        return yml_path

    def get_resource_path(self) -> Path:
        """
        Returns the path to the file where the resource is defined
        for yml-only nodes (generic tests, metrics, exposures, sources)
        this will be the path to the yml file where the definitions
        for all others this will be the .sql or .py file for the resource
        """
        return Path(self.node.original_file_path)

    def add_model_access(self, access_type: AccessType) -> None:
        """Adds a model contract to the model's yaml"""
        yml_path = self.get_patch_path()
        logger.info(f"Adding model contract for {self.node.name} at {yml_path}")
        # read the yml file
        # pass empty dict if no file contents returned
        models_yml = self.file_manager.read_file(yml_path)

        if isinstance(models_yml, str):
            raise FileEditorException(
                f"Unexpected string values in dumped model data in {yml_path}."
            )

        updated_yml = self.add_access_to_model_yml(
            model_name=self.node.name,
            access_type=access_type,
            models_yml=models_yml,
        )
        # write the updated yml to the file
        self.file_manager.write_file(yml_path, updated_yml)

    def update_model_refs(self, model_name: str, project_name: str) -> None:
        """Updates the model refs in the model's sql file"""
        model_path = self.get_resource_path()

        if model_path is None:
            raise ModelFileNotFoundError(
                f"Unable to find path to model {self.node.name}. Aborting."
            )

        # read the model file
        model_code = str(self.file_manager.read_file(model_path))
        # This can be defined in the init for this class.
        ref_update_methods = {"sql": self.update_sql_refs, "python": self.update_python_refs}
        # Here, we're trusting the dbt-core code to check the languages for us. 🐉
        updated_code = ref_update_methods[self.node.language](
            model_name=model_name,
            project_name=project_name,
            model_code=model_code,
        )
        # write the updated model code to the file
        self.file_manager.write_file(model_path, updated_code)
