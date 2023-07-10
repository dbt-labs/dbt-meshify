import os
from collections import OrderedDict
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

from dbt.contracts.graph.nodes import Group, ManifestNode
from dbt.contracts.results import CatalogTable
from dbt.node_types import AccessType, NodeType
from loguru import logger

from dbt_meshify.storage.file_manager import DbtFileManager


def filter_empty_dict_items(dict_to_filter: Dict[str, Any]):
    """Filters out empty dictionary items"""
    return {k: v for k, v in dict_to_filter.items() if v}


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


def resources_yml_to_dict(resources_yml: Optional[Dict], resource_type: NodeType = NodeType.Model):
    """Converts a yml dict to a named dictionary for easier operation"""
    return (
        {resource["name"]: resource for resource in resources_yml[resource_type.pluralize()]}
        if resources_yml
        else {}
    )


class DbtMeshFileEditor:
    """
    Class to operate on the contents of a dbt project's files
    to add the dbt mesh functionality
    includes editing yml entries and sql file contents
    """

    @staticmethod
    def add_group_to_yml(group: Group, groups_yml: Dict[str, Any]):
        """Add a group to a yml file"""
        if groups_yml is None:
            groups_yml = {}

        groups = resources_yml_to_dict(groups_yml, NodeType.Group)
        group_yml = groups.get(group.name) or {}

        group_yml.update({"name": group.name})
        owner = group_yml.get("owner", {})
        owner.update(filter_empty_dict_items(group.owner.to_dict()))
        group_yml["owner"] = owner

        groups[group.name] = filter_empty_dict_items(group_yml)

        groups_yml["groups"] = list(groups.values())
        return groups_yml

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

    @staticmethod
    def add_group_and_access_to_model_yml(
        model_name: str, group: Group, access_type: AccessType, models_yml: Dict[str, Any]
    ):
        """Add group and access configuration to a model's YAMl properties."""
        # parse the yml file into a dictionary with model names as keys
        models_yml = DbtMeshFileEditor.add_access_to_model_yml(model_name, access_type, models_yml)
        models_yml = DbtMeshFileEditor.add_group_to_model_yml(model_name, group, models_yml)
        return models_yml

    def get_source_yml_entry(
        self, resource_name: str, full_yml: Dict[str, Any], source_name: str
    ) -> Tuple[Dict[str, Any], Optional[Dict[str, Any]]]:
        """
        Remove a single source entry from a source defintion block, return source definition with single source entry and the remainder of the original
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
                raise ValueError('Missing source name')
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

    def add_model_contract_to_yml(
        self, model_name: str, model_catalog: Optional[CatalogTable], models_yml: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Adds a model contract to the model's yaml"""
        # set up yml order

        # parse the yml file into a dictionary with model names as keys
        models = resources_yml_to_dict(models_yml)
        model_yml = models.get(model_name) or {"name": model_name, "columns": [], "config": {}}

        # isolate the columns from the existing model entry
        yml_cols: List[Dict] = model_yml.get("columns", [])
        catalog_cols = model_catalog.columns or {} if model_catalog else {}
        catalog_cols = {k.lower(): v for k, v in catalog_cols.items()}

        # add the data type to the yml entry for columns that are in yml
        yml_cols = [
            {**yml_col, "data_type": catalog_cols[yml_col["name"]].type.lower()}
            for yml_col in yml_cols
            if yml_col.get("name") in catalog_cols.keys()
        ]

        # append missing columns in the table to the yml entry
        yml_col_names = [col["name"].lower() for col in yml_cols]
        for col_name, col in catalog_cols.items():
            if col_name.lower() not in yml_col_names:
                yml_cols.append({"name": col_name.lower(), "data_type": col.type.lower()})

        # update the columns in the model yml entry
        model_yml.update({"columns": yml_cols})
        # add contract to the model yml entry
        # this part should come from the same service as what we use for the standalone command when we get there
        model_config = model_yml.get("config", {})
        model_config.update({"contract": {"enforced": True}})
        model_yml["config"] = model_config
        # update the model entry in the full yml file
        # if no entries exist, add the model entry
        # otherwise, update the existing model entry in place
        processed = process_model_yml(model_yml)
        models[model_name] = processed

        models_yml["models"] = list(models.values())
        return models_yml

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

    def add_model_version_to_yml(
        self,
        model_name,
        models_yml,
        prerelease: Optional[bool] = False,
        defined_in: Optional[os.PathLike] = None,
    ) -> Dict[str, Any]:
        """Adds a model version to the model's yaml"""
        # set up yml order

        models = resources_yml_to_dict(models_yml)
        model_yml = models.get(model_name) or {
            "name": model_name,
            "latest_version": 0,
            "versions": [],
        }
        # add the version to the model yml entry
        versions_list = model_yml.get("versions") or []
        latest_version = model_yml.get("latest_version") or 0
        latest_yml_version = self.get_latest_yml_defined_version(model_yml)
        version_dict: Dict[str, Union[int, str, os.PathLike]] = {}
        if not versions_list:
            version_dict["v"] = 1
            latest_version += 1
        # if the model has versions, add the next version
        # if prerelease flag is true, do not increment the latest_version
        elif prerelease:
            version_dict = {"v": latest_yml_version + 1}
        else:
            version_dict = {"v": latest_yml_version + 1}
            latest_version += 1
        # add the defined_in key if it exists
        if defined_in:
            version_dict["defined_in"] = defined_in
        # add the version to the model yml entry
        versions_list.append(version_dict)
        # update the latest version in the model yml entry
        model_yml["versions"] = versions_list
        model_yml["latest_version"] = latest_version

        processed = process_model_yml(model_yml)
        models[model_name] = processed

        models_yml["models"] = list(models.values())
        return models_yml

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
            # find yml path for resoruces that are not defined
            yml_path = Path(self.node.patch_path.split("://")[1]) if self.node.patch_path else None
        else:
            yml_path = self.get_resource_path()

        # if the model doesn't have a patch path, create a new yml file in the models directory
        if not yml_path:
            resource_path = self.get_resource_path()

            if resource_path is None:
                # If this happens, then the model doesn't have a model file, either, which is cause for alarm.
                raise Exception(f"Unable to locate the file defining {self.node.name}. Aborting")

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

    def add_model_contract(self) -> None:
        """Adds a model contract to the model's yaml"""
        yml_path = self.get_patch_path()
        # read the yml file
        # pass empty dict if no file contents returned
        models_yml = self.file_manager.read_file(yml_path)

        if isinstance(models_yml, str):
            raise Exception(f"Unexpected string values in dumped model data in {yml_path}.")

        updated_yml = self.add_model_contract_to_yml(
            model_name=self.node.name,
            model_catalog=self.model_catalog,
            models_yml=models_yml,
        )
        # write the updated yml to the file
        self.file_manager.write_file(yml_path, updated_yml)

    def add_model_access(self, access_type: AccessType) -> None:
        """Adds a model contract to the model's yaml"""
        yml_path = self.get_patch_path()
        logger.info(f"Adding model contract for {self.node.name} at {yml_path}")
        # read the yml file
        # pass empty dict if no file contents returned
        models_yml = self.file_manager.read_file(yml_path)

        if isinstance(models_yml, str):
            raise Exception(f"Unexpected string values in dumped model data in {yml_path}.")

        updated_yml = self.add_access_to_model_yml(
            model_name=self.node.name,
            access_type=access_type,
            models_yml=models_yml,
        )
        # write the updated yml to the file
        self.file_manager.write_file(yml_path, updated_yml)

    def add_model_version(
        self, prerelease: Optional[bool] = False, defined_in: Optional[os.PathLike] = None
    ) -> None:
        """Adds a model version to the model's yaml"""

        yml_path = self.get_patch_path()

        # read the yml file
        # pass empty dict if no file contents returned
        models_yml = self.file_manager.read_file(yml_path) or {}
        latest_yml_version = self.get_latest_yml_defined_version(
            resources_yml_to_dict(models_yml).get(self.node.name, {})  # type: ignore
        )
        try:
            updated_yml = self.add_model_version_to_yml(
                model_name=self.node.name,
                models_yml=models_yml,
                prerelease=prerelease,
                defined_in=defined_in,
            )
            # write the updated yml to the file
            self.file_manager.write_file(yml_path, updated_yml)
            logger.info("Model version added to model yml")
        except Exception as e:
            logger.error(f"Error adding model version to model yml: {e}")
            logger.exception(e)
        # create the new version file

        # if we're incrementing the version, write the new version file with a copy of the code
        latest_version = int(self.node.latest_version) if self.node.latest_version else 0
        last_version_file_name = f"{self.node.name}_v{latest_version}.{self.node.language}"
        next_version_file_name = (
            f"{defined_in}.{self.node.language}"
            if defined_in
            else f"{self.node.name}_v{latest_yml_version + 1}.{self.node.language}"
        )
        model_path = self.get_resource_path()

        if model_path is None:
            raise Exception(f"Unable to find path to model {self.node.name}. Aborting.")

        model_folder = model_path.parent
        next_version_path = model_folder / next_version_file_name
        last_version_path = model_folder / last_version_file_name

        # if this is the first version, rename the original file to the next version
        if not self.node.latest_version:
            logger.info(f"Creating first version of {self.node.name} at {next_version_path}")
            Path(self.project_path).joinpath(model_path).rename(
                Path(self.project_path).joinpath(next_version_path)
            )
        else:
            # if existing versions, create the new one
            logger.info(f"Creating new version of {self.node.name} at {next_version_path}")
            self.file_manager.write_file(next_version_path, self.node.raw_code)
            # if the existing version doesn't use the _v{version} naming convention, rename it to the previous version
            if not model_path.stem.endswith(f"_v{latest_version}"):
                logger.info(
                    f"Renaming existing version of {self.node.name} from {model_path.name} to {last_version_path.name}"
                )
                Path(self.project_path).joinpath(model_path).rename(
                    Path(self.project_path).joinpath(last_version_path)
                )

    def update_model_refs(self, model_name: str, project_name: str) -> None:
        """Updates the model refs in the model's sql file"""
        model_path = self.get_resource_path()

        if model_path is None:
            raise Exception(f"Unable to find path to model {self.node.name}. Aborting.")

        # read the model file
        model_code = str(self.file_manager.read_file(model_path))
        # This can be defined in the init for this clas.
        ref_update_methods = {'sql': self.update_sql_refs, 'python': self.update_python_refs}
        # Here, we're trusting the dbt-core code to check the languages for us. 🐉
        updated_code = ref_update_methods[self.node.language](
            model_name=model_name,
            project_name=project_name,
            model_code=model_code,
        )
        # write the updated model code to the file
        self.file_manager.write_file(model_path, updated_code)
