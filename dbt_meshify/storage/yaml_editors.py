import os
from collections import OrderedDict
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from dbt.contracts.graph.nodes import Group, ManifestNode
from dbt.contracts.results import CatalogTable
from dbt.node_types import AccessType

from dbt_meshify.storage.file_manager import DbtFileManager


def filter_empty_dict_items(dict_to_filter: Dict[str, Any]):
    """Filters out empty dictionary items"""
    return {k: v for k, v in dict_to_filter.items() if v is not None}


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


def resources_yml_to_dict(resources_yml: Dict[str, Any], resource_type: str = "models"):
    """Converts a yml dict to a named dictionary for easier operation"""
    return (
        {resource["name"]: resource for resource in resources_yml[resource_type]}
        if resources_yml
        else {}
    )


class DbtMeshModelYmlEditor:
    """
    Class to operate on the contents of a dbt project's dbt_project.yml file
    to add the dbt-core concepts specific to the dbt linker
    """

    @staticmethod
    def add_group_to_yml(group: Group, groups_yml: Dict[str, Any]):
        """Add a group to a yml file"""
        if groups_yml is None:
            groups_yml = {}

        groups = resources_yml_to_dict(groups_yml, "groups")
        group_yml = groups.get(group.name) or {}

        group_yml.update({"name": group.name})
        owner = group_yml.get("owner", {})
        owner.update(filter_empty_dict_items(group.owner.to_dict()))
        group_yml["owner"] = owner

        groups[group.name] = filter_empty_dict_items(group_yml)

        groups_yml["groups"] = list(groups.values())
        return groups_yml

    @staticmethod
    def add_group_and_access_to_model_yml(
        model_name: str, group: Group, access_type: AccessType, models_yml: Dict[str, Any]
    ):
        """Add group and access configuration to a model's YAMl properties."""
        # parse the yml file into a dictionary with model names as keys
        models = resources_yml_to_dict(models_yml)
        model_yml = models.get(model_name) or {"name": model_name, "columns": [], "config": {}}

        model_yml.update({"access": access_type.value, "group": group.name})
        models[model_name] = process_model_yml(model_yml)

        models_yml["models"] = list(models.values())
        return models_yml

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
        return max([v.get("v") for v in model_yml_versions]) if model_yml_versions else 0

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


class DbtMeshModelConstructor(DbtMeshModelYmlEditor):
    def __init__(
        self,
        project_path: Path,
        model_node: ManifestNode,
        model_catalog: Optional[CatalogTable] = None,
    ):
        self.project_path = project_path
        self.model_node = model_node
        self.model_catalog = model_catalog
        self.name = model_node.name
        self.file_manager = DbtFileManager(
            read_project_path=project_path, write_project_path=project_path
        )

    def get_model_yml_path(self) -> Path:
        """Returns the path to the model yml file"""
        yml_path = (
            Path(self.model_node.patch_path.split("://")[1])
            if self.model_node.patch_path
            else None
        )

        # if the model doesn't have a patch path, create a new yml file in the models directory
        if not yml_path:
            model_path = self.get_model_path()

            if model_path is None:
                # If this happens, then the model doesn't have a model file, either, which is cause for alarm.
                raise Exception(
                    f"Unable to locate the file defining {self.model_node.name}. Aborting"
                )

            yml_path = model_path.parent / "_models.yml"
            self.file_manager.write_file(yml_path, {})
        return yml_path

    def get_model_path(self) -> Optional[Path]:
        """Returns the path to the model file"""
        return (
            Path(self.model_node.original_file_path)
            if self.model_node.original_file_path
            else None
        )

    def add_model_contract(self) -> None:
        """Adds a model contract to the model's yaml"""
        yml_path = self.get_model_yml_path()
        # read the yml file
        # pass empty dict if no file contents returned
        models_yml = self.file_manager.read_file(yml_path)

        if isinstance(models_yml, str):
            raise Exception(f"Unexpected string values in dumped model data in {yml_path}.")

        updated_yml = self.add_model_contract_to_yml(
            model_name=self.model_node.name,
            model_catalog=self.model_catalog,
            models_yml=models_yml,
        )
        # write the updated yml to the file
        self.file_manager.write_file(yml_path, updated_yml)

    def add_model_version(
        self, prerelease: Optional[bool] = False, defined_in: Optional[os.PathLike] = None
    ) -> None:
        """Adds a model version to the model's yaml"""

        yml_path = self.get_model_yml_path()

        # read the yml file
        # pass empty dict if no file contents returned
        models_yml = self.file_manager.read_file(yml_path) or {}
        latest_yml_version = self.get_latest_yml_defined_version(
            resources_yml_to_dict(models_yml).get(self.model_node.name, {})  # type: ignore
        )
        updated_yml = self.add_model_version_to_yml(
            model_name=self.model_node.name,
            models_yml=models_yml,
            prerelease=prerelease,
            defined_in=defined_in,
        )
        # write the updated yml to the file
        self.file_manager.write_file(yml_path, updated_yml)

        # if we're incrementing the version, write the new version file with a copy of the code
        latest_version = (
            int(self.model_node.latest_version) if self.model_node.latest_version else 0
        )
        last_version_file_name = (
            f"{self.model_node.name}_v{latest_version}.{self.model_node.language}"
        )
        next_version_file_name = (
            f"{defined_in}.{self.model_node.language}"
            if defined_in
            else f"{self.model_node.name}_v{latest_yml_version + 1}.{self.model_node.language}"
        )
        model_path = self.get_model_path()

        if model_path is None:
            raise Exception(f"Unable to find path to model {self.model_node.name}. Aborting.")

        model_folder = model_path.parent
        next_version_path = model_folder / next_version_file_name
        last_version_path = model_folder / last_version_file_name

        # if this is the first version, rename the original file to the next version
        if not self.model_node.latest_version:
            Path(self.project_path).joinpath(model_path).rename(
                Path(self.project_path).joinpath(next_version_path)
            )
        else:
            # if existing versions, create the new one
            self.file_manager.write_file(next_version_path, self.model_node.raw_code)
            # if the existing version doesn't use the _v{version} naming convention, rename it to the previous version
            if not model_path.stem.endswith(f"_v{latest_version}"):
                Path(self.project_path).joinpath(model_path).rename(
                    Path(self.project_path).joinpath(last_version_path)
                )
