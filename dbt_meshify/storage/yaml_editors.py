import os
from collections import OrderedDict
from pathlib import Path
from typing import Any, Dict, Optional

from dbt.contracts.graph.nodes import Group, ManifestNode
from dbt.contracts.results import CatalogTable
from dbt.node_types import AccessType

from dbt_meshify.storage.file_manager import DbtFileManager


class DbtMeshModelYmlEditor:
    """
    Class to operate on the contents of a dbt project's dbt_project.yml file
    to add the dbt-core concepts specific to the dbt linker
    """

    @staticmethod
    def add_group_to_yml(group: Group, full_yml_dict: Dict[str, Any]):
        """Add a group to a yml file"""
        if full_yml_dict is None:
            full_yml_dict = {}

        group_list = full_yml_dict.get("groups", []) or []
        groups = {group["name"]: group for group in group_list}
        group_yml = groups.get(group.name) or {}

        group_yml.update({"name": group.name})
        owner = group_yml.get("owner", {})
        owner.update({k: v for k, v in group.owner.to_dict().items() if v is not None})
        group_yml["owner"] = owner

        groups[group.name] = {k: v for k, v in group_yml.items() if v is not None}

        full_yml_dict["groups"] = list(groups.values())
        return full_yml_dict

    @staticmethod
    def add_group_and_access_to_model_yml(
        model_name: str, group: Group, access_type: AccessType, full_yml_dict: Dict[str, Any]
    ):
        """Add group and access configuration to a model's YAMl properties."""

        model_ordered_dict = OrderedDict.fromkeys(
            ["name", "description", "access", "config", "meta", "columns"]
        )
        # parse the yml file into a dictionary with model names as keys
        models = (
            {model["name"]: model for model in full_yml_dict["models"]} if full_yml_dict else {}
        )
        model_yml = models.get(model_name) or {"name": model_name, "columns": [], "config": {}}

        model_yml.update({"access": access_type.value})
        config = model_yml.get("config", {})
        config.update({"group": group.name})
        model_yml["config"] = config

        model_ordered_dict.update(model_yml)
        models[model_name] = {k: v for k, v in model_ordered_dict.items() if v is not None}

        full_yml_dict["models"] = list(models.values())
        return full_yml_dict

    def process_model_yml(self, model_yml: str):
        """Processes the yml contents to be written back to a file"""
        model_ordered_dict = OrderedDict.fromkeys(
            [
                "name",
                "description",
                "latest_version",
                "access",
                "config",
                "meta",
                "tests",
                "columns",
                "versions",
            ]
        )
        model_ordered_dict.update(model_yml)
        # remove any keys with None values
        model_ordered_dict = {k: v for k, v in model_ordered_dict.items() if v is not None}
        return model_ordered_dict

    def add_model_contract_to_yml(
        self, model_name: str, model_catalog: CatalogTable, full_yml_dict: Dict[str, str]
    ) -> None:
        """Adds a model contract to the model's yaml"""
        # set up yml order

        # parse the yml file into a dictionary with model names as keys

        models = (
            {model["name"]: model for model in full_yml_dict["models"]} if full_yml_dict else {}
        )
        model_yml = models.get(model_name) or {"name": model_name, "columns": [], "config": {}}

        # isolate the columns from the existing model entry
        yml_cols = model_yml.get("columns", [])
        catalog_cols = model_catalog.columns or {}
        # add the data type to the yml entry for columns that are in yml
        # import pdb; pdb.set_trace()
        yml_cols = [
            {**yml_col, "data_type": catalog_cols.get(yml_col.get("name")).type.lower()}
            for yml_col in yml_cols
            if yml_col.get("name") in catalog_cols.keys()
        ]
        # append missing columns in the table to the yml entry
        yml_col_names = [col.get("name").lower() for col in yml_cols]
        for col_name, col in catalog_cols.items():
            if col_name.lower() not in yml_col_names:
                yml_cols.append({"name": col_name.lower(), "data_type": col.type.lower()})
        # update the columns in the model yml entry
        model_yml.update({"columns": yml_cols})
        # add contract to the model yml entry
        # this part should come from the same service as what we use for the standalone command when we get there
        model_yml.update({"config": {"contract": {"enforced": True}}})
        # update the model entry in the full yml file
        # if no entries exist, add the model entry
        # otherwise, update the existing model entry in place
        processed = self.process_model_yml(model_yml)
        models[model_name] = processed

        full_yml_dict["models"] = list(models.values())
        return full_yml_dict

    def add_model_version_to_yml(
        self,
        model_name,
        full_yml_dict,
        prerelease: Optional[bool] = False,
        defined_in: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Adds a model version to the model's yaml"""
        # set up yml order

        models = (
            {model["name"]: model for model in full_yml_dict["models"]} if full_yml_dict else {}
        )
        model_yml = models.get(model_name) or {
            "name": model_name,
            "latest_version": 0,
            "versions": [],
        }
        # add the version to the model yml entry
        versions_list = model_yml.get("versions") or []
        latest_version = model_yml.get("latest_version") or 0
        version_dict = {}
        if not versions_list:
            version_dict["v"] = 1
            latest_version += 1
        # if the model has versions, add the next version
        # if prerelease flag is true, do not increment the latest_version
        elif prerelease:
            version_dict = {"v": latest_version + 1}
        else:
            version_dict = {"v": latest_version + 1}
            latest_version += 1
        # add the defined_in key if it exists
        if defined_in:
            version_dict["defined_in"] = defined_in
        # add the version to the model yml entry
        versions_list.append(version_dict)
        # update the latest version in the model yml entry
        model_yml["versions"] = versions_list
        model_yml["latest_version"] = latest_version

        processed = self.process_model_yml(model_yml)
        models[model_name] = processed

        full_yml_dict["models"] = list(models.values())
        return full_yml_dict


class DbtMeshModelConstructor(DbtMeshModelYmlEditor):
    def __init__(
        self,
        project_path: os.PathLike,
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

    def add_model_contract(self) -> None:
        """Adds a model contract to the model's yaml"""

        # get the patch path for the model
        node = self.model_node
        yml_path = Path(node.patch_path.split("://")[1]) if node.patch_path else None
        original_file_path = Path(node.original_file_path) if node.original_file_path else None
        # if the model doesn't have a patch path, create a new yml file in the models directory
        # TODO - should we check if there's a model yml file in the models directory and append to it?
        if not yml_path:
            yml_path = original_file_path.parent / "_models.yml"
            self.file_manager.write_file(yml_path)
        model_catalog = self.model_catalog
        # read the yml file
        # pass empty dict if no file contents returned
        full_yml_dict = self.file_manager.read_file(yml_path) or {}
        updated_yml = self.add_model_contract_to_yml(
            model_name=node.name, model_catalog=model_catalog, full_yml_dict=full_yml_dict
        )
        # write the updated yml to the file
        self.file_manager.write_file(yml_path, updated_yml)

    def add_model_version(
        self, prerelease: Optional[bool] = False, defined_in: Optional[os.PathLike] = None
    ) -> None:
        """Adds a model version to the model's yaml"""

        node = self.model_node
        yml_path = Path(node.patch_path.split("://")[1]) if node.patch_path else None
        original_file_path = Path(node.original_file_path) if node.original_file_path else None
        # if the model doesn't have a patch path, create a new yml file in the models directory
        # TODO - should we check if there's a model yml file in the models directory and append to it?
        if not yml_path:
            yml_path = original_file_path.parent / "_models.yml"
            self.file_manager.write_file(yml_path)
        # read the yml file
        # pass empty dict if no file contents returned
        full_yml_dict = self.file_manager.read_file(yml_path) or {}
        updated_yml = self.add_model_version_to_yml(
            model_name=node.name,
            full_yml_dict=full_yml_dict,
            prerelease=prerelease,
            defined_in=defined_in,
        )
        # write the updated yml to the file
        self.file_manager.write_file(yml_path, updated_yml)
        # create the new version file

        # if we're incrementing the version, write the new version file with a copy of the code
        latest_version = node.latest_version if node.latest_version else 0
        last_version_file_name = f"{node.name}_v{latest_version}.{node.language}"
        next_version_file_name = (
            f"{defined_in}.{node.language}"
            if defined_in
            else f"{node.name}_v{latest_version + 1}.{node.language}"
        )
        next_version_path = original_file_path.parent / next_version_file_name
        last_version_path = original_file_path.parent / last_version_file_name

        # if this is the first version, rename the original file to the next version
        if not node.latest_version:
            Path(self.project_path).joinpath(original_file_path).rename(
                Path(self.project_path).joinpath(next_version_path)
            )
        else:
            # if existing versions, create the new one
            self.file_manager.write_file(next_version_path, node.raw_code)
            # if the existing version doesn't use the _v{version} naming convention, rename it to the previous version
            if not original_file_path.root.endswith(f"_v{latest_version}.{node.language}"):
                Path(self.project_path).joinpath(original_file_path).rename(
                    Path(self.project_path).joinpath(last_version_path)
                )
