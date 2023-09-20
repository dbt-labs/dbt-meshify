from pathlib import Path
from typing import Any, Dict, Optional, Union

from dbt.contracts.graph.nodes import ModelNode
from loguru import logger

from dbt_meshify.change import (
    ChangeSet,
    EntityType,
    FileChange,
    Operation,
    ResourceChange,
)
from dbt_meshify.dbt_projects import DbtProject, DbtSubProject
from dbt_meshify.storage.file_content_editors import NamedList, safe_update
from dbt_meshify.storage.file_manager import FileManager, YAMLFileManager


class ModelVersionerException(Exception):
    """Exceptions raised during versioning of a Model."""


class ModelVersioner:
    """Class for creating and incrementing version Changes on Models."""

    def __init__(
        self,
        project: Union[DbtProject, DbtSubProject],
        file_manager: Optional[FileManager] = None,
    ) -> None:
        self.project = project
        self.file_manager = YAMLFileManager() if not file_manager else file_manager

    def load_model_yml(self, path: Path, name: str) -> Dict:
        """Load the Model patch YAML for a given ModelNode."""
        try:
            raw_yml = self.file_manager.read_file(path)
        except FileNotFoundError as e:
            logger.debug(f"Unable to load file for versioning. {path}. {e}")
            return {}

        if not isinstance(raw_yml, dict):
            raise Exception(
                f"Unexpected contents in {path}. Expected yaml but found {type(raw_yml)}."
            )

        return NamedList(raw_yml.get("models", [])).get(name, {})

    @staticmethod
    def get_model_versions(model_yml: Dict) -> NamedList:
        """
        Returns all versions defined in a model's YAML configuration.
        """
        return NamedList(index_field="v", source_list=model_yml.get("versions", []))

    @staticmethod
    def get_latest_model_version(model_versions: NamedList) -> int:
        """
        Models can have newer versions defined in YAML that are not the `latest_version`
        (current version). Parse out what the max version currently is.
        """
        if len(model_versions) == 0:
            return 0

        try:
            return max([int(v.get("v")) for v in model_versions.values()])
        except ValueError:
            raise ValueError("Version not an integer, can't increment version.")

    def add_version(self, model: ModelNode, defined_in: Optional[Path] = None) -> ChangeSet:
        """Pass add versioning to an existing model."""

        path = self.project.resolve_patch_path(model)
        model_path = self.project.path / model.original_file_path

        model_yml = self.load_model_yml(path, model.name)

        model_versions: NamedList = self.get_model_versions(model_yml)

        if len(model_versions) > 0:
            raise ModelVersionerException(
                f"The model {model.name} already has versions defined. Please use bump-version instead."
            )

        new_latest_version_number = 1
        new_version_data: Dict[str, Any] = {"v": new_latest_version_number}

        if defined_in:
            new_version_data["defined_in"] = defined_in

        model_versions[new_version_data["v"]] = new_version_data

        next_version_file_name = model_path.parent / Path(
            f"{defined_in}.{model.language}"
            if defined_in
            else f"{model.name}_v{new_latest_version_number}.{model.language}"
        )

        change_set = ChangeSet()

        change_set.add(
            ResourceChange(
                operation=Operation.Update if path.exists() else Operation.Add,
                entity_type=EntityType.Model,
                identifier=model.name,
                path=path,
                data={
                    "name": model.name,
                    "latest_version": new_latest_version_number,
                    "versions": model_versions.to_list(),
                },
            )
        )

        change_set.add(
            FileChange(
                operation=Operation.Move,
                path=next_version_file_name,
                entity_type=EntityType.Code,
                identifier=model.name,
                source=model_path,
            )
        )

        return change_set

    def bump_version(
        self,
        model: ModelNode,
        prerelease: bool = False,
        defined_in: Optional[Path] = None,
        model_override: Optional[Dict] = None,
    ) -> ChangeSet:
        """Increment a model's version, and create a new version definition and model file for the new version number"""

        path = self.project.resolve_patch_path(model)
        model_path = self.project.path / model.original_file_path

        model_yml = self.load_model_yml(path, model.name)
        if model_override:
            model_yml = safe_update(model_yml, model_override)

        model_versions: NamedList = self.get_model_versions(model_yml)
        greatest_version = self.get_latest_model_version(model_versions)

        if len(model_versions) == 0:
            raise ModelVersionerException(
                f"The model {model.name} does not have any versions defined. Please use add-version first."
            )

        # Within dbt-core, if unset `latest_version` defaults to the greatest version identifier.
        latest_version = model_yml.get("latest_version", greatest_version)

        # Bump versions
        new_greatest_version_number = greatest_version + 1
        new_latest_version_number = latest_version if prerelease else latest_version + 1

        # Setup the new version definitions
        new_version_data: Dict[str, Any] = {"v": new_greatest_version_number}
        if defined_in:
            new_version_data["defined_in"] = defined_in
        model_versions[new_version_data["v"]] = new_version_data

        # Determine the file name for the versioned model
        next_version_file_name = model_path.parent / Path(
            f"{defined_in}.{model.language}"
            if defined_in
            else f"{model.name}_v{new_greatest_version_number}.{model.language}"
        )

        change_set = ChangeSet()
        change_set.add(
            ResourceChange(
                operation=Operation.Update if path.exists() else Operation.Add,
                entity_type=EntityType.Model,
                identifier=model.name,
                path=path,
                data={
                    "name": model.name,
                    "latest_version": new_latest_version_number,
                    "versions": model_versions.to_list(),
                },
            )
        )
        change_set.add(
            FileChange(
                operation=Operation.Copy,
                path=next_version_file_name,
                entity_type=EntityType.Code,
                identifier=model.name,
                source=model_path,
            )
        )

        return change_set
