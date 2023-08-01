from pathlib import Path
from typing import Any, Dict, Optional, Union

from dbt.contracts.graph.nodes import ModelNode

from dbt_meshify.change import Change, EntityType, Operation
from dbt_meshify.dbt_projects import DbtProject, DbtSubProject
from dbt_meshify.storage.file_content_editors import NamedList
from dbt_meshify.storage.file_manager import DbtFileManager


class ModelVersioner:
    """Class for creating and incrementing version Changes on Models."""

    def __init__(self, project: Union[DbtProject, DbtSubProject]) -> None:
        self.project = project
        self.file_manager = DbtFileManager(read_project_path=project.path)

    def load_model_yml(self, path: Path) -> Dict:
        """Load the Model patch YAML for a given ModelNode."""
        model_yml = self.file_manager.read_file(path)
        if not isinstance(model_yml, dict):
            raise Exception(
                f"Unexpected contents in {path}. Expected yaml but found {type(model_yml)}."
            )
        return model_yml

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
            return max([int(v.get("v")) for v in model_versions])
        except ValueError:
            raise ValueError("Version not an integer, can't increment version.")

    def generate_version(
        self, model: ModelNode, prerelease: bool = False, defined_in: Optional[Path] = None
    ) -> Change:
        """Create a Change that adds a Version config to a Model."""

        path = self.project.resolve_patch_path(model)
        model_yml = self.load_model_yml(path)
        model_versions: NamedList = self.get_model_versions(model_yml)

        current_version = model_yml.get("latest_version", 0)
        latest_version = self.get_latest_model_version(model_versions)

        new_latest_version_number = latest_version
        new_current_version_number = current_version

        # TODO: This logic is reducible.
        if not model_versions:
            new_latest_version_number = 1
            new_current_version_number = current_version + 1

        # if the model has versions, add the next version
        # if prerelease flag is true, do not increment the latest_version
        elif prerelease:
            new_latest_version_number = latest_version + 1
        else:
            new_latest_version_number = latest_version + 1
            new_current_version_number = current_version + 1

        new_version_data: Dict[str, Any] = {"v": new_latest_version_number}

        # add the defined_in key if it exists
        if defined_in:
            new_version_data["defined_in"] = defined_in

        model_versions[new_version_data["v"]] = new_version_data

        return Change(
            operation=Operation.Update,
            entity_type=EntityType.Model,
            identifier=model.name,
            path=path,
            data={
                "name": model.name,
                "latest_version": new_current_version_number,
                "versions": model_versions.to_list(),
            },
        )
