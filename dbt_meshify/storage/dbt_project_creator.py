from pathlib import Path
from typing import Optional, Set

from dbt.contracts.graph.nodes import ManifestNode
from dbt.node_types import AccessType
from loguru import logger

from dbt_meshify.dbt_projects import DbtSubProject
from dbt_meshify.storage.file_content_editors import (
    DbtMeshConstructor,
    filter_empty_dict_items,
)
from dbt_meshify.storage.file_manager import DbtFileManager
from dbt_meshify.utilities.grouper import ResourceGrouper


class DbtSubprojectCreator:
    """
    Takes a `DbtSubProject` and creates the directory structure and files for it.
    """

    def __init__(self, subproject: DbtSubProject, target_directory: Optional[Path] = None):
        self.subproject = subproject
        self.target_directory = target_directory if target_directory else subproject.path
        self.file_manager = DbtFileManager(
            read_project_path=subproject.parent_project.path,
            write_project_path=self.target_directory,
        )
        self.subproject_boundary_models = self._get_subproject_boundary_models()

    def _get_subproject_boundary_models(self) -> Set[str]:
        """
        get a set of boundary model unique_ids for all the selected resources
        """
        nodes = set(filter(lambda x: not x.startswith("source"), self.subproject.resources))
        parent_project_name = self.subproject.parent_project.name
        interface = ResourceGrouper.identify_interface(
            graph=self.subproject.graph.graph, selected_bunch=nodes
        )
        boundary_models = set(
            filter(
                lambda x: (x.startswith("model") and x.split('.')[1] == parent_project_name),
                interface,
            )
        )
        return boundary_models

    def write_project_file(self) -> None:
        """
        Writes the dbt_project.yml file for the subproject in the specified subdirectory
        """
        contents = self.subproject.project.to_dict()
        # was gettinga  weird serialization error from ruamel on this value
        # it's been deprecated, so no reason to keep it
        contents.pop("version")
        # this one appears in the project yml, but i don't think it should be written
        contents.pop("query-comment")
        contents = filter_empty_dict_items(contents)
        # project_file_path = self.target_directory / Path("dbt_project.yml")
        self.file_manager.write_file(Path("dbt_project.yml"), contents)

    def copy_packages_yml_file(self) -> None:
        """
        Writes the dbt_project.yml file for the subproject in the specified subdirectory
        """
        self.file_manager.copy_file(Path("packages.yml"))

    def copy_packages_dir(self) -> None:
        """
        Writes the dbt_packages directory to the subproject's subdirectory to avoid the need for an immediate `dbt deps` command
        """
        raise NotImplementedError("copy_packages_dir not implemented yet")

    def update_dependencies_yml(self) -> None:
        try:
            contents = self.file_manager.read_file(Path("dependencies.yml"))
        except FileNotFoundError:
            contents = {"projects": []}

        contents["projects"].append({"name": self.subproject.name})  # type: ignore
        self.file_manager.write_file(Path("dependencies.yml"), contents, writeback=True)

    def update_child_refs(self, resource: ManifestNode) -> None:
        downstream_models = [
            node.unique_id
            for node in self.subproject.manifest.nodes.values()
            if node.resource_type == "model" and resource.unique_id in node.depends_on.nodes  # type: ignore
        ]
        for model in downstream_models:
            model_node = self.subproject.get_manifest_node(model)
            if not model_node:
                raise KeyError(f"Resource {model} not found in manifest")
            meshify_constructor = DbtMeshConstructor(
                project_path=self.subproject.parent_project.path, node=model_node, catalog=None
            )
            meshify_constructor.update_model_refs(
                model_name=resource.name, project_name=self.subproject.name
            )

    def initialize(self) -> None:
        """Initialize this subproject as a full dbt project at the provided `target_directory`."""
        subproject = self.subproject
        for unique_id in subproject.resources | subproject.custom_macros | subproject.groups:
            resource = subproject.get_manifest_node(unique_id)
            catalog = subproject.get_catalog_entry(unique_id)
            if not resource:
                raise KeyError(f"Resource {unique_id} not found in manifest")
            meshify_constructor = DbtMeshConstructor(
                project_path=subproject.parent_project.path, node=resource, catalog=catalog
            )
            if resource.resource_type in ["model", "test", "snapshot", "seed"]:
                # ignore generic tests, as moving the yml entry will move the test too
                if resource.resource_type == "test" and len(resource.unique_id.split(".")) == 4:
                    continue
                if resource.unique_id in self.subproject_boundary_models:
                    logger.info(
                        f"Adding contract to and publicizing boundary node {resource.unique_id}"
                    )
                    try:
                        meshify_constructor.add_model_contract()
                        meshify_constructor.add_model_access(access_type=AccessType.Public)
                        logger.success(
                            f"Successfully added contract to and publicized boundary node {resource.unique_id}"
                        )
                    except Exception as e:
                        logger.error(
                            f"Failed to add contract to and publicize boundary node {resource.unique_id}"
                        )
                        logger.exception(e)
                    # apply access method too
                    logger.info(f"Updating ref functions for children of {resource.unique_id}...")
                    try:
                        self.update_child_refs(resource)
                        logger.success(
                            f"Successfully updated ref functions for children of {resource.unique_id}"
                        )
                    except Exception as e:
                        logger.error(
                            f"Failed to update ref functions for children of {resource.unique_id}"
                        )
                        logger.exception(e)

                logger.info(
                    f"Moving {resource.unique_id} and associated YML to subproject {subproject.name}..."
                )
                try:
                    self.move_resource(meshify_constructor)
                    self.move_resource_yml_entry(meshify_constructor)
                    logger.success(
                        f"Successfully moved {resource.unique_id} and associated YML to subproject {subproject.name}"
                    )
                except Exception as e:
                    logger.error(
                        f"Failed to move {resource.unique_id} and associated YML to subproject {subproject.name}"
                    )
                    logger.exception(e)

            elif resource.resource_type in ["macro", "group"]:
                logger.info(f"Copying {resource.unique_id} to subproject {subproject.name}...")
                try:
                    self.copy_resource(meshify_constructor)
                    logger.success(
                        f"Successfully copied {resource.unique_id} to subproject {subproject.name}"
                    )
                except Exception as e:
                    logger.error(
                        f"Failed to copy {resource.unique_id} to subproject {subproject.name}"
                    )
                    logger.exception(e)
            else:
                logger.info(
                    f"Moving resource {resource.unique_id} to subproject {subproject.name}..."
                )
                try:
                    self.move_resource_yml_entry(meshify_constructor)
                    logger.success(
                        f"Successfully moved resource {resource.unique_id} to subproject {subproject.name}"
                    )
                except Exception as e:
                    logger.error(
                        f"Failed to move resource {resource.unique_id} to subproject {subproject.name}"
                    )
                    logger.exception(e)

        self.write_project_file()
        self.copy_packages_yml_file()
        self.update_dependencies_yml()
        # self.copy_packages_dir()

    def move_resource(self, meshify_constructor: DbtMeshConstructor) -> None:
        """
        move a resource file from one project to another

        """
        current_path = meshify_constructor.get_resource_path()
        self.file_manager.move_file(current_path)

    def copy_resource(self, meshify_constructor: DbtMeshConstructor) -> None:
        """
        copy a resource file from one project to another

        """
        resource_path = meshify_constructor.get_resource_path()
        contents = self.file_manager.read_file(resource_path)
        self.file_manager.write_file(resource_path, contents)

    def move_resource_yml_entry(self, meshify_constructor: DbtMeshConstructor) -> None:
        """
        move a resource yml entry from one project to another
        """
        current_yml_path = meshify_constructor.get_patch_path()
        new_yml_path = self.file_manager.write_project_path / current_yml_path
        full_yml_entry = self.file_manager.read_file(current_yml_path)
        source_name = (
            meshify_constructor.node.source_name
            if hasattr(meshify_constructor.node, "source_name")
            else None
        )
        resource_entry, remainder = meshify_constructor.get_yml_entry(
            resource_name=meshify_constructor.node.name,
            full_yml=full_yml_entry,  # type: ignore
            resource_type=meshify_constructor.node.resource_type,
            source_name=source_name,
        )
        try:
            existing_yml = self.file_manager.read_file(new_yml_path)
        except FileNotFoundError:
            existing_yml = None
        new_yml_contents = meshify_constructor.add_entry_to_yml(
            resource_entry, existing_yml, meshify_constructor.node.resource_type  # type: ignore
        )
        self.file_manager.write_file(current_yml_path, new_yml_contents)
        if remainder:
            self.file_manager.write_file(current_yml_path, remainder, writeback=True)
        else:
            self.file_manager.delete_file(current_yml_path)
