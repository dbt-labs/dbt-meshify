from pathlib import Path
from typing import Optional

from dbt_meshify.dbt_projects import DbtSubProject
from dbt_meshify.storage.file_manager import DbtFileManager
from dbt_meshify.storage.yaml_editors import DbtMeshConstructor, filter_empty_dict_items


class DbtSubprojectCreator:
    """
    Takes a dbt subproject and creates the directory structure and files for it.
    """

    def __init__(self, subproject: DbtSubProject, target_directory: Optional[Path] = None):
        self.subproject = subproject
        self.target_directory = target_directory if target_directory else subproject.default_path
        self.file_manager = DbtFileManager(
            read_project_path=subproject.parent_project.path,
            write_project_path=self.target_directory,
        )

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

    def initialize(self) -> None:
        """Initialize this subproject as a full dbt project at the provided `target_directory`."""
        subproject = self.subproject
        for unique_id in subproject.resources | subproject.custom_macros:
            resource = subproject.get_manifest_node(unique_id)
            if not resource:
                raise KeyError(f"Resource {unique_id} not found in manifest")
            meshify_constructor = DbtMeshConstructor(
                project_path=subproject.parent_project.path, node=resource, catalog=None
            )
            if resource.resource_type in ["model", "test", "snapshot", "seed"]:
                # ignore generic tests, as moving the yml entry will move the test too
                if resource.resource_type == "test" and len(resource.unique_id.split(".")) == 4:
                    continue
                self.move_resource(meshify_constructor)
                self.move_resource_yml_entry(meshify_constructor)
            elif resource.resource_type == "macro":
                self.copy_resource(meshify_constructor)
            else:
                self.move_resource_yml_entry(meshify_constructor)

        self.write_project_file()
        self.copy_packages_yml_file()

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
