from pathlib import Path
from typing import Dict, Optional, Set

from dbt.contracts.graph.nodes import (
    CompiledNode,
    GenericTestNode,
    ModelNode,
    NodeType,
    Resource,
    SnapshotNode,
)
from dbt.node_types import AccessType
from loguru import logger

from dbt_meshify.change import (
    ChangeSet,
    EntityType,
    FileChange,
    Operation,
    ResourceChange,
)
from dbt_meshify.dbt_projects import DbtSubProject
from dbt_meshify.storage.file_content_editors import NamedList, filter_empty_dict_items
from dbt_meshify.storage.file_manager import YAMLFileManager, yaml
from dbt_meshify.utilities.contractor import Contractor
from dbt_meshify.utilities.dependencies import DependenciesUpdater
from dbt_meshify.utilities.grouper import ResourceGrouper
from dbt_meshify.utilities.references import ReferenceUpdater


class DbtSubprojectCreator:
    """
    Takes a `DbtSubProject` and creates the directory structure and files for it.
    """

    def __init__(
        self,
        project: DbtSubProject,
    ):
        if not isinstance(project, DbtSubProject):
            raise TypeError(f"DbtSubprojectCreator requires a DbtSubProject, got {type(project)}")

        self.subproject = project

        self.project_boundary_models = self._get_subproject_boundary_models()

    def load_resource_yml(
        self, path: Path, name: str, resource_type: NodeType, nested_name: Optional[str] = None
    ) -> Dict:
        """Load the Model patch YAML for a given ModelNode."""
        raw_yml = YAMLFileManager.read_file(path) if path.exists() else {}

        if not isinstance(raw_yml, dict):
            raise Exception(
                f"Unexpected contents in {path}. Expected yaml but found {type(raw_yml)}."
            )

        if resource_type == NodeType.Source:
            if nested_name is None:
                raise ValueError("Missing source name")

            source = NamedList(raw_yml.get(resource_type.pluralize(), [])).get(nested_name, {})
            table = source.get("tables", {}).get(name, {})
            source["tables"] = NamedList([table])

            return source

        return NamedList(raw_yml.get(resource_type.pluralize(), [])).get(name, {})

    def _get_subproject_boundary_models(self) -> Set[str]:
        """
        get a set of boundary model unique_ids for all the selected resources
        """
        nodes = set(filter(lambda x: not x.startswith("source"), self.subproject.resources))  # type: ignore
        parent_project_name = self.subproject.parent_project.name  # type: ignore
        cleaned_graph = ResourceGrouper.clean_subgraph(self.subproject.graph.graph)
        interface = ResourceGrouper.identify_interface(graph=cleaned_graph, selected_bunch=nodes)
        boundary_models = set(
            filter(
                lambda x: (x.startswith("model") and x.split(".")[1] == parent_project_name),
                interface,
            )
        )
        return boundary_models

    def write_project_file(self) -> FileChange:
        """
        Writes the dbt_project.yml file for the subproject in the specified subdirectory
        """
        contents = self.subproject.project.to_dict()
        # was getting a weird serialization error from ruamel on this value
        # it's been deprecated, so no reason to keep it
        contents.pop("version")
        # this one appears in the project yml, but i don't think it should be written
        contents.pop("query-comment")
        contents = filter_empty_dict_items(contents)

        # Serialize the `require-dbt-version` field into a string. This happens because dbt is expecting a list of
        # strings, but also accepts a singular string value, too. We can handle the latter case by checking the length
        # of each item in the list. If they're all one character long, then we're really working with a single version
        # string.
        if "require-dbt-version" in contents:
            if max([len(version) for version in contents["require-dbt-version"]]) == 1:
                contents["require-dbt-version"] = "".join(contents["require-dbt-version"])

        return FileChange(
            operation=Operation.Add,
            entity_type=EntityType.Code,
            identifier="dbt_project.yml",
            path=self.subproject.path / Path("dbt_project.yml"),
            data=yaml.dump(contents),
        )

    def copy_packages_yml_file(self) -> FileChange:
        """
        Writes the dbt_project.yml file for the subproject in the specified subdirectory
        """
        return FileChange(
            operation=Operation.Copy,
            entity_type=EntityType.Code,
            identifier="packages.yml",
            path=self.subproject.path / Path("packages.yml"),
            source=self.subproject.parent_project.path / Path("packages.yml"),
        )

    def initialize(self) -> ChangeSet:
        """Initialize this subproject as a full dbt project at the provided `target_directory`."""

        change_set = ChangeSet()

        subproject = self.subproject

        contractor = Contractor(project=subproject)
        grouper = ResourceGrouper(project=subproject)
        reference_updater = ReferenceUpdater(project=subproject)

        parent_contractor = Contractor(project=subproject.parent_project)
        parent_resource_grouper = ResourceGrouper(project=subproject.parent_project)

        logger.info(
            f"Identifying operations required to split {subproject.name} from {subproject.parent_project.name}."
        )

        for unique_id in subproject.resources | subproject.custom_macros | subproject.groups:
            resource = subproject.get_manifest_node(unique_id)
            if not resource:
                raise KeyError(f"Resource {unique_id} not found in manifest")

            if resource.resource_type in ["model", "test", "snapshot", "seed"]:
                # ignore generic tests, as moving the yml entry will move the test too
                if resource.resource_type == "test" and len(resource.unique_id.split(".")) == 4:
                    continue
                if resource.unique_id in self.project_boundary_models:
                    if isinstance(resource, ModelNode):
                        logger.debug(
                            f"Generate contract to and access for boundary node {resource.unique_id}"
                        )
                        change_set.add(contractor.generate_contract(resource))
                        change_set.add(
                            grouper.generate_access(model=resource, access=AccessType.Public)
                        )

                    # apply access method too
                    if isinstance(resource, CompiledNode):
                        logger.debug(
                            f"Updating ref functions for children of {resource.unique_id}..."
                        )
                        change_set.extend(
                            reference_updater.update_child_refs(resource, change_set)
                        )

                logger.debug(
                    f"Moving {resource.unique_id} and associated YML to subproject {subproject.name}..."
                )
                change_set.add(self.move_resource(resource))
                change_set.extend(self.move_resource_yml_entry(resource))

                if isinstance(resource, (ModelNode, GenericTestNode, SnapshotNode)) and any(
                    node
                    for node in resource.depends_on.nodes
                    if node in self.subproject.xproj_parents_of_resources
                ):
                    change_set.extend(reference_updater.update_parent_refs(resource))

            elif resource.resource_type in ["macro", "group"]:
                if hasattr(resource, "patch_path") and resource.patch_path:
                    change_set.add(self.copy_resource_yml(resource))
                change_set.add(self.copy_resource(resource))

            else:
                logger.debug(
                    f"Moving resource {resource.unique_id} to subproject {subproject.name}..."
                )

                change_set.extend(self.move_resource_yml_entry(resource))

        # add contracts and access to parents of split models
        for unique_id in subproject.xproj_parents_of_resources:
            resource = subproject.get_manifest_node(unique_id)

            if not resource:
                raise KeyError(f"Resource {unique_id} not found in manifest")

            if not isinstance(resource, ModelNode):
                continue

            change_set.add(parent_contractor.generate_contract(resource))

            change_set.add(
                parent_resource_grouper.generate_access(model=resource, access=AccessType.Public)
            )

        change_set.add(self.write_project_file())
        change_set.add(self.copy_packages_yml_file())
        change_set.add(
            DependenciesUpdater.update_dependencies_yml(
                upstream_project=self.subproject.parent_project,
                downstream_project=self.subproject,
                reversed=self.subproject.is_parent_of_parent_project,
            )
        )

        return change_set

        # self.copy_packages_dir()

    def move_resource(self, resource: Resource) -> FileChange:
        """
        move a resource file from one project to another
        """

        return FileChange(
            operation=Operation.Move,
            entity_type=EntityType.Code,
            identifier=resource.name,
            path=self.subproject.resolve_file_path(resource),
            source=self.subproject.parent_project.resolve_file_path(resource),
        )

    def copy_resource(self, resource: Resource) -> FileChange:
        """
        Copy a resource file from one project to another
        """

        return FileChange(
            operation=Operation.Copy,
            entity_type=EntityType.Code,
            identifier=resource.name,
            path=self.subproject.resolve_file_path(resource),
            source=self.subproject.parent_project.resolve_file_path(resource),
        )

    def copy_resource_yml(self, resource: Resource) -> ResourceChange:
        """
        copy a resource yaml from one project to another

        """

        current_yml_path = self.subproject.parent_project.resolve_patch_path(resource)
        new_yml_path = self.subproject.resolve_patch_path(resource)

        source_name = resource.source_name if hasattr(resource, "source_name") else None

        resource_entry = self.load_resource_yml(
            current_yml_path, resource.name, resource.resource_type, source_name
        )
        return ResourceChange(
            operation=Operation.Add,
            entity_type=EntityType(resource.resource_type.value),
            identifier=resource.name,
            path=new_yml_path,
            data=resource_entry,
            source_name=source_name,
        )

    def move_resource_yml_entry(self, resource: Resource) -> ChangeSet:
        """
        move a resource yml entry from one project to another
        """
        change_set = ChangeSet()
        current_yml_path = self.subproject.parent_project.resolve_patch_path(resource)
        new_yml_path = self.subproject.resolve_patch_path(resource)

        source_name = resource.source_name if hasattr(resource, "source_name") else None

        resource_entry = self.load_resource_yml(
            current_yml_path, resource.name, resource.resource_type, source_name
        )
        if resource_entry:
            change_set.extend(
                [
                    ResourceChange(
                        operation=Operation.Add,
                        entity_type=EntityType(resource.resource_type.value),
                        identifier=resource.name,
                        path=new_yml_path,
                        data=resource_entry,
                        source_name=source_name,
                    ),
                    ResourceChange(
                        operation=Operation.Remove,
                        entity_type=EntityType(resource.resource_type.value),
                        identifier=resource.name,
                        path=current_yml_path,
                        data={},
                        source_name=source_name,
                    ),
                ]
            )

        return change_set
