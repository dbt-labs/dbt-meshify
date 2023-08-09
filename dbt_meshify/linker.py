from dataclasses import dataclass
from enum import Enum
from typing import Set, Union

from dbt.node_types import AccessType
from loguru import logger

from dbt_meshify.dbt_projects import BaseDbtProject, DbtProject
from dbt_meshify.exceptions import FatalMeshifyException, FileEditorException
from dbt_meshify.storage.dbt_project_editors import DbtProjectEditor, YMLOperationType
from dbt_meshify.storage.file_content_editors import DbtMeshConstructor


class ProjectDependencyType(str, Enum):
    """ProjectDependencyTypes define how the dependency relationship was defined."""

    Source = "source"
    Package = "package"


@dataclass
class ProjectDependency:
    """ProjectDependencies define shared resources between two different projects"""

    upstream_resource: str
    upstream_project_name: str
    downstream_resource: str
    downstream_project_name: str
    type: ProjectDependencyType

    def __key(self):
        return self.upstream_resource, self.downstream_resource, self.type

    def __hash__(self):
        return hash(self.__key())

    def __eq__(self, other):
        if isinstance(other, ProjectDependency):
            return self.__key() == other.__key()
        return NotImplemented


class Linker:
    """
    Linker computes the metagraph between separate DbtProjects. This includes
    providing an interface for mapping the dependencies between projects be
    it source-hacked models, imported packages, or explicit cross-project
    references.
    """

    @staticmethod
    def _find_relation_dependencies(
        source_relations: Set[str], target_relations: Set[str]
    ) -> Set[str]:
        """
        Identify dependencies between projects using shared relation names.
        """
        return source_relations.intersection(target_relations)

    def _source_dependencies(
        self,
        project: Union[BaseDbtProject, DbtProject],
        other_project: Union[BaseDbtProject, DbtProject],
    ) -> Set[ProjectDependency]:
        """
        Identify source-hack dependencies between projects.

        Source hacking occurs when Project A defines a model, and a downstream project (Project B)
        defines a source for the materialization of that same model.
        """

        relations = self._find_relation_dependencies(
            source_relations={
                model.relation_name
                for model in project.models.values()
                if model.relation_name is not None
            },
            target_relations={
                source.relation_name
                for source in other_project.sources().values()
                if source.relation_name is not None
            },
        )

        forward_dependencies = {
            ProjectDependency(
                upstream_resource=project.model_relation_names[relation],
                upstream_project_name=project.name,
                downstream_resource=other_project.source_relation_names[relation],
                downstream_project_name=other_project.name,
                type=ProjectDependencyType.Source,
            )
            for relation in relations
        }

        backwards_relations = self._find_relation_dependencies(
            source_relations={
                model.relation_name
                for model in other_project.models.values()
                if model.relation_name is not None
            },
            target_relations={
                source.relation_name
                for source in project.sources().values()
                if source.relation_name is not None
            },
        )

        backward_dependencies = {
            ProjectDependency(
                upstream_resource=other_project.model_relation_names[relation],
                upstream_project_name=other_project.name,
                downstream_resource=project.source_relation_names[relation],
                downstream_project_name=project.name,
                type=ProjectDependencyType.Source,
            )
            for relation in backwards_relations
        }

        return forward_dependencies | backward_dependencies

    def _package_dependencies(
        self,
        project: Union[BaseDbtProject, DbtProject],
        other_project: Union[BaseDbtProject, DbtProject],
    ) -> Set[ProjectDependency]:
        """
        Identify package-imported dependencies between projects.

        Imported project dependencies occur when Project A defines a model, and a downstream
        project (Project B) imports Project A and references the model.
        """

        if (
            project.project_id not in other_project.installed_packages()
            and other_project.project_id not in project.installed_packages()
        ):
            return set()

        # find which models are in both manifests
        relations = self._find_relation_dependencies(
            source_relations={
                model.relation_name
                for model in project.models.values()
                if model.relation_name is not None
            },
            target_relations={
                model.relation_name
                for model in other_project.models.values()
                if model.relation_name is not None
            },
        )

        # find the children of the shared models in the downstream project
        package_children = [
            {
                'upstream_resource': project.model_relation_names[relation],
                'downstream_resource': child,
            }
            for relation in relations
            for child in other_project.manifest.child_map[project.model_relation_names[relation]]
        ]

        forward_dependencies = {
            ProjectDependency(
                upstream_resource=child['upstream_resource'],
                upstream_project_name=project.name,
                downstream_resource=child['downstream_resource'],
                downstream_project_name=other_project.name,
                type=ProjectDependencyType.Package,
            )
            for child in package_children
        }

        # find the children of the shared models in the downstream project
        backward_package_children = [
            {
                'upstream_resource': other_project.model_relation_names[relation],
                'downstream_resource': child,
            }
            for relation in relations
            for child in project.manifest.child_map[other_project.model_relation_names[relation]]
        ]

        backward_dependencies = {
            ProjectDependency(
                upstream_resource=child['upstream_resource'],
                upstream_project_name=other_project.name,
                downstream_resource=child['downstream_resource'],
                downstream_project_name=project.name,
                type=ProjectDependencyType.Package,
            )
            for child in backward_package_children
        }

        return forward_dependencies | backward_dependencies

    def dependencies(
        self,
        project: Union[BaseDbtProject, DbtProject],
        other_project: Union[BaseDbtProject, DbtProject],
    ) -> Set[ProjectDependency]:
        """Detect dependencies between two projects and return a list of resources shared."""

        dependencies = set()

        # Detect Source Dependencies
        source_dependencies = self._source_dependencies(project, other_project)
        dependencies.update(source_dependencies)

        # Detect package-defined dependencies
        package_dependencies = self._package_dependencies(project, other_project)
        dependencies.update(package_dependencies)

        return dependencies

    def resolve_dependency(
        self,
        dependency: ProjectDependency,
        upstream_project: DbtProject,
        downstream_project: DbtProject,
    ):
        upstream_manifest_entry = upstream_project.get_manifest_node(dependency.upstream_resource)
        if not upstream_manifest_entry:
            raise ValueError(
                f"Could not find upstream resource {dependency.upstream_resource} in project {upstream_project.name}"
            )
        downstream_manifest_entry = downstream_project.get_manifest_node(
            dependency.downstream_resource
        )
        upstream_catalog_entry = upstream_project.get_catalog_entry(dependency.upstream_resource)
        upstream_mesh_constructor = DbtMeshConstructor(
            project_path=upstream_project.path,
            node=upstream_manifest_entry,  # type: ignore
            catalog=upstream_catalog_entry,
        )

        downstream_mesh_constructor = DbtMeshConstructor(
            project_path=downstream_project.path,
            node=downstream_manifest_entry,  # type: ignore
            catalog=None,
        )
        downstream_editor = DbtProjectEditor(downstream_project)
        # for either dependency type, add contracts and make model public
        try:
            upstream_mesh_constructor.add_model_access(AccessType.Public)
            logger.success(
                f"Successfully update model access : {dependency.upstream_resource} is now {AccessType.Public}"
            )
        except FatalMeshifyException as e:
            logger.error(f"Failed to update model access: {dependency.upstream_resource}")
            raise e
        try:
            upstream_mesh_constructor.add_model_contract()
            logger.success(f"Successfully added contract to model: {dependency.upstream_resource}")
        except FatalMeshifyException as e:
            logger.error(f"Failed to add contract to model: {dependency.upstream_resource}")
            raise e

        if dependency.type == ProjectDependencyType.Source:
            for child in downstream_project.manifest.child_map[dependency.downstream_resource]:
                constructor = DbtMeshConstructor(
                    project_path=downstream_project.path,
                    node=downstream_project.get_manifest_node(child),  # type: ignore
                    catalog=None,
                )
                try:
                    constructor.replace_source_with_refs(
                        source_unique_id=dependency.downstream_resource,
                        model_unique_id=dependency.upstream_resource,
                    )
                    logger.success(
                        f"Successfully replaced source function with ref to upstream resource: {dependency.downstream_resource} now calls {dependency.upstream_resource} directly"
                    )
                except FileEditorException as e:
                    logger.error(
                        f"Failed to replace source function with ref to upstream resource"
                    )
                    raise e
                try:
                    downstream_editor.update_resource_yml_entry(
                        downstream_mesh_constructor, operation_type=YMLOperationType.Delete
                    )
                    logger.success(
                        f"Successfully deleted unnecessary source: {dependency.downstream_resource}"
                    )
                except FatalMeshifyException as e:
                    logger.error(f"Failed to delete unnecessary source")
                    raise e

        if dependency.type == ProjectDependencyType.Package:
            try:
                downstream_mesh_constructor.update_model_refs(
                    model_name=upstream_manifest_entry.name,
                    project_name=dependency.upstream_project_name,
                )
                logger.success(
                    f"Successfully updated model refs: {dependency.downstream_resource} now references {dependency.upstream_resource}"
                )
            except FileEditorException as e:
                logger.error(f"Failed to update model refs")
                raise e

        # for both types, add upstream project to downstream project's dependencies.yml
        try:
            downstream_editor.update_dependencies_yml(parent_project=upstream_project.name)
            logger.success(
                f"Successfully added {dependency.upstream_project_name} to {dependency.downstream_project_name}'s dependencies.yml"
            )
        except FileEditorException as e:
            logger.error(
                f"Failed to add {dependency.upstream_project_name} to {dependency.downstream_project_name}'s dependencies.yml"
            )
            raise e
