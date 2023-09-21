from dataclasses import dataclass
from enum import Enum
from typing import Optional, Set, Union

from dbt.contracts.graph.nodes import CompiledNode, ModelNode, SourceDefinition
from dbt.node_types import AccessType

from dbt_meshify.change import ChangeSet, EntityType, Operation, ResourceChange
from dbt_meshify.dbt_projects import BaseDbtProject, DbtProject, PathedProject
from dbt_meshify.utilities.contractor import Contractor
from dbt_meshify.utilities.dependencies import DependenciesUpdater
from dbt_meshify.utilities.grouper import ResourceGrouper
from dbt_meshify.utilities.references import ReferenceUpdater, get_latest_file_change


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
                model.relation_name.lower()
                for model in project.models.values()
                if model.relation_name is not None
            },
            target_relations={
                source.relation_name.lower()
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
                model.relation_name.lower()
                for model in other_project.models.values()
                if model.relation_name is not None
            },
            target_relations={
                source.relation_name.lower()
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
                model.relation_name.lower()
                for model in project.models.values()
                if model.relation_name is not None
            },
            target_relations={
                model.relation_name.lower()
                for model in other_project.models.values()
                if model.relation_name is not None
            },
        )

        # find the children of the shared models in the downstream project
        package_children = [
            {
                "upstream_resource": project.model_relation_names[relation],
                "downstream_resource": child,
            }
            for relation in relations
            for child in other_project.manifest.child_map[project.model_relation_names[relation]]
        ]

        forward_dependencies = {
            ProjectDependency(
                upstream_resource=child["upstream_resource"],
                upstream_project_name=project.name,
                downstream_resource=child["downstream_resource"],
                downstream_project_name=other_project.name,
                type=ProjectDependencyType.Package,
            )
            for child in package_children
        }

        # find the children of the shared models in the downstream project
        backward_package_children = [
            {
                "upstream_resource": other_project.model_relation_names[relation],
                "downstream_resource": child,
            }
            for relation in relations
            for child in project.manifest.child_map[other_project.model_relation_names[relation]]
        ]

        backward_dependencies = {
            ProjectDependency(
                upstream_resource=child["upstream_resource"],
                upstream_project_name=other_project.name,
                downstream_resource=child["downstream_resource"],
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

    def generate_delete_source_properties(
        self, project: PathedProject, resource: SourceDefinition
    ) -> ResourceChange:
        """Create a ResourceChange that removes a source from its properties YAML file."""

        return ResourceChange(
            operation=Operation.Remove,
            entity_type=EntityType.Source,
            identifier=resource.name,
            path=project.resolve_patch_path(resource),
            data={},
            source_name=resource.source_name,
        )

    def resolve_dependency(
        self,
        dependency: ProjectDependency,
        upstream_project: DbtProject,
        downstream_project: DbtProject,
        current_change_set: Optional[ChangeSet] = None,
    ) -> ChangeSet:
        upstream_manifest_entry = upstream_project.get_manifest_node(dependency.upstream_resource)
        if not upstream_manifest_entry:
            raise ValueError(
                f"Could not find upstream resource {dependency.upstream_resource} in project {upstream_project.name}"
            )
        downstream_manifest_entry = downstream_project.get_manifest_node(
            dependency.downstream_resource
        )

        if downstream_manifest_entry is None:
            raise TypeError(
                f"Unable to find the downstream entity {dependency.downstream_resource} in downstream project."
            )

        resource_grouper = ResourceGrouper(project=upstream_project)
        contractor = Contractor(project=upstream_project)
        reference_updater = ReferenceUpdater(project=downstream_project)

        change_set = ChangeSet()

        if isinstance(upstream_manifest_entry, ModelNode):
            change_set.add(
                resource_grouper.generate_access(
                    model=upstream_manifest_entry, access=AccessType.Public
                )
            )

            change_set.add(contractor.generate_contract(model=upstream_manifest_entry))

        if dependency.type == ProjectDependencyType.Source:
            for child in downstream_project.manifest.child_map[dependency.downstream_resource]:
                child_resource = downstream_project.get_manifest_node(child)

                if child_resource is None:
                    raise Exception("Identified child resource not found in downstream project.")
                elif not isinstance(child_resource, CompiledNode):
                    raise TypeError(
                        "The child resource identified in this Source dependency is not a CompiledNode. "
                        f"{child_resource.unique_id}"
                    )
                elif not isinstance(upstream_manifest_entry, CompiledNode):
                    raise TypeError(
                        "The upstream resource identified in this Source dependency is not a CompiledNode. "
                        f"{upstream_manifest_entry.unique_id}"
                    )

                change_set.add(
                    reference_updater.replace_source_with_refs(
                        resource=child_resource,
                        upstream_resource=upstream_manifest_entry,
                        source_unique_id=dependency.downstream_resource,
                    )
                )

                if not isinstance(downstream_manifest_entry, SourceDefinition):
                    raise TypeError(
                        "The downstream resource identified in this Source dependency is not a Source. "
                        f"{downstream_manifest_entry.unique_id}"
                    )

                change_set.add(
                    self.generate_delete_source_properties(
                        project=downstream_project, resource=downstream_manifest_entry
                    )
                )

        if dependency.type == ProjectDependencyType.Package:
            if not isinstance(downstream_manifest_entry, CompiledNode):
                raise TypeError(
                    f"The downstream resource identified in this Package dependency is not a CompiledNode. "
                    f"{downstream_manifest_entry.unique_id}"
                )
            elif not isinstance(upstream_manifest_entry, CompiledNode):
                raise TypeError(
                    f"The upstream resource identified in this Package dependency is not a CompiledNode. "
                    f"{upstream_manifest_entry.unique_id}"
                )

            if current_change_set:
                previous_change = get_latest_file_change(
                    changeset=current_change_set,
                    identifier=downstream_manifest_entry.name,
                    path=downstream_project.resolve_file_path(downstream_manifest_entry),
                )

            code_to_update = (
                previous_change.data
                if (previous_change and previous_change.data)
                else downstream_manifest_entry.raw_code
            )

            change_set.add(
                reference_updater.generate_reference_update(
                    project_name=upstream_project.name,
                    downstream_node=downstream_manifest_entry,
                    upstream_node=upstream_manifest_entry,
                    code=code_to_update,
                    downstream_project=downstream_project,
                )
            )

            # for both types, add upstream project to downstream project's dependencies.yml

        change_set.add(
            DependenciesUpdater.update_dependencies_yml(
                upstream_project=upstream_project, downstream_project=downstream_project
            )
        )

        return change_set
