from dataclasses import dataclass
from enum import Enum
from typing import Set

from dbt_meshify.dbt_projects import BaseDbtProject


class ProjectDependencyType(str, Enum):
    """ProjectDependencyTypes define how the dependency relationship was defined."""

    Source = "source"
    Package = "package"


@dataclass
class ProjectDependency:
    """ProjectDependencies define shared resources between two different projects"""

    upstream: str
    downstream: str
    type: ProjectDependencyType

    def __key(self):
        return self.upstream, self.downstream, self.type

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
        Identify dependencies between projects using shared relations.
        """
        return source_relations.intersection(target_relations)

    def _source_dependencies(
        self, project: BaseDbtProject, other_project: BaseDbtProject
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
                upstream=project.model_relation_names[relation],
                downstream=other_project.source_relation_names[relation],
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
                upstream=other_project.model_relation_names[relation],
                downstream=project.source_relation_names[relation],
                type=ProjectDependencyType.Source,
            )
            for relation in backwards_relations
        }

        return forward_dependencies | backward_dependencies

    def _package_dependencies(
        self, project: BaseDbtProject, other_project: BaseDbtProject
    ) -> Set[ProjectDependency]:
        """
        Identify package-imported dependencies between projects.

        Imported project dependencies occur when Project A defines a model, and a downstream
        project (Project B) imports Project A and references the model.
        """

        if project.project_id not in other_project.installed_packages():
            return set()

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

        return {
            ProjectDependency(
                upstream=project.model_relation_names[relation],
                downstream=other_project.model_relation_names[relation],
                type=ProjectDependencyType.Package,
            )
            for relation in relations
        }

    def dependencies(
        self, project: BaseDbtProject, other_project: BaseDbtProject
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
