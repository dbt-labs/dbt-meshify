from typing import Union

from dbt_meshify.change import EntityType, Operation, ResourceChange
from dbt_meshify.dbt_projects import DbtProject, DbtSubProject


class DependenciesUpdater:
    @staticmethod
    def update_dependencies_yml(
        upstream_project: Union[DbtProject, DbtSubProject],
        downstream_project: Union[DbtProject, DbtSubProject],
        reversed: bool = False,
    ) -> ResourceChange:
        true_upstream_project = upstream_project
        true_downstream_project = downstream_project

        # TODO: Leverage project relationships to sort this out more correctly. This may
        # break if we're working with a "middle project" between two other projects.
        if reversed:
            true_upstream_project = downstream_project
            true_downstream_project = upstream_project

        path = true_downstream_project.path / "dependencies.yml"

        return ResourceChange(
            operation=Operation.Update if path.exists() else Operation.Add,
            entity_type=EntityType.Project,
            identifier=true_upstream_project.name,
            path=path,
            data={"name": true_upstream_project.name},
        )
