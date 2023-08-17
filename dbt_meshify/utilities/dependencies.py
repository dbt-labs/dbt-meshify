from typing import Union

from dbt_meshify.change import EntityType, FileChange, Operation
from dbt_meshify.dbt_projects import DbtProject, DbtSubProject
from dbt_meshify.storage.file_manager import YAMLFileManager, yaml


class DependenciesUpdater:
    @staticmethod
    def update_dependencies_yml(
        upstream_project: Union[DbtProject, DbtSubProject],
        downstream_project: Union[DbtProject, DbtSubProject],
        reversed: bool = False,
    ) -> FileChange:
        true_upstream_project = upstream_project
        true_downstream_project = downstream_project

        # TODO: Leverage project relationships to sort this out more correctly. This may
        # break if we're working with a "middle project" between two other projects.
        if reversed:
            true_upstream_project = downstream_project
            true_downstream_project = upstream_project

        try:
            contents = YAMLFileManager.read_file(true_downstream_project.path / "dependencies.yml")

        except FileNotFoundError:
            contents = {"projects": []}

        contents["projects"].append({"name": true_upstream_project.name})

        return FileChange(
            operation=Operation.Add,
            entity_type=EntityType.Code,
            identifier="dependencies.yml",
            path=true_downstream_project.path / "dependencies.yml",
            data=yaml.dump(contents),
        )
