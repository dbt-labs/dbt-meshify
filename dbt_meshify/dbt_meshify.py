import copy
import logging
from typing import Optional, Dict, Set

from dbt_meshify.dbt_projects import DbtProject, DbtSubProject

logger = logging.getLogger()


class Meshify:
    """Meshify manages cross-project dependencies, and enables stitching of project references."""

    def __init__(self):
        self.relationships: Dict[str, Set[str]] = {}

    def register_relationship(self, source: str, target: str) -> None:
        """Register the relationship between two projects"""
        logger.debug(f"Registering the relationship between {source} and {target}")
        entry = self.relationships.get(source, set())
        entry.add(target)
        self.relationships[source] = entry

    def create_subproject(
        self,
        dbt_project: DbtProject | DbtSubProject,
        project_name: str,
        select: str,
        exclude: Optional[str] = None,
    ) -> DbtSubProject:
        """Create a new DbtSubProject using NodeSelection syntax."""

        subproject_resources = dbt_project.select_resources(select, exclude)

        # Construct a new project and inject the new manifest
        subproject = DbtSubProject(
            name=project_name, parent_project=dbt_project, resources=subproject_resources
        )

        # Record the subproject to create a cross-project dependency edge list
        self.register_relationship(dbt_project.name, project_name)

        return subproject
