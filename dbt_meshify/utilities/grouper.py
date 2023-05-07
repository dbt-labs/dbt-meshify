from dataclasses import dataclass
from typing import Optional, Tuple, List, Dict

from dbt.contracts.graph.nodes import Group
from dbt.contracts.graph.unparsed import Owner
from dbt.node_types import AccessType, NodeType

from dbt_meshify.dbt_projects import BaseDbtProject

@dataclass
class ResourceGroup:
    group: Group
    resources: Dict[str, AccessType]


class ResourceGrouper:
    """
    The ResourceGrouper is responsible for generating a dbt-core Group for a
    collection of resources in a DbtProject, and for providing access control
    recommendations based on the reference characteristics for each resource.
    """

    def __init__(self, project: BaseDbtProject):
        self.project = project
        # TODO: Construct a proper dbt-core Graph from a manifest.

    def create_group(
        self,
        name: str,
        owner: Owner,
        select: str,
        exclude: Optional[str] = None
    ) -> ResourceGroup:
        """Create a Group for a dbt project."""

        group = Group(
            name=name,
            owner=owner,
            package_name=self.project.name,
            original_file_path='',
            unique_id=f'group.{self.project.name}.{name}',
            path='',
            resource_type=NodeType.Group
        )

        # TODO: Use the project graph to identify access levels for each resource in the graph

        return ResourceGroup(
            group=group,
            resources={}
        )

