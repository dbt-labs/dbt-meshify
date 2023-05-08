from dataclasses import dataclass
from typing import Optional, Dict, Set

import networkx
from dbt.contracts.graph.nodes import Group
from dbt.contracts.graph.unparsed import Owner
from dbt.node_types import AccessType, NodeType

from dbt_meshify.dbt_projects import BaseDbtProject


class ResourceGroupingException(BaseException):
    """Exceptions relating to grouping of resources."""


@dataclass
class ResourceGroup:
    """
    A definition class for storing what Groups need to be created,
    and the resources and access levels within that Group.
    """

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

    @staticmethod
    def identify_interface(graph: networkx.Graph, selected_bunch: Set[str]) -> Set[str]:
        """
        Given a graph, find the interface nodes, where interface nodes are the nodes that
        either have downstream consumers outside the selected bunch OR are nodes
        with no downstream consumers.
        """
        boundary_nodes = networkx.node_boundary(graph, selected_bunch)
        leaf_nodes = {node for node, out_degree in graph.out_degree() if out_degree == 0}
        return boundary_nodes | leaf_nodes

    def create_group(
        self, name: str, owner: Owner, select: str, exclude: Optional[str] = None
    ) -> ResourceGroup:
        """Create a ResourceGroup for a dbt project."""

        group = Group(
            name=name,
            owner=owner,
            package_name=self.project.name,
            original_file_path="",
            # TODO: This seems a bit yucky. How does dbt-core generate unique_ids?
            unique_id=f"group.{self.project.name}.{name}",
            path="",
            resource_type=NodeType.Group,
        )

        nodes = self.project.select_resources(select, exclude, output_key="unique_id")

        # Check if any of the selected nodes are already in a group. If so, raise an exception.
        for node in nodes:
            existing_group = self.project.manifest.nodes[node].config.group

            if existing_group is None:
                continue

            raise ResourceGroupingException(
                f"The node {node} has been selected for addition to a new group, however {node} is already part "
                f"of the {existing_group} group. Please remove {node} from its group and try again."
            )

        # We can make some simplifying assumptions about recommended access for a group.
        # For example, interfaces (nodes on the boundary of a subgraph or leaf nodes) should be public,
        # whereas nodes that are not a referenced are safe for a private access level.

        boundary_nodes = self.identify_interface(self.project.graph.graph, nodes)
        resources = {
            node: AccessType.Public if node in boundary_nodes else AccessType.Private
            for node in nodes
        }

        return ResourceGroup(group=group, resources=resources)
