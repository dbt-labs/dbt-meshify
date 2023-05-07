from dataclasses import dataclass
from typing import Optional, Dict, Set

import networkx
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.graph.nodes import Group
from dbt.contracts.graph.unparsed import Owner
from dbt.graph import Graph
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
        self.graph = self._load_graph(self.project.manifest)

    @staticmethod
    def _load_graph(manifest: Manifest) -> Graph:
        """Generate a dbt Graph using a project manifest and the internal dbt Compiler and Linker."""
        # TODO: Consider using NetworkX directly to limit internal dbt dependencies.

        from dbt.compilation import Compiler
        from dbt.compilation import Linker

        compiler = Compiler(config={})
        linker = Linker()
        compiler.link_graph(linker=linker, manifest=manifest)
        return Graph(linker.graph)

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
        """Create a Group for a dbt project."""

        group = Group(
            name=name,
            owner=owner,
            package_name=self.project.name,
            original_file_path="",
            unique_id=f"group.{self.project.name}.{name}",
            path="",
            resource_type=NodeType.Group,
        )

        nodes = self.project.select_resources(select, exclude, output_key="unique_id")

        # We can make some simplifying assumptions about recommended access for a group.
        # For example, interfaces (nodes on the boundary of a subgraph or leaf nodes) should be public,
        # whereas nodes that are not a referenced are safe for a private access level.

        boundary_nodes = self.identify_interface(self.graph.graph, nodes)
        resources = {
            node: AccessType.Public if node in boundary_nodes else AccessType.Private
            for node in nodes
        }

        return ResourceGroup(group=group, resources=resources)
