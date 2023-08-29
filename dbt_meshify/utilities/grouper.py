import os
from pathlib import Path
from typing import Dict, Optional, Set, Tuple, Union

import networkx
from dbt.contracts.graph.nodes import Group, ModelNode
from dbt.contracts.graph.unparsed import Owner
from dbt.node_types import AccessType, NodeType
from loguru import logger

from dbt_meshify.change import ChangeSet, EntityType, Operation, ResourceChange
from dbt_meshify.dbt_projects import DbtProject, DbtSubProject


class ResourceGroupingException(BaseException):
    """Exceptions relating to grouping of resources."""


class ResourceGrouper:
    """
    The ResourceGrouper is responsible for generating a dbt-core Group for a
    collection of resources in a DbtProject, and for providing access control
    recommendations based on the reference characteristics for each resource.
    """

    def __init__(self, project: Union[DbtProject, DbtSubProject]):
        self.project = project

    @classmethod
    def identify_interface(cls, graph: networkx.Graph, selected_bunch: Set[str]) -> Set[str]:
        """
        Given a graph, find the interface nodes, where interface nodes are the nodes that
        either have downstream consumers outside the selected bunch OR are nodes
        with no downstream consumers.
        """
        boundary_nodes = {edge[0] for edge in networkx.edge_boundary(graph, selected_bunch)}
        leaf_nodes = {node for node, out_degree in graph.out_degree() if out_degree == 0}
        return boundary_nodes | leaf_nodes

    @classmethod
    def classify_resource_access(cls, graph, nodes):
        """
        Identify what access types each node should have. We can make some simplifying assumptions about
        recommended access for a group.

        For example, interfaces (nodes on the boundary of a subgraph or leaf nodes) should be protected,
        whereas nodes that are not a referenced are safe for a private access level.
        """
        boundary_nodes = cls.identify_interface(graph, nodes)
        resources = {
            node: AccessType.Protected if node in boundary_nodes else AccessType.Private
            for node in nodes
        }
        logger.info(f"Identified resource access types based on the graph: {resources}")
        return resources

    @classmethod
    def clean_subgraph(cls, graph: networkx.DiGraph) -> networkx.DiGraph:
        """Generate a subgraph that does not contain test resource types."""
        test_nodes = set(node for node in graph.nodes if node.startswith("test"))
        return graph.subgraph(set(graph.nodes) - test_nodes)

    def _generate_resource_group(
        self,
        name: str,
        owner: Owner,
        path: os.PathLike,
        select: str,
        exclude: Optional[str] = None,
        selector: Optional[str] = None,
    ) -> Tuple[Group, Dict[str, AccessType]]:
        """Generate the ResourceGroup that we want to apply to the project."""

        group = Group(
            name=name,
            owner=owner,
            package_name=self.project.name,
            original_file_path=os.path.relpath(path, self.project.path),
            unique_id=f"group.{self.project.name}.{name}",
            path=os.path.relpath(path, self.project.path / Path("models")),
            resource_type=NodeType.Group,
        )

        if not (select or exclude or selector):
            nodes = set()
        else:
            nodes = self.project.select_resources(
                select=select, exclude=exclude, selector=selector, output_key="unique_id"
            )

        logger.info(f"Selected {len(nodes)} resources: {nodes}")
        # Check if any of the selected nodes are already in a group of a different name. If so, raise an exception.
        nodes = set(filter(lambda x: not x.startswith("source"), nodes))
        for node in nodes:
            existing_group = self.project.manifest.nodes[node].config.group

            if existing_group is None or existing_group == group.name:
                continue

            raise ResourceGroupingException(
                f"The node {node} has been selected for addition to a new group, however {node} is already part "
                f"of the {existing_group} group. Please remove {node} from its group and try again."
            )

        cleaned_subgraph = self.clean_subgraph(self.project.graph.graph)
        resources = self.classify_resource_access(cleaned_subgraph, nodes)

        return group, resources

    def generate_access(
        self, model: ModelNode, access: AccessType, group: Optional[Group] = None
    ) -> ResourceChange:
        """Generate a Change to set the access level for a model."""

        data: Dict[str, str] = {"name": model.name, "access": access.value}
        if group:
            data["group"] = group.name

        patch_path = self.project.resolve_patch_path(model)

        return ResourceChange(
            operation=Operation.Update if patch_path.exists() else Operation.Add,
            entity_type=EntityType.Model,
            identifier=model.name,
            path=patch_path,
            data=data,
        )

    def add_group(
        self,
        name: str,
        owner: Owner,
        path: Path,
        select: str,
        exclude: Optional[str] = None,
        selector: Optional[str] = None,
    ) -> ChangeSet:
        """Create a ResourceGroup for a dbt project."""

        group, resources = self._generate_resource_group(
            name, owner, path, select, exclude, selector
        )

        changes = ChangeSet()

        changes.add(
            ResourceChange(
                operation=Operation.Add,
                entity_type=EntityType.Group,
                identifier=group.name,
                path=path,
                data={"name": group.name, "owner": group.owner.to_dict()},
            )
        )

        for resource, access_type in resources.items():
            if not resource.startswith("model"):
                continue

            model: ModelNode = self.project.models[resource]
            changes.add(self.generate_access(model, access_type, group))

        return changes
