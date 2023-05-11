import os
from pathlib import Path
from typing import Optional, Dict, Set, Tuple

import networkx
from dbt.contracts.graph.nodes import Group, ModelNode
from dbt.contracts.graph.unparsed import Owner
from dbt.node_types import AccessType, NodeType

from dbt_meshify.dbt_projects import DbtProject
from dbt_meshify.storage.file_manager import DbtFileManager
from dbt_meshify.storage.yaml_editors import DbtMeshYmlEditor


class ResourceGroupingException(BaseException):
    """Exceptions relating to grouping of resources."""


class ResourceGrouper:
    """
    The ResourceGrouper is responsible for generating a dbt-core Group for a
    collection of resources in a DbtProject, and for providing access control
    recommendations based on the reference characteristics for each resource.
    """

    def __init__(self, project: DbtProject):
        self.project = project
        self.meshify = DbtMeshYmlEditor()
        self.file_manager = DbtFileManager(read_project_path=project.path)

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

    def _generate_resource_group(
        self,
        name: str,
        owner: Owner,
        path: os.PathLike,
        select: str,
        exclude: Optional[str] = None,
    ) -> Tuple[Group, Dict[str, AccessType]]:
        """Generate the ResourceGroup that we want to apply to the project."""

        group = Group(
            name=name,
            owner=owner,
            package_name=self.project.name,
            original_file_path=os.path.relpath(path, self.project.path),
            # TODO: This seems a bit yucky. How does dbt-core generate unique_ids?
            unique_id=f"group.{self.project.name}.{name}",
            path=os.path.relpath(path, self.project.path / Path("models")),
            resource_type=NodeType.Group,
        )

        nodes = self.project.select_resources(select, exclude, output_key="unique_id")

        # Check if any of the selected nodes are already in a group of a different name. If so, raise an exception.
        for node in nodes:
            existing_group = self.project.manifest.nodes[node].config.group

            if existing_group is None or existing_group == group.name:
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

        return group, resources

    def add_group(
        self,
        name: str,
        owner: Owner,
        path: os.PathLike,
        select: str,
        exclude: Optional[str] = None,
    ) -> None:
        """Create a ResourceGroup for a dbt project."""

        group, resources = self._generate_resource_group(name, owner, path, select, exclude)

        group_path = Path(group.original_file_path)
        try:
            group_yml = self.file_manager.read_file(group_path)
        except FileNotFoundError:
            group_yml = {}

        output_yml = self.meshify.add_group_to_yml(group, group_yml)
        self.file_manager.write_file(group_path, output_yml)

        for resource, access_type in resources.items():
            model: ModelNode = self.project.manifest.nodes[resource]
            path = Path(model.patch_path.split("://")[1]) if model.patch_path else None
            try:
                file_yml = self.file_manager.read_file(path)
            except FileNotFoundError:
                file_yml = {}

            output_yml = self.meshify.add_group_and_access_to_model_yml(
                model.name, group, access_type, file_yml
            )

            self.file_manager.write_file(path, output_yml)
