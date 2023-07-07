import os
from pathlib import Path
from typing import Any, Dict, Optional, Set, Tuple, Union

import networkx
from dbt.contracts.graph.nodes import Group, ModelNode
from dbt.contracts.graph.unparsed import Owner
from dbt.node_types import AccessType, NodeType
from loguru import logger

from dbt_meshify.dbt_projects import DbtProject, DbtSubProject
from dbt_meshify.storage.file_content_editors import DbtMeshFileEditor
from dbt_meshify.storage.file_manager import DbtFileManager


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
        self.meshify = DbtMeshFileEditor()
        self.file_manager = DbtFileManager(read_project_path=project.path)

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

        For example, interfaces (nodes on the boundary of a subgraph or leaf nodes) should be public,
        whereas nodes that are not a referenced are safe for a private access level.
        """
        boundary_nodes = cls.identify_interface(graph, nodes)
        resources = {
            node: AccessType.Public if node in boundary_nodes else AccessType.Private
            for node in nodes
        }
        logger.info(f"Identified resource access types based on the graph: {resources}")
        return resources

    @classmethod
    def clean_subgraph(cls, graph: networkx.DiGraph) -> networkx.DiGraph:
        """Generate a subgraph that does not contain test resource types."""
        test_nodes = set(node for node in graph.nodes if node.startswith('test'))
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

    def add_group(
        self,
        name: str,
        owner: Owner,
        path: os.PathLike,
        select: str,
        exclude: Optional[str] = None,
        selector: Optional[str] = None,
    ) -> None:
        """Create a ResourceGroup for a dbt project."""

        group, resources = self._generate_resource_group(
            name, owner, path, select, exclude, selector
        )

        group_path = Path(group.original_file_path)
        try:
            group_yml: Dict[str, str] = self.file_manager.read_file(group_path)  # type: ignore
        except FileNotFoundError:
            group_yml = {}

        output_yml = self.meshify.add_group_to_yml(group, group_yml)
        self.file_manager.write_file(group_path, output_yml)

        logger.info(f"Adding resources to group '{group.name}'...")
        for resource, access_type in resources.items():
            # TODO: revisit this logic other resource types
            if not resource.startswith("model"):
                continue
            model: ModelNode = self.project.models[resource]
            if model.patch_path:
                path = Path(model.patch_path.split("://")[1])
            else:
                if not model.original_file_path:
                    raise Exception("Unable to locate model file. Failing.")

                path = Path(model.original_file_path).parent / '_models.yml'

            try:
                file_yml: Dict[str, Any] = self.file_manager.read_file(path)  # type: ignore
            except FileNotFoundError:
                file_yml = {}

            logger.info(
                f"Adding model '{model.name}' to group '{group.name}' in file '{path.name}'"
            )
            try:
                output_yml = self.meshify.add_group_and_access_to_model_yml(
                    model.name, group, access_type, file_yml
                )

                self.file_manager.write_file(path, output_yml)
                logger.success(f"Successfully added model '{model.name}' to group '{group.name}'")
            except Exception as e:
                logger.error(f"Failed to add model '{model.name}' to group '{group.name}'")
                logger.exception(e)
