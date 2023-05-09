import os
from typing import Iterable

from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.graph.nodes import ModelNode
from dbt.node_types import NodeType

from dbt_meshify.dbt_projects import BaseDbtProject
from dbt_meshify.storage.file_manager import DbtFileManager


class ProjectYamlStorage:
    """
    The ProjectYamlStorage creates and modifies YAML files based on a DbtProject, its
    Manifest, and a list of resources that have changed.
    """

    def __init__(self, project_path: os.PathLike):
        self.file_manager = DbtFileManager(read_project_path=project_path)

    def _write_models(self, nodes: Iterable[ModelNode]):
        """Update model property files in-place."""

        for model in filter(lambda x: x.resource_type == NodeType.Model, nodes):

            new_dictionary = model.to_dict()
            print(model.original_file_path)

    def store(self, project: BaseDbtProject):
        """Store Project data in YAML files where appropriate."""

        self._write_models(project.manifest.nodes.values())
