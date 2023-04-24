import copy
import logging
from typing import List, Dict, Any, Optional, Self, MutableMapping, Set
import os
import hashlib
import yaml
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.graph.nodes import SourceDefinition
from dbt.contracts.project import Project
from dbt.graph import UniqueId

from dbt_meshify.dbt import Dbt

logger = logging.getLogger()


class DbtProject:
    @staticmethod
    def _load_project(path) -> Project:
        """Load a dbt Project configuration"""
        project_dict = yaml.load(open(os.path.join(path, "dbt_project.yml")), Loader=yaml.Loader)
        return Project.from_dict(project_dict)

    @classmethod
    def from_directory(cls, directory: os.PathLike) -> Self:
        """Create a new DbtProject using a dbt project directory"""

        dbt = Dbt()

        return DbtProject(
            manifest=dbt.parse(directory),
            project=cls._load_project(directory),
            dbt=dbt,
            path=directory,
        )

    def __init__(
        self, manifest: Manifest, project: Project, dbt: Dbt, path: Optional[os.PathLike] = None
    ) -> None:
        self.path = path
        self.dbt = dbt
        self.manifest = manifest
        self.project = project
        self.subprojects: Dict[str, Set[UniqueId]] = {}

    def select_resources(self, select: str, exclude: Optional[str] = None) -> Set[str]:
        args = ["--select", select]
        if exclude:
            args.extend(["--exclude", exclude])

        results = self.dbt.ls(self.path, args)

        return set(results)

    def create_subproject(
        self,
        project_name: str,
        select: str,
        exclude: Optional[str] = None,
        target_directory: Optional[os.PathLike] = None,
    ) -> Self:
        """Create a new DbtProject using NodeSelection syntax."""

        subproject = copy.deepcopy(self)

        # Update project parameters
        if target_directory is None:
            target_directory = os.path.join(os.getcwd(), project_name)
        subproject.path = target_directory
        subproject.project.name = project_name

        # Prune the project manifest.
        logger.warning("Project manifest pruning has not been implemented yet.")
        subproject_resources = self.select_resources(select, exclude)

        # Record the subproject to create a cross-project dependency graph
        self.subprojects[project_name] = subproject_resources

        return subproject

    def sources(self) -> MutableMapping[str, SourceDefinition]:
        return self.manifest.sources

    def models(self) -> Dict[str, Dict[Any, Any]]:
        return {
            node_name: node
            for node_name, node in self.manifest.nodes.items()
            if node.resource_type == "model"
        }

    def installed_packages(self) -> Set[str]:
        project_packages = []
        for key in ["nodes", "sources", "exposures", "metrics", "sources", "macros"]:
            items = getattr(self.manifest, key)
            for key, item in items.items():
                if item.package_name:
                    _hash = hashlib.md5()
                    _hash.update(item.package_name.encode("utf-8"))
                    project_packages.append(_hash.hexdigest())
        return set(project_packages)

    def project_id(self) -> str:
        return self.manifest.metadata.project_id

    def installs(self, other) -> bool:
        """
        Returns true if this project installs the other project as a package
        """
        return self.project_id in other.installed_packages()

    def get_model_by_relation_name(self, relation_name: str) -> Dict[str, Dict[Any, Any]]:
        return {k: v for k, v in self.models().items() if v.relation_name == relation_name}

    def shares_source_metadata(self, other) -> bool:
        """
        Returns true if there is any shared metadata between this project's sources and the models in the other project
        """
        my_sources = {k: v.relation_name for k, v in self.sources().items()}
        their_models = {k: v.relation_name for k, v in other.models().items()}
        return any(item in set(their_models.values()) for item in my_sources.values())

    def overlapping_sources(self, other) -> dict[str, dict[str, SourceDefinition | Any]]:
        """
        Returns any shared metadata between this sources in this project and the models in the other  project
        """
        shared_sources = {}
        for source_id, source in self.sources().items():
            relation_name = source.relation_name
            upstream_model = other.get_model_by_relation_name(relation_name)
            shared_sources[source_id] = {"source": source, "upstream_model": upstream_model}

        return shared_sources

    def depends_on(self, other) -> bool:
        """
        Returns true if this project depends on the other project as a package or via shared metadata
        """
        return self.installs(other) or self.shares_source_metadata(other)


class DbtProjectHolder:
    def __init__(self) -> None:
        self.relative_project_paths: List[str] = []

    def project_map(self) -> dict:
        project_map = {}
        for relative_project_path in self.relative_project_paths:
            project = DbtProject(relative_project_path)
            project_map[project.project_id()] = project
        return project_map

    def add_relative_project_path(self, relative_project_path) -> None:
        self.relative_project_paths.append(relative_project_path)
