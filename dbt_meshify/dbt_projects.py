import copy
import hashlib
import logging
import os
import json
from pathlib import Path
from typing import Dict, Any, Optional, MutableMapping, Set, Union

import yaml
from dbt_meshify.file_manager import DbtFileManager
from dbt_meshify.dbt_meshify import DbtMeshModelYmlEditor
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.graph.nodes import SourceDefinition, ModelNode, ManifestNode
from dbt.contracts.project import Project
from dbt.contracts.results import CatalogArtifact

from dbt_meshify.dbt import Dbt

logger = logging.getLogger()


class BaseDbtProject:
    """A base-level representation of a dbt project."""

    def __init__(
        self,
        manifest: Manifest,
        project: Project,
        catalog: CatalogArtifact,
        name: Optional[str] = None,
    ) -> None:
        self.manifest = manifest
        self.project = project
        self.catalog = catalog
        self.name = name if name else project.name
        self.relationships: Dict[str, Set[str]] = {}

        self.model_relation_names: Dict[str, str] = {
            model.relation_name: unique_id for unique_id, model in self.models().items()
        }
        self.source_relation_names: Dict[str, str] = {
            source.relation_name: unique_id for unique_id, source in self.sources().items()
        }

    def register_relationship(self, project: str, resources: Set[str]) -> None:
        """Register the relationship between two projects"""
        logger.debug(f"Registering the relationship between {project} and its resources")
        entry = self.relationships.get(project, set())
        self.relationships[project] = entry.union(resources)

    def sources(self) -> MutableMapping[str, SourceDefinition]:
        return self.manifest.sources

    def models(self) -> Dict[str, ModelNode]:
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

    @property
    def project_id(self) -> str:
        return self.manifest.metadata.project_id

    def installs(self, other) -> bool:
        """
        Returns true if this project installs the other project as a package
        """
        return self.project_id in other.installed_packages()

    def get_model_by_relation_name(self, relation_name: str) -> Optional[ModelNode]:
        model_id = self.model_relation_names.get(relation_name)
        if not model_id:
            return None

        return self.manifest.nodes.get(model_id)

    def shares_source_metadata(self, other) -> bool:
        """
        Returns true if there is any shared metadata between this project's sources and the models in the other project
        """
        my_sources = {k: v.relation_name for k, v in self.sources().items()}
        their_models = {k: v.relation_name for k, v in other.models().items()}
        return any(item in set(their_models.values()) for item in my_sources.values())

    def overlapping_sources(self, other) -> Dict[str, Dict[str, Union[SourceDefinition, Any]]]:
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

    def get_catalog_entry(self, unique_id: str) -> Dict[str, Any]:
        """Returns the catalog entry for a model in the dbt project's catalog"""
        return self.catalog.nodes.get(unique_id, {})

    def get_manifest_node(self, unique_id: str) -> ManifestNode:
        """Returns the catalog entry for a model in the dbt project's catalog"""
        return self.manifest.nodes.get(unique_id, {})

class DbtProject(BaseDbtProject):
    @staticmethod
    def _load_project(path) -> Project:
        """Load a dbt Project configuration"""
        project_dict = yaml.load(open(os.path.join(path, "dbt_project.yml")), Loader=yaml.Loader)
        return Project.from_dict(project_dict)

    @classmethod
    def from_directory(cls, directory: os.PathLike) -> "DbtProject":
        """Create a new DbtProject using a dbt project directory"""

        dbt = Dbt()

        return DbtProject(
            manifest=dbt.parse(directory),
            project=cls._load_project(directory),
            catalog=dbt.docs_generate(directory),
            dbt=dbt,
            path=directory,
        )

    def __init__(
        self,
        manifest: Manifest,
        project: Project,
        catalog: CatalogArtifact,
        dbt: Dbt,
        path: Optional[os.PathLike] = None,
        name: Optional[str] = None,
    ) -> None:
        super().__init__(manifest, project, catalog, name)
        self.path = path
        self.dbt = dbt
        

    def select_resources(self, select: str, exclude: Optional[str] = None, output_key: Optional[str] = None) -> Set[str]:
        """Select dbt resources using NodeSelection syntax"""
        args = []
        if select:
            args = ["--select", select]
        if exclude:
            args.extend(["--exclude", exclude])

        results = self.dbt.ls(self.path, args, output_key=output_key)
        if output_key:
            results = [json.loads(resource).get(output_key) for resource in results]

        return set(results)

    def split(
        self,
        project_name: str,
        select: str,
        exclude: Optional[str] = None,
    ) -> "DbtSubProject":
        """Create a new DbtSubProject using NodeSelection syntax."""

        subproject_resources = self.select_resources(select, exclude)

        # Construct a new project and inject the new manifest
        subproject = DbtSubProject(
            name=project_name, parent_project=copy.deepcopy(self), resources=subproject_resources
        )

        # Record the subproject to create a cross-project dependency edge list
        self.register_relationship(project_name, subproject_resources)

        return subproject


class DbtSubProject(BaseDbtProject):
    """
    DbtSubProjects are a filtered representation of a dbt project that leverage
    a parent DbtProject for their manifest and project definitions until a real DbtProject
    is created on disk.
    """

    def __init__(self, name: str, parent_project: DbtProject, resources: Set[str]):
        self.name = name
        self.resources = resources
        self.parent = parent_project

        self.manifest = parent_project.manifest.deepcopy()
        self.project = copy.deepcopy(parent_project.project)
        self.catalog = parent_project.catalog

        super().__init__(self.manifest, self.project, self.catalog)

    def select_resources(self, select: str, exclude: Optional[str] = None) -> Set[str]:
        """
        Select resources using the parent DbtProject and filtering down to only include resources in this
        subproject.
        """
        args = ["--select", select]
        if exclude:
            args.extend(["--exclude", exclude])

        results = self.parent.dbt.ls(self.parent.path, args)

        return set(results) - self.resources

    def split(
        self,
        project_name: str,
        select: str,
        exclude: Optional[str] = None,
    ) -> "DbtSubProject":
        """Create a new DbtSubProject using NodeSelection syntax."""

        subproject_resources = self.select_resources(select, exclude)

        # Construct a new project and inject the new manifest
        subproject = DbtSubProject(
            name=project_name,
            parent_project=copy.deepcopy(self.parent),
            resources=subproject_resources,
        )

        # Record the subproject to create a cross-project dependency edge list
        self.register_relationship(project_name, subproject_resources)

        return subproject

    def initialize(self, target_directory: os.PathLike):
        """Initialize this subproject as a full dbt project at the provided `target_directory`."""

        # TODO: Implement project initialization

        raise NotImplementedError


class DbtProjectHolder:
    def __init__(self) -> None:
        self.projects: Dict[str, BaseDbtProject] = {}

    def project_map(self) -> dict:
        return self.projects

    def register_project(self, project: BaseDbtProject) -> None:
        self.projects[project.project_id] = project
