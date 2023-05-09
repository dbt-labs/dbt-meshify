import copy
import hashlib
import logging
import os
import json
from pathlib import Path
from typing import Dict, Any, Optional, MutableMapping, Set, Union

import yaml
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.graph.nodes import SourceDefinition, ModelNode, ManifestNode
from dbt.contracts.project import Project
from dbt.contracts.results import CatalogArtifact
from dbt.graph import Graph

from dbt_meshify.dbt import Dbt
from dbt_meshify.storage.file_manager import DbtFileManager
from dbt_meshify.storage.yaml_editors import DbtMeshYmlEditor

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
        self.meshify = DbtMeshYmlEditor()

        self.model_relation_names: Dict[str, str] = {
            model.relation_name: unique_id for unique_id, model in self.models().items()
        }
        self.source_relation_names: Dict[str, str] = {
            source.relation_name: unique_id for unique_id, source in self.sources().items()
        }

        self._graph = None

        self._changes: Dict[str, str] = {}


    @staticmethod
    def _load_graph(manifest: Manifest) -> Graph:
        """Generate a dbt Graph using a project manifest and the internal dbt Compiler and Linker."""

        from dbt.compilation import Compiler
        from dbt.compilation import Linker

        compiler = Compiler(config={})
        linker = Linker()
        compiler.link_graph(linker=linker, manifest=manifest)
        return Graph(linker.graph)

    @property
    def graph(self):
        """Get the dbt-core Graph for a given project Manifest"""
        if self._graph:
            return self._graph

        self._graph = self._load_graph(self.manifest)
        return self._graph

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

    def select_resources(
        self, select: str, exclude: Optional[str] = None, output_key: Optional[str] = None
    ) -> Set[str]:
        """Select dbt resources using NodeSelection syntax"""
        raise NotImplementedError


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
        self.file_manager = DbtFileManager(read_project_path=path, write_project_path=path)

    def select_resources(
        self, select: str, exclude: Optional[str] = None, output_key: Optional[str] = None
    ) -> Set[str]:
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

    def add_model_contract(self, unique_id: str) -> None:
        """Adds a model contract to the model's yaml"""

        # get the patch path for the model
        node = self.get_manifest_node(unique_id)
        yml_path = Path(node.patch_path.split("://")[1]) if node.patch_path else None
        original_file_path = Path(node.original_file_path) if node.original_file_path else None
        # if the model doesn't have a patch path, create a new yml file in the models directory
        # TODO - should we check if there's a model yml file in the models directory and append to it?
        if not yml_path:
            yml_path = original_file_path.parent / "_models.yml"
            self.file_manager.write_file(yml_path)
        model_catalog = self.get_catalog_entry(unique_id)
        # read the yml file
        # pass empty dict if no file contents returned
        full_yml_dict = self.file_manager.read_file(yml_path) or {}
        updated_yml = self.meshify.add_model_contract_to_yml(
            model_name=node.name, model_catalog=model_catalog, full_yml_dict=full_yml_dict
        )
        # write the updated yml to the file
        self.file_manager.write_file(yml_path, updated_yml)


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
