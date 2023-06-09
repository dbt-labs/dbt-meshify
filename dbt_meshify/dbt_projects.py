import copy
import hashlib
import json
import os
from pathlib import Path
from typing import Any, Dict, MutableMapping, Optional, Set, Union

import yaml
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.graph.nodes import (
    Documentation,
    Exposure,
    Group,
    Macro,
    ManifestNode,
    ModelNode,
    Resource,
    SourceDefinition,
)
from dbt.contracts.project import Project
from dbt.contracts.results import CatalogArtifact, CatalogTable
from dbt.graph import Graph
from dbt.node_types import NodeType
from loguru import logger

from dbt_meshify.dbt import Dbt
from dbt_meshify.storage.file_content_editors import (
    DbtMeshConstructor,
    filter_empty_dict_items,
)
from dbt_meshify.storage.file_manager import DbtFileManager


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
        self._models: Optional[Dict[str, ModelNode]] = None

        self.model_relation_names: Dict[str, str] = {
            model.relation_name: unique_id
            for unique_id, model in self.models.items()
            if model.relation_name is not None
        }
        self.source_relation_names: Dict[str, str] = {
            source.relation_name: unique_id
            for unique_id, source in self.sources().items()
            if source.relation_name is not None
        }

        self._graph = None

        self._changes: Dict[str, str] = {}

    @staticmethod
    def _load_graph(manifest: Manifest) -> Graph:
        """Generate a dbt Graph using a project manifest and the internal dbt Compiler and Linker."""

        from dbt.compilation import Compiler, Linker

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

    @property
    def models(self) -> Dict[str, ModelNode]:
        if self._models:
            return self._models

        self._models = {
            node_name: node
            for node_name, node in self.manifest.nodes.items()
            if node.resource_type == "model" and isinstance(node, ModelNode)
        }

        return self._models

    def installed_packages(self) -> Set[str]:
        project_packages = []
        for key in ["nodes", "sources", "exposures", "metrics", "sources", "macros"]:
            items = getattr(self.manifest, key)
            for key, item in items.items():
                if item.package_name:
                    _hash = hashlib.md5()
                    _hash.update(item.package_name.encode("utf-8"))
                    if _hash.hexdigest() != self.manifest.metadata.project_id:
                        project_packages.append(_hash.hexdigest())
        return set(project_packages)

    @property
    def project_id(self) -> Optional[str]:
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

        return self.manifest.nodes.get(model_id)  # type: ignore

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

    def get_catalog_entry(self, unique_id: str) -> Optional[CatalogTable]:
        """Returns the catalog entry for a model in the dbt project's catalog"""
        return self.catalog.nodes.get(unique_id)

    def get_manifest_node(self, unique_id: str) -> Optional[Resource]:
        """Returns the manifest entry for a resource in the dbt project's manifest"""
        if unique_id.split(".")[0] in [
            "model",
            "seed",
            "snapshot",
            "test",
            "analysis",
            "snapshot",
        ]:
            return self.manifest.nodes.get(unique_id)
        pluralized = NodeType(unique_id.split(".")[0]).pluralize()
        resources = getattr(self.manifest, pluralized)
        return resources.get(unique_id)


class DbtProject(BaseDbtProject):
    @staticmethod
    def _load_project(path) -> Project:
        """Load a dbt Project configuration"""
        project_dict = yaml.load(open(os.path.join(path, "dbt_project.yml")), Loader=yaml.Loader)
        return Project.from_dict(project_dict)

    @classmethod
    def from_directory(cls, directory: os.PathLike, read_catalog: bool) -> "DbtProject":
        """Create a new DbtProject using a dbt project directory"""

        dbt = Dbt()
        project = cls._load_project(directory)

        def get_catalog(directory: os.PathLike, read_catalog: bool) -> CatalogArtifact:
            catalog_path = Path(directory) / (project.target_path or "target") / "catalog.json"
            if read_catalog:
                logger.info(f"Reading catalog from {catalog_path}")
                try:
                    catalog_dict = json.loads(catalog_path.read_text())
                    return CatalogArtifact.from_dict(catalog_dict)
                except FileNotFoundError:
                    logger.info(f"Catalog not found at {catalog_path}, running dbt docs generate")
                    return dbt.docs_generate(directory)
            else:
                return dbt.docs_generate(directory)

        return DbtProject(
            manifest=dbt.parse(directory),
            project=project,
            catalog=get_catalog(directory, read_catalog),
            dbt=dbt,
            path=Path(directory),
        )

    def __init__(
        self,
        manifest: Manifest,
        project: Project,
        catalog: CatalogArtifact,
        dbt: Dbt,
        path: Path = Path(os.getcwd()),
        name: Optional[str] = None,
    ) -> None:
        super().__init__(manifest, project, catalog, name)
        self.path = path
        self.dbt = dbt

    def select_resources(
        self,
        select: str,
        exclude: Optional[str] = None,
        selector: Optional[str] = None,
        output_key: Optional[str] = None,
    ) -> Set[str]:
        """Select dbt resources using NodeSelection syntax"""
        args = []
        if select:
            args = ["--select", select]
        if exclude:
            args.extend(["--exclude", exclude])
        if selector:
            args.extend(["--selector", selector])

        results = self.dbt.ls(self.path, args, output_key=output_key)
        if output_key:
            results = [json.loads(resource).get(output_key) for resource in results]

        return set(results)

    def split(
        self,
        project_name: str,
        select: str,
        exclude: Optional[str] = None,
        selector: Optional[str] = None,
    ) -> "DbtSubProject":
        """Create a new DbtSubProject using NodeSelection syntax."""

        subproject_resources = self.select_resources(
            select=select, exclude=exclude, selector=selector, output_key="unique_id"
        )

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
        self.parent_project = parent_project
        self.path = parent_project.path / Path(name)

        # self.manifest = parent_project.manifest.deepcopy()
        # i am running into a bug with the core deepcopy -- checking with michelle
        self.manifest = copy.deepcopy(parent_project.manifest)
        self.project = copy.deepcopy(parent_project.project)
        self.catalog = parent_project.catalog
        self.custom_macros = self._get_custom_macros()
        self.groups = self._get_indirect_groups()

        self._rename_project()

        super().__init__(self.manifest, self.project, self.catalog, self.name)

    def _rename_project(self) -> None:
        """
        edits the project yml to take any instance of the parent project name and update it to the subproject name
        """
        project_dict = self.project.to_dict()
        for key in [resource.pluralize() for resource in NodeType]:
            if self.parent_project.name in project_dict.get(key, {}).keys():
                project_dict[key][self.name] = project_dict[key].pop(self.parent_project.name)
        project_dict["name"] = self.name
        self.project = Project.from_dict(project_dict)

    def _get_custom_macros(self) -> Set[str]:
        """
        get a set of macro unique_ids for all the selected resources
        """
        macros_set = set()
        for unique_id in self.resources:
            resource = self.get_manifest_node(unique_id)
            if not resource or any(
                isinstance(resource, class_) for class_ in [Documentation, Group]
            ):
                continue
            macros = resource.depends_on.macros  # type: ignore
            project_macros = [
                macro
                for macro in macros
                if hashlib.md5((macro.split(".")[1]).encode()).hexdigest()
                == self.manifest.metadata.project_id
            ]
            macros_set.update(project_macros)
        return macros_set

    def _get_indirect_groups(self) -> Set[str]:
        """
        get a set of group unique_ids for all the selected resources
        """
        groups = set()
        for unique_id in self.resources:
            resource = self.get_manifest_node(unique_id)  # type: ignore
            if not resource or any(
                isinstance(resource, class_)
                for class_ in [Documentation, Group, Exposure, SourceDefinition, Macro]
            ):
                continue
            group = resource.group  # type: ignore
            if group:
                group_unique_id = f"group.{self.parent_project.name}.{group}"
                groups.update({group_unique_id})
        return groups

    def select_resources(
        self,
        select: str,
        exclude: Optional[str] = None,
        selector: Optional[str] = None,
        output_key: Optional[str] = None,
    ) -> Set[str]:
        """
        Select resources using the parent DbtProject and filtering down to only include resources in this
        subproject.
        """

        results = self.parent_project.select_resources(
            select=select, exclude=exclude, selector=selector, output_key=output_key
        )

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
            parent_project=copy.deepcopy(self.parent_project),
            resources=subproject_resources,
        )

        # Record the subproject to create a cross-project dependency edge list
        self.register_relationship(project_name, subproject_resources)

        return subproject


class DbtProjectHolder:
    def __init__(self) -> None:
        self.projects: Dict[str, BaseDbtProject] = {}

    def project_map(self) -> dict:
        return self.projects

    def register_project(self, project: BaseDbtProject) -> None:
        if project.project_id:
            self.projects[project.project_id] = project
