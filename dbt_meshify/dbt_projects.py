# first party
from typing import List, Dict, Any
import os
import hashlib

# third party
try:
    from dbt.cli.main import dbtRunner, dbtRunnerResult
    from dbt.contracts.graph.manifest import Manifest
except ImportError:
    dbtRunner = None

if dbtRunner is not None:
    dbt_runner = dbtRunner()
else:
    dbt_runner = None

class LocalDbtProject:

    def __init__(self, relative_path_to_project: str) -> None:
        self.relative_path_to_project = relative_path_to_project
        self.manifest = self.get_project_manifest()
        self.subprojects = []
    
    @property
    def path(self) -> os.PathLike:
        return os.path.join(os.getcwd(), self.relative_path_to_project)

    def dbt_operation(self, operation: List[str]) -> dbtRunnerResult:
        start_wd = os.getcwd()
        os.chdir(self.path)
        result = dbt_runner.invoke(operation)
        os.chdir(start_wd)
        if not result.success:
            raise Exception(result.exception)
        return result
    
    def get_project_manifest(self) -> Manifest:
        manifest_result = self.dbt_operation(["--quiet", "parse"])
        if manifest_result.success:
            return manifest_result.result
        
    def get_subproject_resources(self, subproject_selector: str) -> List[str]:
        ls_results = self.dbt_operation(["--log-level", "none", "ls", "-s", subproject_selector])
        if ls_results.success:
            return ls_results.result
        
    def add_subproject(self, subproject_dict: Dict[str, str]) -> None:
        self.subprojects.append(subproject_dict)
    
    def update_subprojects_with_resources(self) -> List[str]:
        [subproject.update({"resources": self.get_subproject_resources(subproject["selector"])}) for subproject in self.subprojects]

    def sources(self) -> Dict[str, Dict[Any, Any]]:
        return self.manifest.sources
    
    def models(self) -> Dict[str, Dict[Any, Any]]:
        return {node_name: node for node_name, node in self.manifest.nodes.items() if node.resource_type == 'model'}
    
    def installed_packages(self) -> List[str]:
        project_packages = []
        for key in ["nodes", "sources", "exposures", "metrics", "sources", "macros"]:
            items = getattr(self.manifest, key)
            for key, item in items.items():
                if item.package_name:
                    _hash = hashlib.md5()
                    _hash.update(item.package_name.encode('utf-8'))
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
    
    def overlapping_sources(self, other) -> bool:
        """
        Returns any shared metadata between this sources in this project and the models in the other  project
        """
        shared_sources = {}
        for source_id, source in self.sources().items():
            relation_name = source.relation_name
            upstream_model = other.get_model_by_relation_name(relation_name)
            shared_sources[source_id] = {
                    "source": source,
                    "upstream_model": upstream_model
                }

        return shared_sources

    def depends_on(self, other) -> bool:
        """
        Returns true if this project depends on the other project as a package or via shared metadata
        """
        return self.installs(other) or self.shares_source_metadata(other)



class LocalProjectHolder():

    def __init__(self) -> None:
        self.relative_project_paths: List[str] = []

    def project_map(self) -> dict:
        project_map = {}
        for relative_project_path in self.relative_project_paths:
            project = LocalDbtProject(relative_project_path)
            project_map[project.project_id()] = project
        return project_map 

    def add_relative_project_path(self, relative_project_path) -> None:
        self.relative_project_paths.append(relative_project_path)


