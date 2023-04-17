# first party
from typing import List, Dict, Any
import os
import hashlib

# third party
try:
    from dbt.cli.main import dbtRunner
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
    
    def path_to_project(self) -> str:
        return os.getcwd() + '/' + self.relative_path_to_project
    
    def get_project_manifest(self) -> dict:
        start_wd = os.getcwd()
        print(start_wd)
        os.chdir(self.path_to_project())
        result = dbt_runner.invoke(["--log-level", "none", "parse"])
        os.chdir(start_wd)
        if result.success:
            return result.result

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
        return self.project_id in other.installed_packages()

class LocalProjectHolder():

    def __init__(self) -> None:
        self.relative_project_paths: List[str] = []

    def project_map(self) -> dict:
        project_map = {}
        for relative_project_path in self.relative_project_paths:
            project = LocalDbtProject(relative_project_path)
        return project_map 

    def add_relative_project_path(self, relative_project_path) -> None:
        self.relative_project_paths.append(relative_project_path)


