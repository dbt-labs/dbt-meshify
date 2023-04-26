# first party
from typing import List, Dict, Any, Optional
import os
import hashlib
import yaml
from dbt_meshify.file_manager import DbtFileManager
from dbt_meshify.dbt_meshify import DbtMeshConfigEditor

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

class Dbt: 

    def __init__(self):
      self.dbt_runner = dbtRunner()
   
    def invoke(self, directory: Optional[os.PathLike] = None, runner_args: Optional[List[str]] = None) -> dbtRunnerResult:
        starting_directory = os.getcwd()
        if directory:
            os.chdir(directory)
        result = self.dbt_runner.invoke(runner_args if runner_args else [])
        os.chdir(starting_directory)

        if not result.success:
            raise result.exception
        return result.result

    def parse(self, directory: os.PathLike):
        return self.invoke(directory, ['--quiet', 'parse'])

    def ls(self, directory: os.PathLike, arguments: List[str] = []) -> dbtRunnerResult:
        """ 
        Excute dbt ls with the given arguments and return the result as a list of strings. 
        Log level is set to none to prevent dbt from printing to stdout.
        """
        args = ["--log-format", "json", "--log-level", "none", "ls"] + arguments
        return self.invoke(directory, args)

    def docs_generate(self, directory: os.PathLike) -> dbtRunnerResult:
        """ 
        Excute dbt docs generate with the given arguments
        """
        args = ["--quiet", "docs", "generate"]
        return self.invoke(directory, args)

class DbtProject:

    def __init__(self, relative_path: str) -> None:
        self.relative_path = relative_path
        self.dbt = Dbt()
        self.file_manager = DbtFileManager()
        self.meshify = DbtMeshConfigEditor()
        self.config_editor = DbtMeshConfigEditor()
        self.manifest = self._load_manifest()
        self.catalog = self._load_catalog()
        self.subprojects = []
    
    @property
    def path(self) -> os.PathLike:
        return os.path.join(os.getcwd(), self.relative_path)

    def _load_project(self):
        """Load a dbt Project configuration"""
        return yaml.load(open(os.path.join(self.path, 'dbt_project.yml')),  Loader=yaml.Loader)

    def _load_manifest(self) -> Manifest:
        """Load a manifest for a project."""
        return self.dbt.parse(self.path)

    def _load_catalog(self):
        """Load a catalog for a project."""
        return self.dbt.docs_generate(self.path)
        
    def get_subproject_resources(self, subproject_selector: str) -> List[str]:
        return self.dbt.ls(self.path, ["-s", subproject_selector])
        
    def add_subproject(self, subproject_dict: Dict[str, str]) -> None:
        self.subprojects.append(subproject_dict)
    
    def update_subprojects_with_resources(self) -> List[str]:
        [subproject.update({"resources": self.get_subproject_resources(subproject["selector"])}) for subproject in self.subprojects]
    
    def get_catalog_entry(self, unique_id: str) -> Dict[str, Any]:
        """Returns the catalog entry for a model in the dbt project's catalog"""
        return self.catalog.nodes.get(unique_id, {})
    
    def get_manifest_node(self, unique_id: str) -> Dict[str, Any]:
        """Returns the catalog entry for a model in the dbt project's catalog"""
        return self.manifest.nodes.get(unique_id, {})

    def sources(self) -> Dict[str, Dict[Any, Any]]:
        return self.manifest.sources
    
    def models(self) -> Dict[str, Dict[Any, Any]]:
        return {node_name: node for node_name, node in self.manifest.nodes.items() if node.resource_type == 'model'}
    
    def add_model_contract(self, unique_id: str) -> None:
        """Adds a model contract to the model's yaml"""
        
        # get the patch path for the model
        node = self.get_manifest_node(unique_id)
        yml_path = node.patch_path.split("://")[1] if node.patch_path else None
        # if the model doesn't have a patch path, create a new yml file in the models directory
        # TODO - should we check if there's a model yml file in the models directory and append to it?
        if not yml_path:
            yml_path = "/".join(node.original_file_path.split(".")[0].split("/")[:-1]) + "_models.yml"
            self.create_file(yml_path)
        model_catalog = self.get_catalog_entry(unique_id)
        # read the yml file
        full_yml_dict = self.file_manager.read_file(yml_path)
        updated_yml = self.meshify.add_model_contract_to_yml(full_yml_dict, model_catalog, node.name)
        # write the updated yml to the file
        self.file_manager.write_file(yml_path, updated_yml)
    
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



class DbtProjectHolder():

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


