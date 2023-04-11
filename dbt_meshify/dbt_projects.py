# first party
from typing import List 
import os

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
    
    def path_to_project(self) -> str:
        return os.getcwd() + '/' + self.relative_path_to_project
    
    def get_project_manifest(self) -> dict:
        os.chdir(self.path_to_project())
        result, success = dbt_runner.invoke(["--log-level", "none", "parse"])
        if success:
            return result
        
class LocalProjectHolder():

    def __init__(self) -> None:
        self.relative_project_paths: List[str] = []

    def project_map(self) -> dict:
        project_map = {}
        for relative_project_path in self.relative_project_paths:
            project = LocalDbtProject(relative_project_path)
            project_map[relative_project_path] = project
        return project_map 

    def add_relative_project_path(self, relative_project_path) -> None:
        self.relative_project_paths.append(relative_project_path)


