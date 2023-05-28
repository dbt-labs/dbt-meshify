# classes that deal specifcally with mile manipulation
# of dbt files to be used in the meshify dbt project

import os
from abc import ABC
from pathlib import Path
from typing import Any, Dict, NewType, Optional, Union

import yaml
from dbt.contracts.results import CatalogTable

FileContent = Union[Dict[str, str], str]


class BaseFileManager(ABC):
    def read_file(self, path: os.PathLike) -> Any:
        pass

    def write_file(self, path: os.PathLike, file_contents: Optional[Any] = None) -> None:
        pass


class YmlFileManager(BaseFileManager):
    def __init__(
        self, read_project_path: os.PathLike, write_project_path: Optional[os.PathLike] = None
    ):
        self.read_project_path = read_project_path
        self.write_project_path = write_project_path if write_project_path else read_project_path

    def read_file(self, path: os.PathLike) -> Dict[str, str]:
        """Returns the yaml for a model in the dbt project's manifest"""
        return yaml.load(open(os.path.join(self.read_project_path, path)), Loader=yaml.FullLoader)

    def write_file(
        self, path: os.PathLike, file_contents: Optional[Dict[str, str]] = None
    ) -> None:
        """Returns the yaml for a model in the dbt project's manifest"""

        with open(os.path.join(self.write_project_path, path), "w+") as file:
            # Serialize the updated data back to YAML and write it to the file
            yaml.safe_dump(file_contents, file, sort_keys=False)


class FileManager(BaseFileManager):
    def __init__(
        self, read_project_path: os.PathLike, write_project_path: Optional[os.PathLike] = None
    ) -> None:
        self.read_project_path = read_project_path
        self.write_project_path = write_project_path if write_project_path else read_project_path

    def read_file(self, path: os.PathLike) -> str:
        """Returns the yaml for a model in the dbt project's manifest"""
        with open(os.path.join(self.read_project_path, path), "r") as file:
            return file.read()

    def write_file(self, path: os.PathLike, file_contents: Optional[str] = None) -> None:
        """Returns the yaml for a model in the dbt project's manifest"""

        with open(os.path.join(self.write_project_path, path), "w+") as file:
            file.write(file_contents or '')


class DbtFileManager:
    def __init__(
        self,
        read_project_path: Optional[os.PathLike] = None,
        write_project_path: Optional[os.PathLike] = None,
    ) -> None:
        self.read_project_path: os.PathLike = read_project_path or Path(os.getcwd())
        self.write_project_path: os.PathLike = (
            write_project_path if write_project_path else self.read_project_path
        )

        self.yml_file_manager = YmlFileManager(self.read_project_path, self.write_project_path)
        self.base_file_manager = FileManager(self.read_project_path, self.write_project_path)

    def read_file(self, path: os.PathLike) -> Union[Dict[str, str], str]:
        """Returns the yaml for a model in the dbt project's manifest"""
        if Path(path).suffix == ".yml":
            return self.yml_file_manager.read_file(path)
        else:
            return self.base_file_manager.read_file(path)

    def write_file(self, path: os.PathLike, file_contents: Optional[FileContent] = None) -> None:
        """Returns the yaml for a model in the dbt project's manifest"""
        if Path(path).suffix == ".yml":
            return self.yml_file_manager.write_file(path, file_contents)  # type: ignore
        else:
            return self.base_file_manager.write_file(path, file_contents)  # type: ignore
