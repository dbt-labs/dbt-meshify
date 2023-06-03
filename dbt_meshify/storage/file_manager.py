# classes that deal specifcally with mile manipulation
# of dbt files to be used in the meshify dbt project
import abc
from abc import ABC
from pathlib import Path
from typing import Any, Dict, Optional, Union

import yaml

FileContent = Union[Dict[str, str], str]


class BaseFileManager(ABC):
    @abc.abstractmethod
    def read_file(self, path: Path) -> Union[Dict[str, Any], str]:
        """Returns the content from a file."""
        pass

    @abc.abstractmethod
    def write_file(self, path: Path, file_contents: Any) -> None:
        """Write content to a file."""
        pass


class DbtFileManager(BaseFileManager):
    def __init__(
        self,
        read_project_path: Path,
        write_project_path: Optional[Path] = None,
    ) -> None:
        self.read_project_path = read_project_path
        self.write_project_path = write_project_path if write_project_path else read_project_path

    def read_file(self, path: Path) -> Union[Dict[str, Any], str]:
        """Returns the yaml for a model in the dbt project's manifest"""
        full_path = self.read_project_path / path
        if full_path.suffix == ".yml":
            return yaml.safe_load(full_path.read_text())
        else:
            return full_path.read_text()

    def write_file(
        self, path: Path, file_contents: Optional[Union[Dict[str, Any], str]] = None
    ) -> None:
        """Returns the yaml for a model in the dbt project's manifest"""
        full_path = self.write_project_path / path
        if full_path.suffix == ".yml":
            string_yml = yaml.safe_dump(file_contents)
            full_path.write_text(string_yml)
        else:
            full_path.write_text(file_contents)  # type: ignore
