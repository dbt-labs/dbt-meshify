# classes that deal specifcally with mile manipulation
# of dbt files to be used in the meshify dbt project
import abc
from abc import ABC
from pathlib import Path
from typing import Any, Dict, Optional, Union

from ruamel.yaml import YAML
from ruamel.yaml.compat import StringIO


class DbtYAML(YAML):
    def dump(self, data, stream=None, **kw):
        inefficient = False
        if stream is None:
            inefficient = True
            stream = StringIO()
        YAML.dump(self, data, stream, **kw)
        if inefficient:
            return stream.getvalue()


yaml = DbtYAML()
yaml.indent(mapping=2, sequence=4, offset=2)

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
            return yaml.load(full_path.read_text())
        else:
            return full_path.read_text()

    def write_file(
        self, path: Path, file_contents: Optional[Union[Dict[str, Any], str]] = None
    ) -> None:
        """Returns the yaml for a model in the dbt project's manifest"""
        full_path = self.write_project_path / path
        if full_path.suffix == ".yml":
            full_path.write_text(yaml.dump(file_contents))
        else:
            full_path.write_text(file_contents)  # type: ignore

    def delete_file(self, path: Path) -> None:
        """deletes the specified file"""
        path.unlink()
