# classes that deal specifically with file manipulation
# of dbt files to be used in the meshify dbt project
import abc
from abc import ABC
from pathlib import Path
from typing import Any, Dict, Optional, Union

from ruamel.yaml import YAML
from ruamel.yaml.compat import StringIO


class DbtYAML(YAML):
    """dbt-compatible YAML class."""

    def __init__(self):
        super().__init__()
        self.preserve_quotes = True
        self.width = 4096
        self.indent(mapping=2, sequence=4, offset=2)

    def dump(self, data, stream=None, **kw):
        inefficient = False
        if stream is None:
            inefficient = True
            stream = StringIO()
        super().dump(data, stream, **kw)
        if inefficient:
            return stream.getvalue()


yaml = DbtYAML()

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
        self,
        path: Path,
        file_contents: Optional[Union[Dict[str, Any], str]] = None,
        writeback=False,
    ) -> None:
        """Returns the yaml for a model in the dbt project's manifest"""
        # workaround to let the same DbtFileManager write back to the same project in the event that it's managing movements between two project paths
        # let it be known I do not like this
        if not writeback:
            full_path = self.write_project_path / path
        else:
            full_path = self.read_project_path / path
        full_path.parent.mkdir(parents=True, exist_ok=True)
        if full_path.suffix == ".yml":
            full_path.write_text(yaml.dump(file_contents))
        else:
            full_path.write_text(file_contents)  # type: ignore

    def copy_file(self, path: Path) -> None:
        file_contents = self.read_file(path)
        self.write_file(path, file_contents)

    def move_file(self, path: Path) -> None:
        """
        move a file from the read project to the write project
        """
        old_path = self.read_project_path / path
        new_path = self.write_project_path / path
        new_path.parent.mkdir(parents=True, exist_ok=True)
        old_path.rename(new_path)

    def delete_file(self, path: Path) -> None:
        """deletes the specified file"""
        path.unlink()
