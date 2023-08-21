# classes that deal specifically with file manipulation
# of dbt files to be used in the meshify dbt project

import shutil
from pathlib import Path
from typing import Any, Dict, List, Protocol

from dbt.contracts.util import Identifier
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
yaml.register_class(Identifier)


class FileManager(Protocol):
    def read_file(self, path: Path):
        """Returns the content from a file."""
        pass

    def write_file(self, path: Path, content: Any) -> None:
        """Write content to a file."""
        pass


class RawFileManager:
    """RawFileManager is a FileManager for operating on raw files in the filesystem."""

    @staticmethod
    def read_file(path: Path) -> str:
        """Read a file from the filesystem and return a string value."""
        return path.read_text()

    @staticmethod
    def touch_file(path: Path) -> None:
        """Create a file, but do not write any data."""
        file = open(path, "w")
        file.close()

    @staticmethod
    def write_file(path: Path, content: str) -> None:
        """Write a string value to a file in the filesystem"""
        path.write_text(content)

    @staticmethod
    def copy_file(source_path: Path, target_path: Path) -> None:
        if not target_path.parent.exists():
            target_path.parent.mkdir(parents=True, exist_ok=True)

        shutil.copy(source_path, target_path)

    @staticmethod
    def move_file(source_path: Path, target_path: Path) -> None:
        """
        move a file from the read project to the write project
        """
        if not target_path.parent.exists():
            target_path.parent.mkdir(parents=True, exist_ok=True)

        shutil.move(source_path, target_path)

    @staticmethod
    def delete_file(path: Path) -> None:
        """deletes the specified file"""

        path.unlink()


class YAMLFileManager:
    """The YAMLFileManager is a FileManager for interacting with YAML files in the filesystem."""

    @staticmethod
    def _clean_content(content: Dict) -> Dict:
        """Clean up a dictionary content to remove empty list fields."""
        keys_to_remove = set()
        for key, value in content.items():
            if isinstance(value, dict):
                content[key] = YAMLFileManager._clean_content(value)
            if isinstance(value, List):
                if len(value) == 0:
                    keys_to_remove.add(key)
                    continue

                for index, subcontent in enumerate(value):
                    if isinstance(subcontent, dict):
                        cleaned_content = YAMLFileManager._clean_content(subcontent)
                        content[key][index] = cleaned_content

        for key in keys_to_remove:
            del content[key]

        return content

    @staticmethod
    def read_file(path: Path) -> Dict:
        """Read a file from the filesystem and return a string value."""
        file_text = RawFileManager.read_file(path)
        return yaml.load(file_text)

    @staticmethod
    def write_file(path: Path, content: Dict) -> None:
        """Write a string value to a file in the filesystem"""
        clean_content = YAMLFileManager._clean_content(content)
        file_text = yaml.dump(clean_content)
        RawFileManager.write_file(path, file_text)
