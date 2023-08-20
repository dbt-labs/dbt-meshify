import os
from pathlib import Path

from dbt_meshify.storage.file_manager import YAMLFileManager


class TestYAMLFileManager:
    def test_empty_lists_are_removed(self):
        """Test that dictionaries containing fields with empty lists have those fields removed when written to YAML."""
        path = Path("example.yml")
        input = {"name": "example", "tests": [], "description": "An example model"}
        file_manager = YAMLFileManager()
        file_manager.write_file(path, input)

        data = file_manager.read_file(path)
        os.unlink(path)
        assert data == {"name": "example", "description": "An example model"}

    def test_nested_empty_lists_are_removed(self):
        """
        Test that dictionaries containing fields with nested empty lists have those fields removed when written to YAML.
        """
        path = Path("example.yml")
        input = {"sources": [{"name": "example", "tests": [], "description": "An example model"}]}
        file_manager = YAMLFileManager()
        file_manager.write_file(path, input)

        data = file_manager.read_file(path)
        os.unlink(path)
        assert data == {"sources": [{"name": "example", "description": "An example model"}]}
