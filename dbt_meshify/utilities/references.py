import re
from typing import Union

from dbt.contracts.graph.nodes import CompiledNode

from dbt_meshify.change import EntityType, FileChange, Operation
from dbt_meshify.dbt_projects import DbtProject, DbtSubProject
from dbt_meshify.storage.file_manager import DbtFileManager


class ReferenceUpdater:
    def __init__(self, project: Union[DbtProject, DbtSubProject]):
        self.project = project
        self.file_manager = DbtFileManager(read_project_path=project.path)
        self.ref_update_methods = {"sql": self.update_sql_refs, "python": self.update_python_refs}

    def update_sql_refs(self, model_name, project_name, model_code):
        """Update refs in a SQL file."""

        # pattern to search for ref() with optional spaces and either single or double quotes
        pattern = re.compile(r"{{\s*ref\s*\(\s*['\"]" + re.escape(model_name) + r"['\"]\s*\)\s*}}")

        # replacement string with the new format
        replacement = f"{{{{ ref('{project_name}', '{model_name}') }}}}"

        # perform replacement
        new_code = re.sub(pattern, replacement, model_code)

        return new_code

    def update_python_refs(self, model_name, project_name, model_code):
        """Update refs in a python file."""

        # pattern to search for ref() with optional spaces and either single or double quotes
        pattern = re.compile(r"dbt\.ref\s*\(\s*['\"]" + re.escape(model_name) + r"['\"]\s*\)")

        # replacement string with the new format
        replacement = f"dbt.ref('{project_name}', '{model_name}')"

        # perform replacement
        new_code = re.sub(pattern, replacement, model_code)

        return new_code

    def generate_reference_update(self, node: CompiledNode) -> FileChange:
        """Generate FileChanges that update the references in a model's code."""

        updated_code = self.ref_update_methods[node.language](
            model_name=node.name,
            project_name=self.project.name,
            model_code=node.raw_code,
        )

        return FileChange(
            operation=Operation.Update,
            entity_type=EntityType.Code,
            identifier=node.name,
            path=self.project.resolve_file_path(node),
            data=updated_code,
        )
