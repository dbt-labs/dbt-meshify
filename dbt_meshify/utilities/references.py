import re
from typing import Union

from dbt.contracts.graph.nodes import CompiledNode
from loguru import logger

from dbt_meshify.change import ChangeSet, EntityType, FileChange, Operation
from dbt_meshify.dbt_projects import DbtProject, DbtSubProject, PathedProject
from dbt_meshify.storage.file_manager import DbtFileManager


class ReferenceUpdater:
    def __init__(self, project: Union[DbtSubProject, DbtProject]):
        self.project = project
        self.file_manager = DbtFileManager(read_project_path=project.path)
        self.ref_update_methods = {
            "sql": self.update_refs__sql,
            "python": self.replace_source_with_ref__python,
        }
        self.source_update_methods = {
            "sql": self.replace_source_with_ref__sql,
            "python": self.replace_source_with_ref__python,
        }

    @staticmethod
    def update_refs__sql(model_name, project_name, model_code):
        """Update refs in a SQL file."""

        # pattern to search for ref() with optional spaces and either single or double quotes
        pattern = re.compile(r"{{\s*ref\s*\(\s*['\"]" + re.escape(model_name) + r"['\"]\s*\)\s*}}")

        # replacement string with the new format
        replacement = f"{{{{ ref('{project_name}', '{model_name}') }}}}"

        # perform replacement
        new_code = re.sub(pattern, replacement, model_code)

        return new_code

    @staticmethod
    def replace_source_with_ref__sql(model_code: str, source_unique_id: str, model_unique_id: str):
        source_parsed = source_unique_id.split(".")
        model_parsed = model_unique_id.split(".")

        # pattern to search for source() with optional spaces and either single or double quotes
        pattern = re.compile(
            r"{{\s*source\s*\(\s*['\"]"
            + re.escape(source_parsed[2])
            + r"['\"]\s*,\s*['\"]"
            + re.escape(source_parsed[3])
            + r"['\"]\s*\)\s*}}"
        )

        # replacement string with the new format
        replacement = f"{{{{ ref('{model_parsed[1]}', '{model_parsed[2]}') }}}}"

        # perform replacement
        new_code = re.sub(pattern, replacement, model_code)

        return new_code

    @staticmethod
    def update_refs__python(model_name, project_name, model_code):
        """Update refs in a python file."""

        # pattern to search for ref() with optional spaces and either single or double quotes
        pattern = re.compile(r"dbt\.ref\s*\(\s*['\"]" + re.escape(model_name) + r"['\"]\s*\)")

        # replacement string with the new format
        replacement = f"dbt.ref('{project_name}', '{model_name}')"

        # perform replacement
        new_code = re.sub(pattern, replacement, model_code)

        return new_code

    @staticmethod
    def replace_source_with_ref__python(
        model_code: str, source_unique_id: str, model_unique_id: str
    ):
        import re

        source_parsed = source_unique_id.split(".")
        model_parsed = model_unique_id.split(".")

        # pattern to search for source() with optional spaces and either single or double quotes
        pattern = re.compile(
            r"dbt\.source\s*\(\s*['\"]"
            + re.escape(source_parsed[2])
            + r"['\"]\s*,\s*['\"]"
            + re.escape(source_parsed[3])
            + r"['\"]\s*\)"
        )

        # replacement string with the new format
        replacement = f'dbt.ref("{model_parsed[1]}", "{model_parsed[2]}")'

        # perform replacement
        new_code = re.sub(pattern, replacement, model_code)

        return new_code

    def generate_reference_update(
        self,
        project_name: str,
        upstream_node: CompiledNode,
        downstream_node: CompiledNode,
        downstream_project: PathedProject,
        code: str,
    ) -> FileChange:
        """Generate FileChanges that update the references in the downstream_node's code."""

        updated_code = self.ref_update_methods[downstream_node.language](
            model_name=upstream_node.name,
            project_name=project_name,
            model_code=code,
        )

        return FileChange(
            operation=Operation.Update,
            entity_type=EntityType.Code,
            identifier=downstream_node.name,
            path=downstream_project.resolve_file_path(downstream_node),
            data=updated_code,
        )

    def update_child_refs(self, resource: CompiledNode) -> ChangeSet:
        """Generate a set of FileChanges to update child references"""

        change_set = ChangeSet()

        for model in self.project.xproj_children_of_resources:
            model_node = self.project.get_manifest_node(model)
            if not model_node:
                raise KeyError(f"Resource {model} not found in manifest")

            # Don't process Resources missing a language attribute
            if not hasattr(model_node, "language") or not isinstance(model_node, CompiledNode):
                continue

            change = self.generate_reference_update(
                project_name=self.project.name,
                upstream_node=resource,
                downstream_node=model_node,
                code=model_node.raw_code,
                downstream_project=self.project.parent_project,
            )

            change_set.add(change)

        return change_set

    def update_parent_refs(self, resource: CompiledNode) -> ChangeSet:
        """Generate a ChangeSet of FileChanges to update parent references"""
        upstream_models = [
            parent
            for parent in resource.depends_on.nodes
            if parent in self.project.xproj_parents_of_resources
        ]

        change_set = ChangeSet()

        code = resource.raw_code

        for model in upstream_models:
            logger.info(f"Updating reference to {model} in {resource.name}.")
            model_node = self.project.get_manifest_node(model)
            if not model_node:
                raise KeyError(f"Resource {model} not found in manifest")

            # Don't process Resources missing a language attribute
            if not hasattr(model_node, "language") or not isinstance(model_node, CompiledNode):
                continue

            change = self.generate_reference_update(
                project_name=self.project.parent_project.name,
                upstream_node=model_node,
                downstream_node=resource,
                code=code,
                downstream_project=self.project,
            )

            code = change.data

            change_set.add(change)

        return change_set

    def replace_source_with_refs(
        self,
        resource: CompiledNode,
        upstream_resource: CompiledNode,
        source_unique_id: str,
    ) -> FileChange:
        """Updates the model source reference in the model's code with a cross-project ref to the upstream model."""

        # Here, we're trusting the dbt-core code to check the languages for us. 🐉
        updated_code = self.source_update_methods[resource.language](
            model_code=resource.raw_code,
            source_unique_id=source_unique_id,
            model_unique_id=upstream_resource.unique_id,
        )

        return FileChange(
            operation=Operation.Update,
            entity_type=EntityType.Code,
            identifier=resource.name,
            path=self.project.resolve_file_path(resource),
            data=updated_code,
        )
