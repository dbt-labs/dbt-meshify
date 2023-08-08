import re

from dbt.contracts.graph.nodes import CompiledNode
from loguru import logger

from dbt_meshify.change import ChangeSet, EntityType, FileChange, Operation
from dbt_meshify.dbt_projects import DbtSubProject
from dbt_meshify.storage.file_manager import DbtFileManager


class ReferenceUpdater:
    def __init__(self, project: DbtSubProject):
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

        logger.warning("NEW CODE")
        logger.warning(new_code)

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

    def generate_reference_update(
        self, upstream_node: CompiledNode, downstream_node: CompiledNode
    ) -> FileChange:
        """Generate FileChanges that update the references in the downstream_node's code."""

        updated_code = self.ref_update_methods[downstream_node.language](
            model_name=upstream_node.name,
            project_name=self.project.name,
            model_code=downstream_node.raw_code,
        )

        return FileChange(
            operation=Operation.Update,
            entity_type=EntityType.Code,
            identifier=downstream_node.name,
            path=self.project.parent_project.resolve_file_path(downstream_node),
            data=updated_code,
        )

    def update_child_refs(self, resource: CompiledNode) -> ChangeSet:
        """Generate a set of FileChanges to update child references"""
        downstream_models = [
            node.unique_id
            for node in self.project.parent_project.manifest.nodes.values()
            if node.resource_type == "model" and resource.unique_id in node.depends_on.nodes  # type: ignore
        ]

        logger.warning("DOWNSTREAM MODELS")
        logger.warning(downstream_models)

        change_set = ChangeSet()

        for model in downstream_models:
            model_node = self.project.get_manifest_node(model)
            if not model_node:
                raise KeyError(f"Resource {model} not found in manifest")

            # Don't process Resources missing a language attribute
            if not hasattr(model_node, "language") or not isinstance(model_node, CompiledNode):
                continue

            change_set.add(
                self.generate_reference_update(upstream_node=resource, downstream_node=model_node)
            )

        return change_set
