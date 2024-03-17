import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from dbt.contracts.graph.nodes import (
    CompiledNode,
    Exposure,
    Macro,
    Resource,
    SemanticModel,
)
from loguru import logger

from dbt_meshify.change import (
    ChangeSet,
    EntityType,
    FileChange,
    Operation,
    ResourceChange,
)
from dbt_meshify.dbt_projects import DbtProject, DbtSubProject, PathedProject


def get_latest_file_change(
    changeset: ChangeSet,
    path: Path,
    identifier: str,
    entity_type: EntityType = EntityType.Code,
    operation: Operation = Operation.Update,
) -> Optional[FileChange]:
    previous_changes: List[FileChange] = [
        change
        for change in changeset.changes
        if (
            isinstance(change, FileChange)
            and change.identifier == identifier
            and change.operation == operation
            and change.entity_type == entity_type
            and change.path == path
        )
    ]
    return previous_changes[-1] if previous_changes else None


class ReferenceUpdater:
    def __init__(self, project: Union[DbtSubProject, DbtProject]):
        self.project = project

        self.ref_update_methods = {
            "sql": self.update_refs__sql,
            "python": self.update_refs__python,
        }
        self.source_update_methods = {
            "sql": self.replace_source_with_ref__sql,
            "python": self.replace_source_with_ref__python,
        }

    @staticmethod
    def update_refs__sql(model_name, project_name, model_code):
        """Update refs in a SQL file."""

        # pattern to search for ref() with optional spaces and either single or double quotes
        # this also captures the version argument if it exists
        pattern = re.compile(
            r"{{\s*ref\s*\(\s*['\"]" + re.escape(model_name) + r"['\"](?:,\s*(v=\d+))?\s*\)\s*}}"
        )

        def replacement_function(match):
            version_arg = match.group(1)
            if version_arg:
                return f"{{{{ ref('{project_name}', '{model_name}', {version_arg}) }}}}"
            else:
                return f"{{{{ ref('{project_name}', '{model_name}') }}}}"

        # perform replacement using the function
        new_code = re.sub(pattern, replacement_function, model_code)

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

        # pattern to search for dbt.ref() with optional spaces and either single or double quotes
        # this also captures the version argument if it exists
        pattern = re.compile(
            r"dbt\.ref\s*\(\s*['\"]" + re.escape(model_name) + r"['\"](?:,\s*(v=\d+))?\s*\)"
        )

        def replacement_function(match):
            version_arg = match.group(1)
            if version_arg:
                return f"dbt.ref('{project_name}', '{model_name}', {version_arg})"
            else:
                return f"dbt.ref('{project_name}', '{model_name}')"

        # perform replacement using the function
        new_code = re.sub(pattern, replacement_function, model_code)

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

    def update_yml_resource_references(
        self,
        project_name: str,
        upstream_resource_name: str,
        resource: Union[Exposure, SemanticModel],
    ) -> Dict[str, Any]:
        new_ref = f"ref('{project_name}', '{upstream_resource_name}')"
        if isinstance(resource, SemanticModel):
            # we can return early, since semantic models only have one ref and no depends_on
            return {"model": new_ref}
        refs = resource.refs
        ref_to_update = f"ref('{upstream_resource_name}')"
        str_refs = []
        for ref in refs:
            package_clause = f"'{ref.package}', " if ref.package else ""
            name_clause = f"'{ref.name}'"
            version_clause = f", v={ref.version}" if ref.version else ""
            str_ref = f"ref({package_clause}{name_clause}{version_clause})"
            if str_ref != ref_to_update:
                str_refs.append(str_ref)
        str_refs.append(new_ref)
        return {"depends_on": str_refs}

    def generate_reference_update(
        self,
        project_name: str,
        upstream_node: Union[CompiledNode, Macro],
        downstream_node: Union[Resource, CompiledNode, Macro],
        downstream_project: PathedProject,
        code: str,
    ) -> Union[FileChange, ResourceChange]:
        """Generate FileChanges that update the references in the downstream_node's code."""

        if isinstance(downstream_node, CompiledNode) or isinstance(downstream_node, Macro):
            language = downstream_node.language if hasattr(downstream_node, "language") else "sql"
            updated_code = self.ref_update_methods[language](
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

        elif isinstance(downstream_node, Exposure) or isinstance(downstream_node, SemanticModel):
            is_exposure = isinstance(downstream_node, Exposure)
            data = self.update_yml_resource_references(
                project_name=project_name,
                upstream_resource_name=upstream_node.name,
                resource=downstream_node,
            )
            return ResourceChange(
                operation=Operation.Update,
                entity_type=EntityType.Exposure if is_exposure else EntityType.SemanticModel,
                identifier=downstream_node.name,
                path=downstream_project.resolve_file_path(downstream_node),
                data=data,
            )
        raise Exception("Invalid node type provided to generate_reference_update.")

    def update_macro_refs(
        self,
        resource: Macro,
    ) -> Optional[FileChange]:
        """Generate a set of FileChanges to update references within a macro"""

        # If we're accidentally operating in a root project, do not rewrite anything.
        if isinstance(self.project, DbtProject):
            return None

        pattern = re.compile(r"{{\s*ref\s*\(\s*['\"]([a-zA-Z_]+)['\"](?:,\s*(v=\d+))?\s*\)\s*}}")
        matches = re.search(pattern, resource.macro_sql)

        if matches is None:
            return None

        model_name = matches.group(1)
        model_id = f"model.{self.project.parent_project.name}.{model_name}"

        # Check if the model is part of the original project.
        if model_id not in self.project.parent_project.resources:
            raise ValueError(
                f"Unable to find {model_id} in the parent project. How did we get here?"
            )

        # Check if the model is part of the new project. If it is, then return None.
        # We don't want to rewrite the ref for a model already in this project..
        if model_id in self.project.resources:
            logger.debug(f"Model {model_id} exists in the new project! Skipping.")
            return None

        change = self.generate_reference_update(
            project_name=self.project.parent_project.name,
            upstream_node=self.project.parent_project.models[model_id],
            downstream_node=resource,
            code=resource.macro_sql,
            downstream_project=self.project,
        )

        if not isinstance(change, FileChange):
            raise TypeError(
                "Incorrect change type returned during macro ref updating. Expected FileChange, but found ResourceChange"
            )

        return change

    def update_child_refs(
        self,
        resource: CompiledNode,
        current_change_set: Optional[ChangeSet] = None,
    ) -> ChangeSet:
        """Generate a set of FileChanges to update child references"""

        if not isinstance(self.project, DbtSubProject):
            raise Exception(
                "The `update_child_refs` method requires the calling project to have a parent project to update."
            )

        change_set = ChangeSet()
        if isinstance(self.project, DbtSubProject):
            compare_project = self.project.parent_project.name
        else:
            compare_project = self.project.name

        for child in self.project.child_map[resource.unique_id]:
            if child in self.project.resources or child.split(".")[1] != compare_project:
                continue
            node = self.project.get_manifest_node(child)
            if not node:
                raise KeyError(f"Resource {child} not found in manifest")

            if current_change_set:
                previous_change = get_latest_file_change(
                    changeset=current_change_set,
                    identifier=node.name,
                    path=self.project.parent_project.resolve_file_path(node),
                )
                code = (
                    previous_change.data
                    if (previous_change and previous_change.data)
                    else getattr(node, "raw_code", "")
                )

            change = self.generate_reference_update(
                project_name=self.project.name,
                upstream_node=resource,
                downstream_node=node,
                code=code,
                downstream_project=self.project.parent_project,
            )

            change_set.add(change)

        return change_set

    def update_parent_refs(self, resource: CompiledNode) -> ChangeSet:
        """Generate a ChangeSet of FileChanges to update parent references"""

        if not isinstance(self.project, DbtSubProject):
            raise Exception(
                "The `update_parent_refs` method requires the calling project to have a parent project to update."
            )

        upstream_models = [
            parent
            for parent in resource.depends_on.nodes
            if parent in self.project.xproj_parents_of_resources
        ]

        change_set = ChangeSet()

        for model in upstream_models:
            logger.debug(f"Updating reference to {model} in {resource.name}.")
            model_node = self.project.get_manifest_node(model)
            if not model_node:
                raise KeyError(f"Resource {model} not found in manifest")

            # Don't process Resources missing a language attribute
            if not hasattr(model_node, "language") or not isinstance(model_node, CompiledNode):
                continue
            previous_change = change_set.changes[-1] if change_set.changes else None

            code = (
                previous_change.data
                if (
                    previous_change
                    and isinstance(previous_change, FileChange)
                    and previous_change.data
                )
                else resource.raw_code
            )

            change = self.generate_reference_update(
                project_name=self.project.parent_project.name,
                upstream_node=model_node,
                downstream_node=resource,
                code=code,
                downstream_project=self.project,
            )

            if change.data is None:
                raise Exception(f"Change has null data despite being provided code. {change}")

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
