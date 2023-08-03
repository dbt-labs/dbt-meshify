from typing import Union

from dbt.contracts.graph.nodes import ModelNode

from dbt_meshify.change import EntityType, Operation, ResourceChange
from dbt_meshify.dbt_projects import DbtProject, DbtSubProject
from dbt_meshify.storage.file_content_editors import NamedList
from dbt_meshify.storage.file_manager import DbtFileManager


class Contractor:
    def __init__(self, project: Union[DbtProject, DbtSubProject]):
        self.project = project
        self.file_manager = DbtFileManager(read_project_path=project.path)

    def generate_contract(self, model: ModelNode) -> ResourceChange:
        """Generate a ChangeSet that adds a contract to a Model."""
        model_catalog = self.project.get_catalog_entry(model.unique_id)

        if not model_catalog or not model_catalog.columns:
            columns = None
        else:
            columns = [
                {"name": name.lower(), "data_type": value.type.lower()}
                for name, value in model_catalog.columns.items()
            ]

        model_data = {
            "name": model.name,
            "config": {"contract": {"enforced": True}},
            "columns": NamedList(columns),
        }

        return ResourceChange(
            operation=Operation.Update,
            entity_type=EntityType.Model,
            identifier=model.name,
            path=self.project.resolve_patch_path(model),
            data=model_data,
        )
