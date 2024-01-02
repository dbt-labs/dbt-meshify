from typing import Union

from dbt.contracts.graph.nodes import ModelNode

from dbt_meshify.change import EntityType, Operation, ResourceChange
from dbt_meshify.dbt_projects import DbtProject, DbtSubProject
from dbt_meshify.storage.file_content_editors import NamedList


class Contractor:
    def __init__(self, project: Union[DbtProject, DbtSubProject]):
        self.project = project

    def generate_contract(self, model: ModelNode) -> ResourceChange:
        """Generate a ChangeSet that adds a contract to a Model."""
        model_catalog = self.project.get_catalog_entry(model.unique_id)

        # create a mapping of the column name to its representation in the yml file to maintain the original case
        original_case = {column_name.lower(): column_name for column_name in model.columns.keys()}

        if not model_catalog or not model_catalog.columns:
            columns = None
        else:
            columns = [
                {
                    "name": original_case.get(name.lower()) or name.lower(),
                    "data_type": value.type.lower(),
                }
                for name, value in model_catalog.columns.items()
            ]

        model_data = {
            "name": model.name,
            "config": {"contract": {"enforced": True}},
        }
        if columns:
            model_data["columns"] = NamedList(columns)

        patch_path = self.project.resolve_patch_path(model)

        return ResourceChange(
            operation=Operation.Update if patch_path.exists() else Operation.Add,
            entity_type=EntityType.Model,
            identifier=model.name,
            path=patch_path,
            data=model_data,
        )
