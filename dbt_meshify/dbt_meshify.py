from pathlib import Path
from typing import Dict
from collections import OrderedDict
from dbt.contracts.results import CatalogTable
from dbt.contracts.graph.nodes import ManifestNode
from dbt_meshify.file_manager import DbtFileManager

class DbtMeshModelYmlEditor:
    """
    Class to operate on the contents of a dbt project's dbt_project.yml file
    to add the dbt-core concepts specific to the dbt mesh
    """

    def add_model_contract_to_yml(self, model_name: str, model_catalog: CatalogTable, full_yml_dict: Dict[str, str]) -> None:
        """Adds a model contract to the model's yaml"""
        # set up yml order
        model_ordered_dict = OrderedDict.fromkeys(["name", "description", "access", "config", "meta","columns"])
        # parse the yml file into a dictionary with model names as keys
        models = { model['name']: model for model in full_yml_dict['models'] } if full_yml_dict else {}
        model_yml = models.get(model_name) or {"name": model_name, "columns": [], "config": {}}

        # isolate the columns from the existing model entry
        yml_cols = model_yml.get("columns", [])
        catalog_cols = model_catalog.columns or {}
        # add the data type to the yml entry for columns that are in yml
        yml_cols = [{**yml_col,'data_type' : catalog_cols.get(yml_col.get("name")).type.lower()} for yml_col in yml_cols]
        # append missing columns in the table to the yml entry
        yml_col_names = [col.get("name").lower() for col in yml_cols]
        for col_name, col in catalog_cols.items():
            if col_name.lower() not in yml_col_names:
                yml_cols.append({"name": col_name.lower(), "data_type": col.type.lower()})
        # update the columns in the model yml entry
        model_yml.update({"columns": yml_cols})
        # add contract to the model yml entry
        # this part should come from the same service as what we use for the standalone command when we get there
        model_yml.update({"config": {"contract": {"enforced": True }}})
        # update the model entry in the full yml file
        # if no entries exist, add the model entry
        # otherwise, update the existing model entry in place
        model_ordered_dict.update(model_yml)
        # remove any keys with None values
        model_ordered_dict = {k: v for k, v in model_ordered_dict.items() if v is not None}
        models[model_name] = model_ordered_dict

        full_yml_dict["models"] = list(models.values())
        return full_yml_dict
    

class DbtMeshModelConstructor(DbtMeshModelYmlEditor):

    def __init__(self, 
        project_path: str,
        model_node: ManifestNode, 
        model_catalog: CatalogTable
    ):  
        self.project_path = project_path
        self.model_node = model_node
        self.model_catalog = model_catalog
        self.name = model_node.name
        self.file_manager = DbtFileManager(read_project_path=project_path, write_project_path=project_path)
    
    def add_model_contract(self) -> None:
        """Adds a model contract to the model's yaml"""
        
        # get the patch path for the model
        node = self.model_node
        yml_path = Path(node.patch_path.split("://")[1]) if node.patch_path else None
        original_file_path = Path(node.original_file_path) if node.original_file_path else None
        # if the model doesn't have a patch path, create a new yml file in the models directory
        # TODO - should we check if there's a model yml file in the models directory and append to it?
        if not yml_path:
            yml_path = original_file_path.parent / "_models.yml"
            self.file_manager.write_file(yml_path)
        model_catalog = self.model_catalog
        # read the yml file
        # pass empty dict if no file contents returned
        full_yml_dict = self.file_manager.read_file(yml_path) or {}
        updated_yml = self.add_model_contract_to_yml(
            model_name=node.name,
            model_catalog=model_catalog,
            full_yml_dict=full_yml_dict
            )
        # write the updated yml to the file
        self.file_manager.write_file(yml_path, updated_yml) 


