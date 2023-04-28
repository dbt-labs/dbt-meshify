from typing import Dict
from collections import OrderedDict
from dbt.contracts.results import CatalogTable

class DbtMeshYmlEditor:
    """
    Class to operate on the contents of a dbt project's dbt_project.yml file
    to add the dbt-core concepts specific to the dbt mesh
    """

    def add_model_contract_to_yml(self, full_yml_dict: Dict[str, str], model_catalog: CatalogTable, model_name: str) -> None:
        """Adds a model contract to the model's yaml"""
        # import pdb; pdb.set_trace()
        model_ordered_dict = OrderedDict.fromkeys(["name", "description", "access", "config", "meta","columns"])
        if full_yml_dict:
            model_yml = [entry for entry in full_yml_dict.get("models", {}) if entry["name"] == model_name].pop()
        else:
            full_yml_dict = {"models": []}
            model_yml = {"name": model_name, "columns": [], "config": {}}
        # isolate the columns from the existing model entry
        yml_cols = model_yml.get("columns", [])
        catalog_cols = model_catalog.columns or {}
        # add the data type to the yml entry for columns that are in yml
        yml_cols = [{**yml_col,'data_type' : catalog_cols.get(yml_col.get("name")).type} for yml_col in yml_cols]
        # append missing columns in the table to the yml entry
        yml_col_names = [col.get("name").lower() for col in yml_cols]
        for col_name, col in catalog_cols.items():
            if col_name.lower() not in yml_col_names:
                yml_cols.append({"name": col_name.lower(), "data_type": col.type.lower()})
        # update the columns in the model yml entry
        model_yml.update({"columns": yml_cols})
        # add contract to the model yml entry
        if not model_yml.get("config"):
            model_yml.update({"config": {"contract": {"enforced": True }}})
        # update the model entry in the full yml file
        # if no entries exist, add the model entry
        # otherwise, update the existing model entry in place
        model_ordered_dict.update(model_yml)
        model_ordered_dict = {k: v for k, v in model_ordered_dict.items() if v is not None}
        if not full_yml_dict.get("models"):
            full_yml_dict.update({"models": [model_ordered_dict]})
        else:
            for i, entry in enumerate(full_yml_dict.get("models", [])):
                if entry["name"] == model_name:
                    full_yml_dict["models"][i] = model_ordered_dict
        return full_yml_dict