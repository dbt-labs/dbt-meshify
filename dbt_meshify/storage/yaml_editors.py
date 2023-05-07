from typing import Dict
from collections import OrderedDict
from dbt.contracts.results import CatalogTable

class DbtMeshYmlEditor:
    """
    Class to operate on the contents of a dbt project's dbt_project.yml file
    to add the dbt-core concepts specific to the dbt linker
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