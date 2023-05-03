from dbt_meshify.dbt_meshify import DbtMeshModelYmlEditor
import yaml
from ..fixtures import (
    shared_model_catalog_entry,
    model_yml_no_col,
    expected_yml_no_col,
    model_yml_one_col,
    expected_yml_one_col,
    model_yml_all_col,
    expected_yml_all_col,
    expected_yml_no_entry,
    model_yml_other_model,
    expected_yml_other_model,
)


meshify = DbtMeshModelYmlEditor()
catalog_entry = shared_model_catalog_entry
model_name = "shared_model"


def read_yml(yml_str):
    return yaml.safe_load(yml_str)


class TestAddContractToYML:
    def test_add_contract_to_yml_no_col(self):
        yml_dict = meshify.add_model_contract_to_yml(
            full_yml_dict=read_yml(model_yml_no_col),
            model_catalog=catalog_entry,
            model_name=model_name,
        )
        assert yml_dict == read_yml(expected_yml_no_col)

    def test_add_contract_to_yml_one_col(self):
        yml_dict = meshify.add_model_contract_to_yml(
            full_yml_dict=read_yml(model_yml_one_col),
            model_catalog=catalog_entry,
            model_name=model_name,
        )
        assert yml_dict == read_yml(expected_yml_one_col)

    def test_add_contract_to_yml_all_col(self):
        yml_dict = meshify.add_model_contract_to_yml(
            full_yml_dict=read_yml(model_yml_all_col),
            model_catalog=catalog_entry,
            model_name=model_name,
        )
        assert yml_dict == read_yml(expected_yml_all_col)

    def test_add_contract_to_yml_no_entry(self):
        yml_dict = meshify.add_model_contract_to_yml(
            full_yml_dict={}, model_catalog=catalog_entry, model_name=model_name
        )
        assert yml_dict == read_yml(expected_yml_no_entry)

    def test_add_contract_to_yml_other_model(self):
        yml_dict = meshify.add_model_contract_to_yml(
            full_yml_dict=read_yml(model_yml_other_model),
            model_catalog=catalog_entry,
            model_name=model_name,
        )
        assert yml_dict == read_yml(expected_yml_other_model)
