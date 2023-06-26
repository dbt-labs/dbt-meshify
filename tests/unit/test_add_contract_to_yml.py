from dbt_meshify.storage.yaml_editors import DbtMeshModelYmlEditor

from ..fixtures import (
    expected_contract_yml_all_col,
    expected_contract_yml_no_col,
    expected_contract_yml_no_entry,
    expected_contract_yml_one_col,
    expected_contract_yml_other_model,
    expected_contract_yml_one_col_one_test,
    model_yml_all_col,
    model_yml_no_col_no_version,
    model_yml_one_col,
    model_yml_other_model,
    model_yml_one_col_one_test,
    shared_model_catalog_entry,
)
from . import read_yml

meshify = DbtMeshModelYmlEditor()
catalog_entry = shared_model_catalog_entry
model_name = "shared_model"


class TestAddContractToYML:
    def test_add_contract_to_yml_no_col(self):
        yml_dict = meshify.add_model_contract_to_yml(
            models_yml=read_yml(model_yml_no_col_no_version),
            model_catalog=catalog_entry,
            model_name=model_name,
        )
        assert yml_dict == read_yml(expected_contract_yml_no_col)

    def test_add_contract_to_yml_one_col(self):
        yml_dict = meshify.add_model_contract_to_yml(
            models_yml=read_yml(model_yml_one_col),
            model_catalog=catalog_entry,
            model_name=model_name,
        )
        assert yml_dict == read_yml(expected_contract_yml_one_col)

    def test_add_contract_to_yml_one_col_one_test(self):
        yml_dict = meshify.add_model_contract_to_yml(
            models_yml=read_yml(model_yml_one_col_one_test),
            model_catalog=catalog_entry,
            model_name=model_name,
        )
        assert yml_dict == read_yml(expected_contract_yml_one_col_one_test)

    def test_add_contract_to_yml_all_col(self):
        yml_dict = meshify.add_model_contract_to_yml(
            models_yml=read_yml(model_yml_all_col),
            model_catalog=catalog_entry,
            model_name=model_name,
        )
        assert yml_dict == read_yml(expected_contract_yml_all_col)

    def test_add_contract_to_yml_no_entry(self):
        yml_dict = meshify.add_model_contract_to_yml(
            models_yml={}, model_catalog=catalog_entry, model_name=model_name
        )
        assert yml_dict == read_yml(expected_contract_yml_no_entry)

    def test_add_contract_to_yml_other_model(self):
        yml_dict = meshify.add_model_contract_to_yml(
            models_yml=read_yml(model_yml_other_model),
            model_catalog=catalog_entry,
            model_name=model_name,
        )
        assert yml_dict == read_yml(expected_contract_yml_other_model)
