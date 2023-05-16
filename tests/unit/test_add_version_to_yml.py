from dbt_meshify.dbt_meshify import DbtMeshModelYmlEditor
import yaml
from ..fixtures import (
    model_yml_no_col_no_version,
    expected_versioned_model_yml_no_version,
    model_yml_increment_version,
    expected_versioned_model_yml_increment_version_no_prerelease,
    expected_versioned_model_yml_increment_version_with_prerelease,
    expected_versioned_model_yml_increment_version_defined_in,
    expected_versioned_model_yml_no_yml,
)


meshify = DbtMeshModelYmlEditor()
model_name = "shared_model"


def read_yml(yml_str):
    return yaml.safe_load(yml_str)


class TestAddContractToYML:
    def test_add_version_to_model_yml_no_yml(self):
        yml_dict = meshify.add_model_version_to_yml(full_yml_dict={}, model_name=model_name)
        assert yml_dict == read_yml(expected_versioned_model_yml_no_yml)

    def test_add_version_to_model_yml_no_version(self):
        yml_dict = meshify.add_model_version_to_yml(
            full_yml_dict=read_yml(model_yml_no_col_no_version), model_name=model_name
        )
        assert yml_dict == read_yml(expected_versioned_model_yml_no_version)

    def test_add_version_to_model_yml_increment_version_no_prerelease(self):
        yml_dict = meshify.add_model_version_to_yml(
            full_yml_dict=read_yml(model_yml_increment_version), model_name=model_name
        )
        assert yml_dict == read_yml(expected_versioned_model_yml_increment_version_no_prerelease)

    def test_add_version_to_model_yml_increment_version_with_prerelease(self):
        yml_dict = meshify.add_model_version_to_yml(
            full_yml_dict=read_yml(model_yml_increment_version),
            model_name=model_name,
            prerelease=True,
        )
        assert yml_dict == read_yml(expected_versioned_model_yml_increment_version_with_prerelease)

    def test_add_version_to_model_yml_increment_version_defined_in(self):
        yml_dict = meshify.add_model_version_to_yml(
            full_yml_dict=read_yml(model_yml_increment_version),
            model_name=model_name,
            defined_in="daves_model",
        )
        assert yml_dict == read_yml(expected_versioned_model_yml_increment_version_defined_in)
