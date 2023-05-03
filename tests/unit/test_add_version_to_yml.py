from dbt_meshify.dbt_meshify import DbtMeshModelYmlEditor
import yaml

model_yml_no_version = """
models:
  - name: shared_model
    description: "this is a test model"
"""

expected_model_yml_no_version = """
models:
  - name: shared_model
    latest_version: 1
    description: "this is a test model"
    versions:
      - v: 1
"""

model_yml_increment_version = """
models:
  - name: shared_model
    latest_version: 1
    description: "this is a test model"
    versions:
      - v: 1
"""

expected_model_yml_increment_version_no_prerelease = """
models:
  - name: shared_model
    latest_version: 2
    description: "this is a test model"
    versions:
      - v: 1
      - v: 2
"""

expected_model_yml_increment_version_with_prerelease = """
models:
  - name: shared_model
    latest_version: 1
    description: "this is a test model"
    versions:
      - v: 1
      - v: 2
"""

expected_model_yml_increment_version_defined_in = """
models:
  - name: shared_model
    latest_version: 2
    description: "this is a test model"
    versions:
      - v: 1
      - v: 2
        defined_in: daves_model
"""

meshify = DbtMeshModelYmlEditor()
model_name = "shared_model"


def read_yml(yml_str):
    return yaml.safe_load(yml_str)


class TestAddContractToYML:
    def test_add_version_to_model_yml_no_version(self):
        yml_dict = meshify.add_model_version_to_yml(
            full_yml_dict=read_yml(model_yml_no_version), model_name=model_name
        )
        assert yml_dict == read_yml(expected_model_yml_no_version)

    def test_add_version_to_model_yml_increment_version_no_prerelease(self):
        yml_dict = meshify.add_model_version_to_yml(
            full_yml_dict=read_yml(model_yml_increment_version), model_name=model_name
        )
        assert yml_dict == read_yml(expected_model_yml_increment_version_no_prerelease)

    def test_add_version_to_model_yml_increment_version_with_prerelease(self):
        yml_dict = meshify.add_model_version_to_yml(
            full_yml_dict=read_yml(model_yml_increment_version),
            model_name=model_name,
            prerelease=True,
        )
        assert yml_dict == read_yml(expected_model_yml_increment_version_with_prerelease)

    def test_add_version_to_model_yml_increment_version_defined_in(self):
        yml_dict = meshify.add_model_version_to_yml(
            full_yml_dict=read_yml(model_yml_increment_version),
            model_name=model_name,
            defined_in="daves_model",
        )
        assert yml_dict == read_yml(expected_model_yml_increment_version_defined_in)
