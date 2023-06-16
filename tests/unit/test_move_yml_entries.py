import yaml
from dbt.node_types import NodeType

from dbt_meshify.storage.yaml_editors import DbtMeshYmlEditor

from ..fixtures import (
    expeceted_remainder_yml__source_yml_multiple_tables,
    expected_remainder_yml__model_yml_other_model,
    expected_remainder_yml__multiple_exposures,
    expected_remainder_yml__multiple_metrics,
    expected_remove_exposure_yml__default,
    expected_remove_metric_yml__default,
    expected_remove_model_yml__default,
    expected_remove_model_yml__model_yml_no_col_no_version,
    expected_remove_model_yml__model_yml_one_col,
    expected_remove_source_yml__default,
    exposure_yml_multiple_exposures,
    exposure_yml_one_exposure,
    metric_yml_multiple_metrics,
    metric_yml_one_metric,
    model_yml_no_col_no_version,
    model_yml_one_col,
    model_yml_other_model,
    source_yml_multiple_tables,
    source_yml_one_table,
)

meshify = DbtMeshYmlEditor()
model_name = "shared_model"
source_name = "test_source"
source_table_name = "table"
exposure_name = "shared_exposure"
metric_name = "real_good_metric"


def read_yml(yml_str):
    return yaml.safe_load(yml_str)


class TestRemoveResourceYml:
    def test_remove_model_yml_simple(self):
        resource_yml, full_yml = meshify.get_yml_entry(
            resource_name=model_name,
            full_yml=read_yml(model_yml_no_col_no_version),
            resource_type=NodeType.Model,
            source_name=None,
        )
        assert resource_yml == read_yml(expected_remove_model_yml__model_yml_no_col_no_version)
        assert full_yml == None

    def test_remove_model_yml_simple_with_description(self):
        resource_yml, full_yml = meshify.get_yml_entry(
            resource_name=model_name,
            full_yml=read_yml(model_yml_one_col),
            resource_type=NodeType.Model,
            source_name=None,
        )
        assert resource_yml == read_yml(expected_remove_model_yml__model_yml_one_col)
        assert full_yml == None

    def test_remove_model_yml_other_model(self):
        resource_yml, full_yml = meshify.get_yml_entry(
            resource_name=model_name,
            full_yml=read_yml(model_yml_other_model),
            resource_type=NodeType.Model,
            source_name=None,
        )
        assert resource_yml == read_yml(expected_remove_model_yml__default)
        assert full_yml == read_yml(expected_remainder_yml__model_yml_other_model)

    def test_remove_source_yml_one_table(self):
        resource_yml, full_yml = meshify.get_yml_entry(
            resource_name=source_table_name,
            full_yml=read_yml(source_yml_one_table),
            resource_type=NodeType.Source,
            source_name=source_name,
        )
        assert resource_yml == read_yml(expected_remove_source_yml__default)
        assert full_yml == None

    def test_remove_source_yml_multiple_table(self):
        resource_yml, full_yml = meshify.get_yml_entry(
            resource_name=source_table_name,
            full_yml=read_yml(source_yml_multiple_tables),
            resource_type=NodeType.Source,
            source_name=source_name,
        )
        assert resource_yml == read_yml(expected_remove_source_yml__default)
        assert full_yml == read_yml(expeceted_remainder_yml__source_yml_multiple_tables)

    def test_remove_exposure_yml_one_exposure(self):
        resource_yml, full_yml = meshify.get_yml_entry(
            resource_name=exposure_name,
            full_yml=read_yml(exposure_yml_one_exposure),
            resource_type=NodeType.Exposure,
            source_name=None,
        )
        assert resource_yml == read_yml(expected_remove_exposure_yml__default)
        assert full_yml == None

    def test_remove_exposure_yml_multiple_exposures(self):
        resource_yml, full_yml = meshify.get_yml_entry(
            resource_name=exposure_name,
            full_yml=read_yml(exposure_yml_multiple_exposures),
            resource_type=NodeType.Exposure,
            source_name=None,
        )
        assert resource_yml == read_yml(expected_remove_exposure_yml__default)
        assert full_yml == read_yml(expected_remainder_yml__multiple_exposures)

    def test_remove_metric_yml_one_metric(self):
        resource_yml, full_yml = meshify.get_yml_entry(
            resource_name=metric_name,
            full_yml=read_yml(metric_yml_one_metric),
            resource_type=NodeType.Metric,
            source_name=None,
        )
        assert resource_yml == read_yml(expected_remove_metric_yml__default)
        assert full_yml == None

    def test_remove_metric_yml_multiple_metrics(self):
        resource_yml, full_yml = meshify.get_yml_entry(
            resource_name=metric_name,
            full_yml=read_yml(metric_yml_multiple_metrics),
            resource_type=NodeType.Metric,
            source_name=None,
        )
        assert resource_yml == read_yml(expected_remove_metric_yml__default)
        assert full_yml == read_yml(expected_remainder_yml__multiple_metrics)


class TestAddResourceYml:
    def test_add_model_yml_simple(self):
        full_yml = meshify.add_entry_to_yml(
            resource_entry=read_yml(expected_remove_model_yml__model_yml_no_col_no_version),
            full_yml=None,
            resource_type=NodeType.Model,
        )
        assert full_yml == read_yml(model_yml_no_col_no_version)

    def test_add_model_yml_other_model(self):
        full_yml = meshify.add_entry_to_yml(
            resource_entry=read_yml(expected_remove_model_yml__default),
            full_yml=read_yml(expected_remainder_yml__model_yml_other_model),
            resource_type=NodeType.Model,
        )
        assert full_yml == read_yml(model_yml_other_model)

    def test_add_source_yml_one_table(self):
        full_yml = meshify.add_entry_to_yml(
            resource_entry=read_yml(expected_remove_source_yml__default),
            full_yml=None,
            resource_type=NodeType.Source,
        )
        assert full_yml == read_yml(source_yml_one_table)

    def test_add_source_yml_multiple_table(self):
        full_yml = meshify.add_entry_to_yml(
            resource_entry=read_yml(expected_remove_source_yml__default),
            full_yml=read_yml(expeceted_remainder_yml__source_yml_multiple_tables),
            resource_type=NodeType.Source,
        )
        expected = read_yml(source_yml_multiple_tables)
        source_entry = list(filter(lambda x: x["name"] == source_name, full_yml['sources']))
        expected_source_entry = list(
            filter(lambda x: x["name"] == source_name, expected['sources'])
        )
        source_tables = source_entry[0].pop("tables")
        expected_source_tables = expected_source_entry[0].pop("tables")
        assert source_entry == expected_source_entry
        assert sorted(source_tables, key=lambda x: x["name"]) == sorted(
            expected_source_tables, key=lambda x: x["name"]
        )

    def test_add_exposure_yml_one_exposure(self):
        full_yml = meshify.add_entry_to_yml(
            resource_entry=read_yml(expected_remove_exposure_yml__default),
            full_yml=None,
            resource_type=NodeType.Exposure,
        )
        assert full_yml == read_yml(exposure_yml_one_exposure)

    def test_add_exposure_yml_multiple_exposures(self):
        full_yml = meshify.add_entry_to_yml(
            resource_entry=read_yml(expected_remove_exposure_yml__default),
            full_yml=read_yml(expected_remainder_yml__multiple_exposures),
            resource_type=NodeType.Exposure,
        )
        assert sorted(full_yml["exposures"], key=lambda x: x["name"]) == sorted(
            read_yml(exposure_yml_multiple_exposures)["exposures"], key=lambda x: x["name"]
        )

    def test_add_metric_yml_one_metric(self):
        full_yml = meshify.add_entry_to_yml(
            resource_entry=read_yml(expected_remove_metric_yml__default),
            full_yml=None,
            resource_type=NodeType.Metric,
        )
        assert full_yml == read_yml(metric_yml_one_metric)

    def test_add_metric_yml_multiple_metrics(self):
        full_yml = meshify.add_entry_to_yml(
            resource_entry=read_yml(expected_remove_metric_yml__default),
            full_yml=read_yml(expected_remainder_yml__multiple_metrics),
            resource_type=NodeType.Metric,
        )
        assert sorted(full_yml["metrics"], key=lambda x: x["name"]) == sorted(
            read_yml(metric_yml_multiple_metrics)["metrics"], key=lambda x: x["name"]
        )
