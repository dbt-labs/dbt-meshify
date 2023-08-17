from pathlib import Path

import yaml

from dbt_meshify.change import EntityType, Operation, ResourceChange
from dbt_meshify.storage.file_content_editors import NamedList, ResourceFileEditor

from ..sql_and_yml_fixtures import (
    expeceted_remainder_yml__source_yml_multiple_tables,
    expected_remainder_yml__model_yml_other_model,
    expected_remainder_yml__multiple_exposures,
    expected_remainder_yml__multiple_metrics,
    expected_remove_exposure_yml__default,
    expected_remove_metric_yml__default,
    expected_remove_model_yml__default,
    expected_remove_model_yml__model_yml_no_col_no_version,
    expected_remove_source_yml__default,
    expected_yml_one_table,
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

model_name = "shared_model"
source_name = "test_source"
source_table_name = "table"
exposure_name = "shared_exposure"
metric_name = "real_good_metric"


def read_yml(yml_str):
    return yaml.safe_load(yml_str)


current_path = Path(".").resolve()


class TestRemoveResourceYml:
    def test_remove_model_yml_simple(self):
        change = ResourceChange(
            operation=Operation.Remove,
            entity_type=EntityType.Model,
            identifier=model_name,
            path=current_path,
            data=read_yml(model_yml_no_col_no_version),
        )
        full_yml = ResourceFileEditor.remove_resource(
            properties=read_yml(model_yml_one_col), change=change
        )
        assert full_yml == {}

    def test_remove_model_yml_simple_with_description(self):
        change = ResourceChange(
            operation=Operation.Remove,
            entity_type=EntityType.Model,
            identifier=model_name,
            path=current_path,
            data={},
        )
        full_yml = ResourceFileEditor.remove_resource(
            properties=read_yml(model_yml_one_col), change=change
        )

        assert full_yml == {}

    def test_remove_model_yml_other_model(self):
        change = ResourceChange(
            operation=Operation.Remove,
            entity_type=EntityType.Model,
            identifier=model_name,
            path=current_path,
            data={},
        )
        full_yml = ResourceFileEditor.remove_resource(
            properties=read_yml(model_yml_other_model), change=change
        )

        assert full_yml == read_yml(expected_remainder_yml__model_yml_other_model)

    def test_remove_source_yml_one_table(self):
        change = ResourceChange(
            operation=Operation.Remove,
            entity_type=EntityType.Source,
            identifier=source_table_name,
            path=current_path,
            data={},
            source_name="test_source",
        )
        full_yml = ResourceFileEditor.remove_resource(
            properties=read_yml(source_yml_one_table), change=change
        )

        assert full_yml == read_yml(expected_yml_one_table)

    def test_remove_source_yml_multiple_table(self):
        change = ResourceChange(
            operation=Operation.Remove,
            entity_type=EntityType.Source,
            identifier=source_table_name,
            path=current_path,
            data={},
            source_name="test_source",
        )
        full_yml = ResourceFileEditor.remove_resource(
            properties=read_yml(source_yml_multiple_tables), change=change
        )

        assert full_yml == read_yml(expeceted_remainder_yml__source_yml_multiple_tables)

    def test_remove_exposure_yml_one_exposure(self):
        change = ResourceChange(
            operation=Operation.Remove,
            entity_type=EntityType.Exposure,
            identifier=exposure_name,
            path=current_path,
            data={},
        )
        full_yml = ResourceFileEditor.remove_resource(
            properties=read_yml(exposure_yml_one_exposure), change=change
        )

        assert full_yml == {}

    def test_remove_exposure_yml_multiple_exposures(self):
        change = ResourceChange(
            operation=Operation.Remove,
            entity_type=EntityType.Exposure,
            identifier=exposure_name,
            path=current_path,
            data={},
        )
        full_yml = ResourceFileEditor.remove_resource(
            properties=read_yml(exposure_yml_multiple_exposures), change=change
        )

        assert full_yml == read_yml(expected_remainder_yml__multiple_exposures)

    def test_remove_metric_yml_one_metric(self):
        change = ResourceChange(
            operation=Operation.Remove,
            entity_type=EntityType.Metric,
            identifier=metric_name,
            path=current_path,
            data={},
        )
        full_yml = ResourceFileEditor.remove_resource(
            properties=read_yml(metric_yml_one_metric), change=change
        )
        assert full_yml == {}

    def test_remove_metric_yml_multiple_metrics(self):
        change = ResourceChange(
            operation=Operation.Remove,
            entity_type=EntityType.Metric,
            identifier=metric_name,
            path=current_path,
            data={},
        )
        full_yml = ResourceFileEditor.remove_resource(
            properties=read_yml(metric_yml_multiple_metrics), change=change
        )

        assert full_yml == read_yml(expected_remainder_yml__multiple_metrics)


class TestAddResourceYml:
    def test_add_model_yml_simple(self):
        data = read_yml(expected_remove_model_yml__model_yml_no_col_no_version)
        change = ResourceChange(
            operation=Operation.Add,
            entity_type=EntityType.Model,
            identifier=data["name"],
            path=current_path,
            data=data,
        )
        full_yml = ResourceFileEditor.update_resource(properties={}, change=change)

        assert full_yml == read_yml(model_yml_no_col_no_version)

    def test_add_model_yml_other_model(self):
        data = read_yml(expected_remove_model_yml__default)
        change = ResourceChange(
            operation=Operation.Add,
            entity_type=EntityType.Model,
            identifier=data["name"],
            path=current_path,
            data=data,
        )
        full_yml = ResourceFileEditor.update_resource(
            properties=read_yml(expected_remainder_yml__model_yml_other_model), change=change
        )

        assert full_yml == read_yml(model_yml_other_model)

    def test_add_source_yml_one_table(self):
        data = read_yml(expected_remove_source_yml__default)
        change = ResourceChange(
            operation=Operation.Add,
            entity_type=EntityType.Source,
            identifier=data["name"],
            path=current_path,
            data=data,
        )
        full_yml = ResourceFileEditor.update_resource(properties={}, change=change)

        assert full_yml == read_yml(source_yml_one_table)

    def test_add_source_yml_multiple_table(self):
        data = read_yml(expected_remove_source_yml__default)
        data["tables"] = NamedList(data["tables"])
        change = ResourceChange(
            operation=Operation.Add,
            entity_type=EntityType.Source,
            identifier=data["name"],
            path=current_path,
            data=data,
        )
        full_yml = ResourceFileEditor.update_resource(
            properties=read_yml(expeceted_remainder_yml__source_yml_multiple_tables), change=change
        )

        expected = read_yml(source_yml_multiple_tables)
        source_entry = list(filter(lambda x: x["name"] == source_name, full_yml["sources"]))
        expected_source_entry = list(
            filter(lambda x: x["name"] == source_name, expected["sources"])
        )
        source_tables = source_entry[0].pop("tables")
        expected_source_tables = expected_source_entry[0].pop("tables")
        assert source_entry == expected_source_entry
        assert sorted(source_tables, key=lambda x: x["name"]) == sorted(
            expected_source_tables, key=lambda x: x["name"]
        )

    def test_add_exposure_yml_one_exposure(self):
        data = read_yml(expected_remove_exposure_yml__default)
        change = ResourceChange(
            operation=Operation.Add,
            entity_type=EntityType.Exposure,
            identifier=data["name"],
            path=current_path,
            data=data,
        )

        full_yml = ResourceFileEditor.update_resource(properties={}, change=change)
        assert full_yml == read_yml(exposure_yml_one_exposure)

    def test_add_exposure_yml_multiple_exposures(self):
        data = read_yml(expected_remove_exposure_yml__default)
        change = ResourceChange(
            operation=Operation.Add,
            entity_type=EntityType.Exposure,
            identifier=data["name"],
            path=current_path,
            data=data,
        )
        full_yml = ResourceFileEditor.update_resource(
            properties=read_yml(expected_remainder_yml__multiple_exposures), change=change
        )

        assert sorted(full_yml["exposures"], key=lambda x: x["name"]) == sorted(
            read_yml(exposure_yml_multiple_exposures)["exposures"], key=lambda x: x["name"]
        )

    def test_add_metric_yml_one_metric(self):
        data = read_yml(expected_remove_metric_yml__default)
        change = ResourceChange(
            operation=Operation.Add,
            entity_type=EntityType.Metric,
            identifier=data["name"],
            path=current_path,
            data=data,
        )
        full_yml = ResourceFileEditor.update_resource(properties={}, change=change)

        assert full_yml == read_yml(metric_yml_one_metric)

    def test_add_metric_yml_multiple_metrics(self):
        data = read_yml(expected_remove_metric_yml__default)
        change = ResourceChange(
            operation=Operation.Add,
            entity_type=EntityType.Metric,
            identifier=data["name"],
            path=current_path,
            data=data,
        )
        full_yml = ResourceFileEditor.update_resource(
            properties=read_yml(expected_remainder_yml__multiple_metrics), change=change
        )

        assert sorted(full_yml["metrics"], key=lambda x: x["name"]) == sorted(
            read_yml(metric_yml_multiple_metrics)["metrics"], key=lambda x: x["name"]
        )
