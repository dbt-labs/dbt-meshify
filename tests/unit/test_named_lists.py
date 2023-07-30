from dbt_meshify.storage.file_content_editors import NamedList


class TestNamedLists:
    def test_create_single_named_list(self):
        """Confirm a single NamedList can be created."""

        data = [{"name": "example"}]

        named_list = NamedList(data)
        print(named_list)
        assert named_list["example"] == {"name": "example"}

    def test_create_nested_named_list(self):
        """Confirm a single NamedList can be created."""

        data = [{"name": "example", "columns": [{"name": "column_one"}]}]

        named_list = NamedList(data)
        print(named_list)
        assert named_list["example"]["name"] == "example"
        assert named_list["example"]["columns"]["column_one"] == {"name": "column_one"}

    def test_to_list_reverses_operation(self):
        """Confirm that the `to_list` method reverses the transformation performed by NamedList."""
        data = [{"name": "example", "columns": [{"name": "column_one"}]}]

        named_list = NamedList(data)
        output_list = named_list.to_list()

        assert output_list == data
