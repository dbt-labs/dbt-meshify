import networkx
import pytest
from dbt.node_types import AccessType

from dbt_meshify.utilities.grouper import ResourceGrouper


class TestResourceGrouper:
    @pytest.fixture
    def example_graph(self):
        graph = networkx.DiGraph()
        graph.add_edges_from([("a", "b"), ("b", "c"), ("b", "d"), ("d", "1")])
        return graph

    def test_resource_grouper_boundary_classification(self, example_graph):
        nodes = {"a", "b", "c", "d"}
        resources = ResourceGrouper.classify_resource_access(example_graph, nodes)

        assert resources == {
            "a": AccessType.Private,
            "b": AccessType.Private,
            "c": AccessType.Public,
            "d": AccessType.Public,
        }
