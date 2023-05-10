###Simply Adding the Lineage and running in the Current Run Context
with open('./core/lineagegraph.py', 'r') as f:
    script = f.read()
exec(script)

import unittest
from unittest.mock import patch
from gremlin_python.driver.protocol import GremlinServerError

class TestLineageGraph(unittest.TestCase):

    def setUp(self,endpoint, key, partition_key):
        self.endpoint = endpoint
        self.key = key
        self.partition_key = partition_key
        self.graph = LineageGraph(self.endpoint, self.key, self.partition_key)

    def test_init_with_valid_arguments(self):
        self.assertIsNotNone(self.graph.client)
        self.assertEqual(self.graph.partition_key, self.partition_key)

    @patch.object(LineageGraph, "print_status_attributes")
    def test_add_vertex_with_partition_key(self, mock_print_status_attributes):
        label = "test-label"
        properties = {"id": "test-id", self.partition_key: "test-value"}
        self.graph.add_vertex(label, properties)
        mock_print_status_attributes.assert_called_once()

    @patch.object(LineageGraph, "print_status_attributes")
    def test_add_vertex_without_partition_key(self, mock_print_status_attributes):
        label = "test-label"
        properties = {"id": "test-id"}
        self.graph.add_vertex(label, properties)
        mock_print_status_attributes.assert_not_called()

    @patch.object(LineageGraph, "print_status_attributes")
    def test_get_vertices_with_valid_arguments(self, mock_print_status_attributes):
        label = "test-label"
        id = "test-id"
        result = self.graph.get_vertices(label, id)
        self.assertIsNotNone(result)
        mock_print_status_attributes.assert_called_once()

    @patch.object(LineageGraph, "print_status_attributes")
    def test_get_vertices_with_invalid_arguments(self, mock_print_status_attributes):
        label = "test-label"
        id = "invalid-id"
        result = self.graph.get_vertices(label, id)
        self.assertIsNone(result)
        mock_print_status_attributes.assert_called_once()

    def test_is_vertex_with_valid_arguments(self):
        label = "test-label"
        id = "test-id"
        result = self.graph.is_vertex(label, id)
        self.assertTrue(result)

    def test_is_vertex_with_invalid_arguments(self):
        label = "test-label"
        id = "invalid-id"
        result = self.graph.is_vertex(label, id)
        self.assertFalse(result)

    @patch.object(LineageGraph, "print_status_attributes")
    def test_update_vertex_with_valid_arguments(self, mock_print_status_attributes):
        id = "test-id"
        properties = {"name": "test-name"}
        self.graph.update_vertex(id, properties)
        mock_print_status_attributes.assert_called_once()

    def test_insert_edges_with_valid_arguments(self):
        source_v_id = "test-source-id"
        target_v_id = "test-target-id"
        edge_label = "test-edge-label"
        properties = {"prop1": "value1", "prop2": "value2"}
        self.graph.insert_edges(source_v_id, target_v_id, edge_label, properties)
        # we can't assert for Gremlin queries results, so we just check if the function call was successful

    @patch.object(LineageGraph, "print_status_attributes")
    def test_drop_vertex_with_valid_arguments(self, mock_print_status_attributes):
        id = "test-id"
        self.graph.drop_vertex(id)
        mock_print_status_attributes.assert_called_once()

    @patch.object(LineageGraph, "print_status_attributes")
    @patch("builtins.print")
    def test_init_with_invalid_arguments(self, mock_print, mock_print_status_attributes):
        endpoint = "invalid-endpoint"
        key = "invalid-key"
        partition_key = "invalid-partition-key"
        with self.assertRaises(ValueError):
            Line
