###Simply Adding the Lineage and running in the Current Run Context
with open('./core/TestLineageClass.py', 'r') as f:
    script = f.read()
exec(script)

import unittest
from unittest.mock import Mock
from gremlin_python.driver import client, serializer, protocol
from gremlin_python.driver.protocol import GremlinServerError
import sys
import traceback
import asyncio

if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

class RunTestLineageGraph(unittest.TestCase):

    def setUp(self):
        self.endpoint = 'endpoint'
        self.key = 'key'
        self.partition_key = 'partition_key'
        self.graph = TestLineageGraph(self.endpoint, self.key, self.partition_key)
        
    def test_add_vertex(self):
        label = 'person'
        properties = {'name': 'John Doe', 'age': 30, 'partition': 'A'}
        self.graph.add_vertex(label, properties)
        # add assertion here to check if vertex is added successfully

    def test_get_vertices(self):
        label = 'person'
        id = '1'
        vertex = self.graph.get_vertices(label, id)
        # add assertion here to check if the returned vertex matches the expected value

    def test_is_vertex(self):
        label = 'person'
        id = '1'
        result = self.graph.is_vertex(label, id)
        # add assertion here to check if the returned result matches the expected value

    def test_update_vertex(self):
        id = '1'
        properties = {'name': 'John Smith', 'age': 35}
        self.graph.update_vertex(id, properties)
        # add assertion here to check if vertex is updated successfully

    def test_insert_edges(self):
        source_v_id = '1'
        target_v_id = '2'
        edge_label = 'knows'
        properties = {'since': '2010'}
        self.graph.insert_edges(source_v_id, target_v_id, edge_label, properties)
        # add assertion here to check if edge is added successfully

    def test_drop_vertex(self):
        id = '1'
        self.graph.drop_vertex(id)
        # add assertion here to check if vertex is dropped successfully
        
if __name__ == '__main__':
    unittest.main()
