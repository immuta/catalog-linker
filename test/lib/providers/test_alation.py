from unittest import TestCase
from unittest.mock import Mock

from json import loads

from lib.providers.alation import AlationProvider


class TestAlationProvider(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.alation_api_response_1 = """[
    {
        "id": "123",
        "name": "RCV_DATA",
        "schema_name": "DATABASE_1.SCHEMA_1_A",
        "description": "Test table 1"
    },
    {
        "id": "456",
        "name": "RCV_DATA",
        "schema_name": "DATABASE_1.SCHEMA_1_B",
        "description": "Test table 2"
    }
]"""

        cls.alation_api_response_empty = "[]"

    def setUp(self):
        self.alationProvider = AlationProvider({
            'id': 'alation',
            'url': 'https://alation.company.com',
            'apikey': 'test-api-key',
            'throttle': 0.1
        })

        self.session_mock = Mock()

        self.json_mock = Mock()
        self.json_mock.side_effect = [
            loads(self.alation_api_response_1)
        ]

        self.get_mock = Mock()

        def get_mock(*args, **kwargs):
            response_mock = Mock()
            response_mock.json = self.json_mock
            response_mock.raise_for_status = Mock()
            return response_mock

        self.get_mock.side_effect = get_mock
        self.session_mock.get = self.get_mock
        self.alationProvider._session = self.session_mock

    def test_process_results(self):
        """Test that results are processed correctly"""
        results = loads(self.alation_api_response_1)
        processed = self.alationProvider.process(results)
        
        self.assertEqual(len(processed), 2)
        self.assertEqual(processed[0]['id'], '123')
        self.assertEqual(processed[0]['name'], 'RCV_DATA')

    def test_search_with_schema(self):
        """Test search with schema and database info"""
        self.alationProvider._session = self.session_mock
        
        datasource = {
            'name': 'RCV_DATA',
            'table_name': 'RCV_DATA',
            'database': 'DATABASE_1',
            'schema': 'SCHEMA_1'
        }
        
        processed = self.alationProvider.search(datasource)
        
        self.assertEqual(self.json_mock.call_count, 1)
        self.get_mock.assert_called_with(
            'https://alation.company.com/catalog/table/?name=rcv_data&schema_name=database_1.schema_1'
        )
        self.assertEqual(len(processed), 2)

    def test_search_empty_results(self):
        """Test handling of empty results"""
        self.alationProvider._session = self.session_mock
        self.json_mock.side_effect = [loads(self.alation_api_response_empty)]
        
        datasource = {
            'name': 'NONEXISTENT',
            'table_name': 'NONEXISTENT',
            'database': 'DATABASE_1',
            'schema': 'SCHEMA_1'
        }
        
        processed = self.alationProvider.search(datasource)
        self.assertEqual(len(processed), 0)


    def test_throttle_configuration(self):
        """Test that throttle configuration is properly set"""
        provider = AlationProvider({
            'id': 'alation',
            'url': 'https://alation.company.com',
            'apikey': 'test-api-key',
            'throttle': 0.1
        })
        self.assertEqual(provider._throttle, 0.1)

        # Test negative throttle becomes 0
        provider = AlationProvider({
            'id': 'alation',
            'url': 'https://alation.company.com',
            'apikey': 'test-api-key',
            'throttle': -1
        })
        self.assertEqual(provider._throttle, 0) 