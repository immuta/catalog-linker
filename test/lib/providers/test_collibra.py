from unittest import TestCase
from unittest.mock import Mock

from json import loads

from lib.providers.collibra import CollibraProvider


class TestCollibraProvider(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.collibra_api_response_1 = """{
  "total": 2,
  "offset": 0,
  "limit": 1000,
  "results": [
    {
      "id": "GUID_ASSET_1",
      "resourceType": "Asset",
      "name": "Snowflake>DATABASE_1>SCHEMA_1_A>RCV_DATA",
      "displayName": "RCV_DATA",
      "domain": {
        "id": "GUID_DOMAIN_1",
        "resourceType": "Domain",
        "name": "Snowflake"
      },
      "type": {
        "id": "GUID_ASSET_TYPE_1",
        "resourceType": "AssetType",
        "name": "Table"
      }
    },
    {
      "id": "GUID_ASSET_2",
      "resourceType": "Asset",
      "name": "Snowflake>DATABASE_1>SCHEMA_1>RCV_DATA",
      "displayName": "RCV_DATA",
      "domain": {
        "id": "GUID_DOMAIN_1",
        "resourceType": "Domain",
        "name": "Snowflake"
      },
      "type": {
        "id": "GUID_ASSET_TYPE_1",
        "resourceType": "AssetType",
        "name": "Table"
      }
    }
  ]
}
        """

        cls.collibra_api_response_2 = """{
  "total": 0,
  "offset": 2,
  "limit": 1000,
  "results": [
  ]
}
        """

    def setUp(self):
        self.collibraProvider = CollibraProvider({
            'asset_types': [],
            'id': 'collibra',
            'limit': 0,
            'match_prefix': 'test>prefix>',
            'match_mode': 'EXACT',
            'password': 'password',
            'url': 'https://immuta.collibra.com',
            'username': 'username',
            'throttle': 0,
        })

        self.session_mock = Mock()

        self.json_mock = Mock()
        self.json_mock.side_effect = [
            loads(self.collibra_api_response_1),
            loads(self.collibra_api_response_2)
        ]

        self.get_mock = Mock()

        def get_mock(*args, **kwargs):
            response_mock = Mock()
            response_mock.json = self.json_mock
            return response_mock

        self.get_mock.side_effect = get_mock

        self.session_mock.get = self.get_mock

    def tearDown(self):
        pass

    def test_defaults(self):
        self.assertEqual(self.collibraProvider.__getattribute__('_limit'), 0)
        self.assertEqual(self.collibraProvider.__getattribute__('_match_mode'), 'EXACT')
        self.assertEqual(self.collibraProvider.__getattribute__('_throttle'), 0)

    def test_process_exact_match(self):
        self.collibraProvider.__setattr__('_session', self.session_mock)
        processed = self.collibraProvider.search('table_name_1')
        self.assertEqual(self.json_mock.call_count, 2)
        self.get_mock.assert_called_with(
            'https://immuta.collibra.com/rest/2.0/assets',
            params={
                'typeIds': [],
                'name': 'test>prefix>table_name_1',
                'nameMatchMode': 'EXACT',
                'limit': 0,
                'offset': 0
            }
        )
        self.assertEqual(len(processed), 2)
        self.assertEqual(processed[0]['id'], 'GUID_ASSET_1')
        self.assertEqual(processed[0]['name'], 'Snowflake>DATABASE_1>SCHEMA_1_A>RCV_DATA')
        self.assertEqual(processed[1]['id'], 'GUID_ASSET_2')
        self.assertEqual(processed[1]['name'], 'Snowflake>DATABASE_1>SCHEMA_1>RCV_DATA')
