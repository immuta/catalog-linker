from unittest import TestCase
from unittest.mock import Mock

from lib.immuta import ImmutaConnection


class TestImmutaConnection(TestCase):
    def setUp(self):
        self.immutaConnection = ImmutaConnection({
            'apikey': 'API_KEY',
            'limit': 0,
            'url': 'https://tenant.hosted.immutacloud.com',
            'throttle': 0,
        })

        self.session_mock = Mock()

        self.put_mock = Mock()

        def put_mock(*args, **kwargs):
            response_mock = Mock()
            response_mock.status_code = 200
            return response_mock

        self.put_mock.side_effect = put_mock

        self.session_mock.put = self.put_mock

    def test_defaults(self):
        self.assertEqual(self.immutaConnection.__getattribute__('_apikey'), 'API_KEY')
        self.assertEqual(self.immutaConnection.__getattribute__('_limit'), 0)
        self.assertEqual(self.immutaConnection.__getattribute__('_throttle'), 0)

    def test_link_catalog(self):
        self.immutaConnection.__setattr__('_session', self.session_mock)
        self.immutaConnection.link_catalog(
            'Collibra',
            {
                'id': 'IMMUTA_ID_1',
                'name': 'Immuta Datasource Name 1'
            },
            {
                'id': 'COLLIBRA_ID_1',
                'name': 'Collibra Asset Name 1'
            }
        )
        self.assertEqual(self.put_mock.call_count, 1)
        self.put_mock.assert_called_with(
            'https://tenant.hosted.immutacloud.com/dataSource/IMMUTA_ID_1',
            json={
                'catalogMetadata': {
                    'id': 'COLLIBRA_ID_1',
                    'provider': 'Collibra'
                }
            }
        )
