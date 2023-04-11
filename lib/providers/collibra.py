import logging
from time import sleep

import pydash as py_
import requests
import urllib3

from lib.providers.provider import Provider

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger('collibra')


class CollibraProvider(Provider):
    def __init__(self, config):
        """
        (dict) config             - Collibra connection config
        (str)  config.id          - ID of the provider ('collibra')
        (str)  config.url         - Base URL of the provider
        (str)  config.username    - Username to authenticate with the provider
        (str)  config.password    - Password to authenticate with the provider
        (int)  config.limit       - Batch size limit, 0 is no limit
        (int)  config.throttle    - Batch processing throttle time in seconds
        (list) config.asset_types - List of asset type UUIDS for searches
        (str)  config.tls.ca      - Collibra CA certificate file path
        """
        super().__init__()
        self.id = config['id']
        self._asset_types = config['asset_types']
        self._baseurl = config['url']
        self._password = config['password']
        self._username = config['username']
        self._match_mode = py_.get(config, 'match_mode', 'END')
        self._match_prefix = py_.get(config, 'match_prefix', '')
        self._session = requests.Session()
        self._session.verify = py_.get(config, 'tls.ca', False)
        self._session.headers.update({'Content-Type': 'application/json'})

        # batch processing limit and throttle time should be 0 if negative
        self._limit = config['limit'] if config['limit'] >= 0 else 0
        self._throttle = config['throttle'] if config['throttle'] >= 0 else 0

    def authenticate(self):
        """
        Authenticates with Collibra, cookie is stored in the session
        automatically for future API calls
        """
        url = f'{self._baseurl}/rest/2.0/auth/sessions'
        payload = {'username': self._username, 'password': self._password}
        response = self._session.post(url, json=payload, verify=False)

        assert (200 <= response.status_code < 300), f'Unable to authenticate with Collibra'

    def process(self, response):
        """
        Process search results by filtering out irrelevant data

        (object) response - search response object

        (list) return - returns list of processed results
        """
        processed = []

        # filter out unnecessary result data
        for result in response['results']:
            processed.append({'id': result['id'], 'name': result['name']})

        return processed

    def search(self, asset_name):
        """
        Search the Collibra catalog for the provided asset name, only the
        assets matching the types provided in the configuration file will be
        found

        (str) asset_name - asset name to search for in Collibra

        (list) return - returns a list of asset names and their ids that match
        """
        url = f'{self._baseurl}/rest/2.0/assets'
        params = {
            'typeIds': self._asset_types,
            'name': f'{self._match_prefix}{asset_name}',
            'nameMatchMode': self._match_mode,
            'limit': self._limit,
            'offset': 0
        }

        # get the first batch of resources and the total count
        response = self._session.get(url, params=params).json()
        processed = self.process(response)

        # update the offset after first batch
        params['offset'] += self._limit

        # process batches unless previous batch was empty
        results = response['results']
        while results:
            # process next batch
            response = self._session.get(url, params=params).json()
            results = response['results']

            # if results is not empty, process response
            if results: processed += self.process(response)

            # update the offset, sleep if throttle time has been set
            params['offset'] += self._limit
            sleep(self._throttle)

        return processed
