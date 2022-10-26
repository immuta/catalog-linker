import json
import logging
import pydash as py_
import requests

from time import sleep

from lib.providers.provider import Provider

# Disable insecure request warnings for log readability
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

import logging
logger = logging.getLogger('collibra')


class CollibraProvider(Provider):
    def __init__(self, config):
        '''
        (dict) config             - Collibra connection config
        (str)  config.id          - ID of the provider ('collibra')
        (str)  config.url         - Base URL of the provider
        (str)  config.username    - Username to authenticate with the provider
        (str)  config.password    - Password to authenticate with the provider
        (int)  config.limit       - Batch size limit, 0 is no limit
        (int)  config.throttle    - Batch processing throttle time in seconds
        (list) config.asset_types - List of asset type UUIDS for searches
        (str)  config.tls.ca      - Collibra CA certificate file path
        '''
        self.id = config['id']
        self._config = config
        self._baseurl = config['url']
        self._username = config['username']
        self._password = config['password']
        self._asset_types = config['asset_types']
        self._session = requests.Session()
        self._session.verify = py_.get(config, 'tls.ca', False)
        self._session.headers.update({ 'Content-Type': 'application/json'})

        # batch processing limit and throttle time should be 0 if negative
        self._limit = config['limit'] if config['limit'] >= 0 else 0
        self._throttle = config['throttle'] if config['throttle'] >= 0 else 0

    def authenticate(self):
        '''
        Authenticates with Collibra, cookie is stored in the session
        automatically for future API calls
        '''
        url = f'{self._baseurl}/rest/2.0/auth/sessions'
        payload={'username': self._username, 'password': self._password}
        response = self._session.post(url, json=payload, verify=False)

        # check response code
        rescode = response.status_code
        assert(rescode >= 200 and rescode < 300), f'Unable to authenticate with Collibra'

    def search(self, asset_name):
        '''
        Search the Collibra catalog for the provided asset name, only the
        assets matching the types provided in the configuration file will be
        found

        (str) asset_name - asset name to search for in Collibra

        (list) return - returns a list of asset names and their ids that match
        '''
        url = f'{self._baseurl}/rest/2.0/assets'
        params = {
            'typeIds': self._asset_types,
            'name': asset_name,
            'nameMatchMode': 'EXACT',
        }

        # if batch processing, set the limit and offset
        batch_process = self._limit > 0
        if batch_process:
            params['limit'] = self._limit,
            params['offset'] = 0

        # get the first batch of resources and the total count
        first_response = self._session.get(url, params=params).json()
        total_results = first_response['total']
        found = self.process(first_response)

        # if batch processing, update the offset after first batch
        if batch_process: params['offset'] += self._limit

        # process batches if limit is set and offset hasn't reached total results
        while batch_process and params['offset'] < total_results:
            # process next batch
            response = self._session.get(url, params=params).json()
            found += self.process(response)

            # update the offset, sleep if throttle time has been set
            params['offset'] += self._limit
            sleep(self._throttle)

        return found

    def process(self, response):
        '''
        Process search results by filtering out irrelevant data

        (object) response - search response object

        (list) return - returns list of processed results
        '''
        processed = []
        for result in response['results']:
            processed.append({'id': result['id'], 'name': result['name']})

        return processed
