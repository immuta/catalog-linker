import json
import logging
import pydash as py_
import requests

from lib.providers.provider import Provider

# Disable insecure request warnings for log readability
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class CollibraProvider(Provider):
    def __init__(self, config):
        '''
        (dict) config             - Collibra connection config
        (str)  config.id          - ID of the provider ('collibra')
        (str)  config.url         - Base URL of the provider
        (str)  config.username    - Username to authenticate with the provider
        (str)  config.password    - Password to authenticate with the provider
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

    def authenticate(self):
        '''
        Authenticates with Collibra, cookie is stored in the session
        automatically for future API calls
        '''
        url = f'{self._baseurl}/rest/2.0/auth/sessions'
        payload={'username': self._username, 'password': self._password}
        response = self._session.post(url, json=payload, verify=False)

    def search(self, asset_names):
        '''
        Search the Collibra catalog for the provided asset names, only the
        assets matching the types provided in the configuration file will be
        found

        (list) asset_names - list of asset names to search for in Collibra

        (list) return - returns a list of asset names and their ids that match
        '''
        # if only a single asset has been passed, ensure it's wrapped in a list
        asset_names = [asset_names] if type(asset_names) is str else asset_names

        found = []
        for asset_name in asset_names:
            params = {
                'typeIds': self._asset_types,
                'name': asset_name,
                'nameMatchMode': 'EXACT'
            }

            url = f'{self._baseurl}/rest/2.0/assets'
            response = self._session.get(url, params=params)
            results = py_.get(response.json(), 'results')
            found += [{'id': result['id'], 'name': result['name']} for result in results]

        return found
