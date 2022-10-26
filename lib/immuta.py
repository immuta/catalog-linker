import json
import logging
import pydash as py_
import requests

# Disable insecure request warnings for log readability
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(level=logging.INFO)


class ImmutaConnection():
    def __init__(self, config):
        '''
        (dict) config        - Immuta connection config
        (str)  config.url    - Base URL of Immuta
        (str)  config.apikey - API Key for authentication with Immuta
        (str)  config.tls.ca - Immuta CA certificate file path
        '''
        self._config = config
        self._baseurl = config['url']
        self._apikey = config['apikey']
        self._session = requests.Session()
        self._session.verify = py_.get(config, 'tls.ca', False)
        self._session.headers.update({ 'Content-Type': 'application/json'})

    def authenticate(self):
        '''
        Authenticates with Immuta, authorization token is stored in the session
        headers
        '''
        url = f'{self._baseurl}/bim/apikey/authenticate'
        payload = {'apikey': self._apikey}
        response = self._session.post(url, json=payload, verify=False)
        token = py_.get(response.json(), 'token')
        self._session.headers.update({ 'Authorization': token })

    def search(self):
        '''
        Search Immuta for all data sources that are not currently linked to an
        external catalog

        (list) return - returns a list of data source names and their ids
        '''
        url = f'{self._baseurl}/dataSource'
        params = {'size': 100}
        datasources = self._session.get(url, params=params).json()

        found = []
        for datasource in datasources['hits']:
            catalog_metadata = py_.get(datasource, 'catalogMetadata', None)
            if(not catalog_metadata):
                found.append({'name': datasource['name'], 'id': datasource['id']})

        return found

    def link_catalog(self, provider_id, datasource_id, resource_id):
        '''
        Link a data source with an external catalog

        (str) provider_id   - the id of the provider being linked
        (str) datasource_id - the id of the Immuta data source being linked to
        (str) resource_id   - the id of the catalog resource being linked
        '''
        payload = {
            'catalogMetadata': {'id': resource_id, 'provider': provider_id}
        }

        # size can be used to get a large number and then use the offset
        url = f'{self._baseurl}/dataSource/{datasource_id}'
        response = self._session.put(url, json=payload)
