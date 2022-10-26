import json
import pydash as py_
import requests

from time import sleep

# Disable insecure request warnings for log readability
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

import logging
logger = logging.getLogger('immuta')


class ImmutaConnection():
    def __init__(self, config):
        '''
        (dict) config          - Immuta connection config
        (str)  config.url      - Base URL of Immuta
        (str)  config.apikey   - API Key for authentication with Immuta
        (int)  config.limit    - Batch size limit, 0 is no limit
        (int)  config.throttle - Batch processing throttle time in seconds
        (str)  config.tls.ca   - Immuta CA certificate file path
        '''
        self._config = config
        self._baseurl = config['url']
        self._apikey = config['apikey']
        self._session = requests.Session()
        self._session.verify = py_.get(config, 'tls.ca', False)
        self._session.headers.update({ 'Content-Type': 'application/json'})

        # batch processing limit and throttle time should be 0 if negative
        self._limit = config['limit'] if config['limit'] >= 0 else 0
        self._throttle = config['throttle'] if config['throttle'] >= 0 else 0

    def authenticate(self):
        '''
        Authenticates with Immuta, authorization token is stored in the session
        headers
        '''
        url = f'{self._baseurl}/bim/apikey/authenticate'
        payload = {'apikey': self._apikey}
        response = self._session.post(url, json=payload, verify=False)

        # check response code
        rescode = response.status_code
        assert(rescode >= 200 and rescode < 300), f'Unable to authenticate with Immuta'

        # set token in session headers
        token = py_.get(response.json(), 'token')
        self._session.headers.update({ 'Authorization': token })

    def search(self):
        '''
        Search Immuta for all data sources that are not currently linked to an
        external catalog

        (list) return - returns a list of data source names and their ids
        '''
        url = f'{self._baseurl}/dataSource'
        params = {
            'size': self._limit,
            'offset': 0
        }

        # get the first batch of data sources and the total count
        first_response = self._session.get(url, params=params).json()
        total_results = first_response['count']
        yield self.process(first_response)

        # update the offset after first batch
        params['offset'] += self._limit

        # process batches when offset hasn't reached total results
        while params['offset'] < total_results:
            # process next batch
            response = self._session.get(url, params=params).json()
            yield self.process(response)

            # update the offset, sleep if throttle time has been set
            params['offset'] += self._limit
            sleep(self._throttle)

    def process(self, response):
        '''
        Process search results by finding all results that are not currently
        linked to an external catalog

        (object) response - search response object

        (list) return - returns list of processed results
        '''
        processed = []

        # filter out results that are already linked to an external catalog
        for hit in response['hits']:
            url = f'{self._baseurl}/dataSource/{hit["id"]}'
            datasource = self._session.get(url).json()
            catalog_metadata = py_.get(datasource, 'catalogMetadata', None)
            if not catalog_metadata:
                processed.append({'name': datasource['name'], 'id': datasource["id"]})

        return processed

    def link_catalog(self, provider_id, datasource, resource):
        '''
        Link a data source with an external catalog

        (str) provider_id   - the id of the provider being linked
        (str) datasource_id - the id of the Immuta data source being linked to
        (str) resource_id   - the id of the catalog resource being linked
        '''
        datasource_name = datasource['name']
        datasource_id = datasource['id']
        resource_name = resource['name']
        resource_id = resource['id']

        # size can be used to get a large number and then use the offset
        url = f'{self._baseurl}/dataSource/{datasource_id}'
        payload = {
            'catalogMetadata': {'id': resource['id'], 'provider': provider_id}
        }

        # attempt to link catalog
        response = self._session.put(url, json=payload)

        # check response code
        rescode = response.status_code
        if rescode >= 200 and rescode < 300:
            logger.info(f'Linked data source "{datasource_name}" (id={datasource_id}) to resource "{resource_name}" (id={resource_id})')
        else:
            logger.error(f'Unable to link data source "{datasource_name}" (id={datasource_id}) to resource "{resource_name}" (id={resource_id})')
