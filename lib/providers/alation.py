"""
Alation implementation of the catalog provider interface.
"""
import logging
from typing import Dict, Optional
from time import sleep

import pydash as py_
import requests

from .provider import Provider

logger = logging.getLogger('alation')

class AlationProvider(Provider):
    """
    Alation catalog provider implementation.
    """
    
    def __init__(self, config):
        """
        (dict) config          - Alation connection config
        (str)  config.id       - ID of the provider ('alation')
        (str)  config.url      - Base URL of the provider
        (str)  config.apikey   - API key to authenticate with the provider
        (int)  config.throttle - Per datasource processing throttle time in seconds
        """
        super().__init__()
        self.id = config['id']
        self._baseurl = config['url'].rstrip('/')
        self._api_key = config['apikey']
        self._session = requests.Session()
        self._session.headers.update({
            'Content-Type': 'application/json',
            'TOKEN': self._api_key
        })
        # throttle time should be 0 if negative
        self._throttle = config['throttle'] if config['throttle'] >= 0 else 0

    def authenticate(self):
        """
        Verifies authentication with Alation using the API key
        """
        url = f'{self._baseurl}/integration/tag/'
        response = self._session.get(url)
        assert (200 <= response.status_code < 300), 'Unable to authenticate with Alation'

    def process(self, response):
        """
        Process search results by filtering out irrelevant data

        (object) response - search response object

        (list) return - returns list of processed results
        """
        processed = []
        
        # filter out unnecessary result data
        for result in response:
            processed.append({
                'id': result['id'],
                'name': result['name'],
            })
        return processed

    def search(self, datasource):
        """
        Search the Alation catalog for the provided asset name

        (dict) datasource - dictionary containing:
            - name: name of asset to search for in Alation
            - id: id of the datasource
            - table_name: name of table (optional)
            - database: database name (optional)
            - schema: schema name (optional)

        (list) return - returns a list of asset names and their ids that match
        """
        try:
            # Build query exactly as alation.ts does
            table_name = datasource.get('table_name')
            if table_name is None:
                return []
            schema_name = f"{datasource['database'].lower()}.{datasource['schema'].lower()}"
            query = f"name={table_name.lower()}&schema_name={schema_name}"
            url = f'{self._baseurl}/catalog/table/?{query}'
            response = self._session.get(url)
            response.raise_for_status()
            
            results = response.json()
            if not results:
                return []

            # Sleep for throttle time if configured, should be less than 1 second ideally
            sleep(self._throttle)

            return self.process(results)

        except Exception as e:
            logger.error(f"Failed to search Alation: {str(e)}")
            return []