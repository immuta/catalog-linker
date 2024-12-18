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
        url = f'{self._baseurl}/rest/2.0/assets'
        params = {
            'typeIds': self._asset_types,
            'name': f'{self._match_prefix}{asset_name}',
            'nameMatchMode': self._match_mode,
            'limit': self._limit,
            'offset': 0
        }
        all_results = []
        max_iterations = 100  # Safety break condition
        iteration = 0

        while iteration < max_iterations:
            try:
                #logger.info(f"Fetching batch with offset {params['offset']}...")
                response = self._session.get(url, params=params, timeout=10)
                response.raise_for_status()
                response_json = response.json()
            except requests.exceptions.RequestException as e:
                logger.error(f"Error fetching data: {e}")
                break

            results = response_json.get('results', [])
            if not isinstance(results, list):
                logger.error("Unexpected response format: 'results' is not a list")
                break

            if not results:
                #logger.info("No more results to process. Exiting loop.")
                break

            for result in self.process({'results': results}):
                asset_id = result.get('id')
                if not asset_id:
                    logger.warning("Skipping result without an ID")
                    continue

            all_results.append(result)

            params['offset'] += len(results)
            iteration += 1
            logger.debug(f"Updated offset to {params['offset']}. Continuing to next batch.")
            sleep(self._throttle)

        #logger.info(f"Finished processing. Total assets processed: {len(all_results)}")
        return all_results
    
    #Fetches the tags associated with a specific datasource (asset) in Collibra. (str) datasource_id - The ID of the Collibra asset (datasource). (list) return - List of tags associated with the datasource.

    def get_tags_for_datasource(self, asset_id):
        
        url = f'{self._baseurl}/rest/2.0/assets/{asset_id}/tags'
        
        try:
            logger.debug(f"Fetching tags for Collibra asset ID: {asset_id}")
            response = self._session.get(url, headers={'accept': 'application/json'}, timeout=10)
            response.raise_for_status()
            tags = response.json()
            
            if not tags:
                logger.warning(f"No tags found for datasource ID {asset_id}")
                return []

            tag_names = [tag.get('name', 'Unnamed Tag') for tag in tags]
            logger.info(f"Tags for Collibra Asset with ID {asset_id}: {', '.join(tag_names)}")
            return tag_names

        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching tags for Collibra Asset ID {asset_id}: {e}")
            return []
