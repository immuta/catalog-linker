import logging
from time import sleep
logger = logging.getLogger('immuta')


import pydash as py_
import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger('immuta')


class ImmutaConnection():
    def __init__(self, config):
        """
        (dict) config          - Immuta connection config
        (str)  config.url      - Base URL of Immuta
        (str)  config.apikey   - API Key for authentication with Immuta
        (int)  config.limit    - Batch size limit, 0 is no limit
        (int)  config.throttle - Batch processing throttle time in seconds
        (str)  config.tls.ca   - Immuta CA certificate file path
        """
        self._baseurl = config['url']
        self._apikey = config['apikey']
        self._session = requests.Session()
        self._session.verify = py_.get(config, 'tls.ca', False)
        self._session.headers.update({'Content-Type': 'application/json'})

        # batch processing limit and throttle time should be 0 if negative
        self._limit = config['limit'] if config['limit'] >= 0 else 0
        self._throttle = config['throttle'] if config['throttle'] >= 0 else 0

    def authenticate(self):
        """
        Authenticates with Immuta, authorization token is stored in the session
        headers
        """
        url = f'{self._baseurl}/bim/apikey/authenticate'
        payload = {'apikey': self._apikey}
        response = self._session.post(url, json=payload, verify=False)

        # check response code
        rescode = response.status_code
        assert (200 <= rescode < 300), f'Unable to authenticate with Immuta'

        # set token in session headers
        token = py_.get(response.json(), 'token')
        self._session.headers.update({'Authorization': token})

    def search(self, provider):
        """
        Search Immuta for all data sources 
        (list) return - returns a list of data source names and their ids
        """
        url = f'{self._baseurl}/dataSource'
        params = {
            'size': self._limit,
            'offset': 0
        }

        # get the first batch of data sources and the total count
        response = self._session.get(url, params=params).json()
        

        yield self.process(response, provider)

        # update the offset after first batch
        params['offset'] += self._limit

        # process batches unless previous batch was empty
        hits = response['hits']
        while hits:
            # process next batch
            response = self._session.get(url, params=params).json()
            hits = response['hits']

            # if batch is not empty, yield
            if hits: yield self.process(response, provider)

            # update the offset, sleep if throttle time has been set
            params['offset'] += self._limit
            sleep(self._throttle)

    def process(self, response, provider):
        processed = []

        # Check if response contains the 'hits' key and is not empty
        if not response or 'hits' not in response or not response['hits']:
            logger.error("Invalid response format or no hits found in the response: %s", response)
            return processed

        # filter out results that are already linked to an external catalog
        for hit in response['hits']:
            url = f'{self._baseurl}/dataSource/{hit["id"]}'
            datasource = self._session.get(url).json()
            datasource_name = datasource['name']
            datasource_id = datasource['id']
            catalog_metadata = py_.get(datasource, 'catalogMetadata', None)
            handler_id = int(datasource['blobHandler']['url'].split('handler/')[-1])

            # Fetch tags for the Immuta datasource
            immuta_tags = self.get_tags(datasource_id)  # Using the refactored get_tags method

            processed.append({
                'name': datasource_name,
                'id': datasource_id,
                #'sqlTableFullName': self.full_name(datasource['blobHandler']['id']),
                'sqlTableFullName': self.full_name(handler_id),
                'tags': immuta_tags
            })

        return processed
    
    def compare_tags(self, provider_tags, immuta_tag_response):
        #immuta_tags = [tag['name'] for tag in immuta_tag_response]
        logger.debug(f"Comparing tags - Provider tags: {provider_tags}, Immuta tags: {immuta_tag_response}")
        #try:
        if not isinstance(provider_tags, list) or not isinstance(immuta_tag_response, list):
            logger.error(f"Invalid tag format. Provider tags: {provider_tags}, Immuta tags: {immuta_tag_response}")
            return False
        return set(provider_tags) == set(immuta_tag_response)


    def link_catalog(self, provider_id, datasource, resource):
        """
        Link a data source with an external catalog

        (str) provider_id   - the id of the provider being linked
        (str) datasource_id - the id of the Immuta data source being linked to
        (str) resource_id   - the id of the catalog resource being linked
        """
        if not isinstance(resource, dict):
            logger.error(f"Invalid resource format: expected a dictionary, got {type(resource)}")
            return
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
        if 200 <= response.status_code < 300:
            logger.info(
                f'Linked data source "{datasource_name}" (id={datasource_id}) to resource "{resource_name}" (id={resource_id})')
        else:
            logger.error(
                f'Unable to link data source "{datasource_name}" (id={datasource_id}) to resource "{resource_name}" (id={resource_id})')
            
    def full_name(self, handler_id:int):
        #using Immuta's databricks handler API to fetch datasource details
        try:
            url = f"{self._baseurl}/databricks/handler/{handler_id}"
            response = self._session.get(url)
            response.raise_for_status()
            parsed_response = response.json()
            return f"{parsed_response['metadata']['database']}>{parsed_response['metadata']['schema']}>{parsed_response['metadata']['table']}"
        except Exception as e:
            message = f"The call has failed due to {str(e)} for datasourceId: {handler_id}."
            logger.error(message)
    
    def unlink_catalog(self, datasource):
        """
        Unlink a catalog provider from a given datasource.

        (str) provider_id  - ID of the catalog provider to unlink.
        (dict) datasource  - The datasource object to unlink.
        """
        datasource_id = datasource['id']

        unlink_url = f"{self._baseurl}/dataSource/{datasource_id}"
        payload = {"catalogMetadata": None}
        try:
            response = self._session.put(unlink_url, json=payload)
            if response.status_code == 200:
                logging.info(f"Successfully unlinked provider collibra tags from datasource '{datasource['name']}'")
            else:
                logging.error(f"Failed to unlink provider collibra tags from datasource '{datasource['name']}': {response.text}")
        except Exception as e:
            logging.error(f"Error unlinking provider collibra tags from datasource '{datasource['name']}': {e}")
    
    # Fetch tags for a specific Immuta data source. (str) datasource_id - The ID of the Immuta data source. (list) return - Returns a list of tag names

    def get_tags(self, datasource_id):
        tags_url = f'{self._baseurl}/dataSource/{datasource_id}/tags'
        
        try:
            # Fetch tags from Immuta
            response = self._session.get(tags_url)
            response.raise_for_status()
            immuta_tag_response = response.json()
            logger.debug(f"API response for tags: {immuta_tag_response}")
            # Ensure immuta_tag_response is in the expected format
            if isinstance(immuta_tag_response, dict) and 'tags' in immuta_tag_response:
                return [tag['name'] for tag in immuta_tag_response['tags']]
            else:
                # Log unexpected response format
                logger.error(f"Unexpected Immuta tags response format: {immuta_tag_response}")
                return []

        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching Immuta tags for datasource ID {datasource_id}: {e}")
            return []
        except ValueError as e:  # JSON decoding error
            logger.error(f"Invalid JSON response for datasource ID {datasource_id}: {e}")
            return []