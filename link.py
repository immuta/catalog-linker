import json
import logging
from time import strftime

import pydash as py_
import urllib3
import yaml

# Set up logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(level=logging.INFO)

from lib.immuta import ImmutaConnection
from lib.providers.factory import ProviderFactory

CONFIG_FILE = 'config.yaml'
CONFIG = None

def handle_multiples(multiples):

    timestamp = strftime('%Y%m%d-%H%M%S')
    filename = f'results/results-{timestamp}.txt'

    try:
        with open(filename, 'w') as f:
            f.write(json.dumps({'multiples': multiples}))
            logging.info(f'Multiple resources were found for one or more data sources, please see {filename}')
    except OSError as e:
        logging.error(f'Unable to open results file at {e.filename}: {e.strerror}')
        print(json.dumps({'multiples': multiples}))
        exit()


def link(immuta, provider):
    """
    Attempt to link all unlinked Immuta data sources as well as incorrectly linked
    resources in Collibra. If multiple resources are found for a
    single Immuta data source, these will not be linked and instead have their
    information written to file for manual linking at a later time.

    (ImmutaConnection) immuta   - object used to interface with Immuta
    (Provider)         provider - object used to interface with the provider
    """
    multiples = []
    for page in immuta.search(provider):  # search is a generator
        for datasource in page:
            # attempt to find a matching resource in the provider
            resources = provider.search(datasource['sqlTableFullName'])
            logging.debug(f"Resources for datasource '{datasource['sqlTableFullName']}': {resources}")

            if isinstance(resources, list):
                if len(resources) > 1:
                    # Handle case for multiple resources, if needed
                    logging.warning(f"Multiple resources found for {datasource['sqlTableFullName']}")
                elif len(resources) == 0:
                    logging.info(f"No resources found for {datasource['sqlTableFullName']}")
                else:
                    # Process the single resource
                    resource = resources[0]  # Assuming it's a list, take the first element
                    # Ensure resource is a dictionary
                    if isinstance(resource, dict):
                        resource_name = resource.get('name', 'Unnamed resource')
                        logging.info(f"Processing Collibra resource: {resource_name}")
                    else:
                        logging.error(f"Invalid resource format: expected a dictionary, got {type(resource)}")
            else:
                logging.error(f"Invalid response format: expected a list, got {type(resources)}")

            if not isinstance(resources, list):
                logging.error(f"Expected a list of resources, but got: {type(resources)}")
                continue

            # when multiple resources are found, save info to write to file later
            if len(resources) > 1:
                multiples.append({'datasource': datasource, 'resources': resources})
                continue

            if not resources:
                logging.info(f"No resources found for datasource: {datasource['sqlTableFullName']}")
                continue

            # Ensure the resource is valid
            resource = resources[0]
            if not isinstance(resource, dict) or 'name' not in resource or 'id' not in resource:
                logging.error(f"Invalid resource format for datasource '{datasource['sqlTableFullName']}': {resource}")
                continue

            # Fetch tags from Immuta
            immuta_tags = []
            try:
                immuta_tag_response = immuta.get_tags(datasource['id'])
                
                # If the response is a list, iterate through it
                if isinstance(immuta_tag_response, list):
                    for item in immuta_tag_response:
                        if isinstance(item, dict):
                            tags = item.get('tags', [])
                            immuta_tags.extend([tag['name'] for tag in tags if isinstance(tag, dict) and 'name' in tag])
                        else:
                            logging.warning(f"Unexpected item format in Immuta tags response: {type(item)}")
                elif isinstance(immuta_tag_response, dict):
                    # Handle the case where response is a dictionary
                    immuta_tags = [tag['name'] for tag in immuta_tag_response.get('tags', [])]
                else:
                    logging.error(f"Unexpected response format for Immuta tags: {type(immuta_tag_response)}")
    
            except Exception as e:
                logger.error(f"Error fetching Immuta tags for datasource ID {datasource['id']}: {e}")
                continue

            # Fetch tags from Collibra
            collibra_tags = []
            try:
                collibra_tag_response = provider.get_tags_for_datasource(resources[0]['id'])
                
                # Check if the response is a list
                if isinstance(collibra_tag_response, list):
                    # Extract 'name' from each tag in the list
                    collibra_tags = [tag['name'] for tag in collibra_tag_response if isinstance(tag, dict) and 'name' in tag]
                else:
                    logging.error(f"Unexpected response format for Collibra tags: {type(collibra_tag_response)}")

            except Exception as e:
                logger.error(f"Error fetching Collibra tags for the asset {resources[0]['name']}: {e}")
                continue

            # Compare tags
            if not immuta.compare_tags(immuta_tag_response, collibra_tag_response):
                # Unlink due to tag mismatch
                logger.info(f"Tags mismatch for datasource '{datasource['sqlTableFullName']}'. Unlinking.")
                immuta.unlink_catalog(datasource)

                # Attempt to re-link immediately
                try:
                    immuta.link_catalog(provider.id, datasource, resources[0])
                    logger.info(f"Re-linked datasource '{datasource['sqlTableFullName']}' to resource '{resources[0]['name']}'")
                except Exception as e:
                    logger.error(f"Failed to re-link datasource '{datasource['sqlTableFullName']}': {e}")
            else:
                logger.info(f"Tags match for datasource '{datasource['sqlTableFullName']}'. No action needed.")

            # write info to file where multiple resources were found for a data source
            if len(multiples) > 0: handle_multiples(multiples)


def main():
    """
    Connect to both Immuta and the external catalog provider and attempt to link
    Immuta data sources to corresponding resources in the provider
    """
    try:  # to load the configuration file
        with open(CONFIG_FILE, 'r') as f:
            CONFIG = yaml.full_load(f)
            IMMUTA_CONFIG = py_.get(CONFIG, 'immuta')
            PROVIDER_CONFIG = py_.get(CONFIG, 'provider')
    except OSError as e:
        logging.error(f'Unable to open configuration file at {e.filename}: {e.strerror}')
        exit()

    # authenticate with Immuta
    immuta = ImmutaConnection(IMMUTA_CONFIG)
    immuta.authenticate()

    # authenticate with the provider
    provider = ProviderFactory.build(PROVIDER_CONFIG)
    provider.authenticate()

    # attempt to link Immuta data sources with provider resources
    link(immuta, provider)


if __name__ == '__main__':
    main()
