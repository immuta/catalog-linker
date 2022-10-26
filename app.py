import os
import pydash as py_
import yaml, json

from time import strftime

# suppress requests warnings for no verification
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from lib.immuta import ImmutaConnection
from lib.providers.factory import ProviderFactory


CONFIG_FILE = 'config.yaml'
CONFIG = None


def handle_multiples(multiples):
    '''
    Writes info of resources and the Immuta data source they potentially
    correspond to to file for manual catalog linking.

    (dict) multiples - dictionary of immuta data sources and provider resources
    '''
    timestamp = strftime('%Y%m%d-%H%M%S')
    filename = f'results-{timestamp}.txt'

    with open(filename, 'w') as f:
        f.write(json.dumps({'multiples': multiples}))
        print(f'Multiple resources were found for one or more data sources, please see {filename}')

def link(immuta, provider):
    '''
    Attempt to link all unlinked Immuta data sources with their corresponding
    resources in an external catalog. If multiple resources are found for a
    single Immuta data source, these will not be linked and instead have their
    information written to file for manual linking at a later time.

    (ImmutaConnection) immuta   - object used to interface with Immuta
    (Provider)         provider - object used to interface with the provider
    '''
    datasources = immuta.search()

    multiples = []
    for datasource in datasources:
        # search provider for data source names to find matching resource
        resources = provider.search(datasource['name'])

        # when multiple resources are found, save info to write to file later
        if len(resources) > 1:
            multiples.append({'datasource': datasource, 'resources': resources})
            continue

        # attempt to link the resource to the data source when found
        if len(resources) > 0:
            resource_id = py_.get(resources, '[0].id')
            immuta.link_catalog(provider.id, datasource['id'], resource_id)

    # write info to file where multiple resources were found for a data source
    if len(multiples) > 0: handle_multiples(multiples)


def main():
    '''
    Connect to both Immuta and the external catalog provider and attempt to link
    Immuta data sources to corresponding resources in the provider
    '''
    try: # to load the configuration file
        with open(CONFIG_FILE, 'r') as f:
            CONFIG = yaml.full_load(f)
            IMMUTA_CONFIG = py_.get(CONFIG, 'immuta')
            PROVIDER_CONFIG = py_.get(CONFIG, 'provider')
    except OSError as e:
        print(f'Unable to open configuration file at {e.filename}: {e.strerror}')
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
