from lib.providers.collibra import CollibraProvider
from lib.providers.provider import ProviderEnum


class ProviderFactory():
    '''
    Factory for Provider objects
    '''
    def __init__(self):
        pass

    @staticmethod
    def build(config):
        '''
        Builds the proper provider object based on the id provided in the
        configuration file.

        (dict) config - provider configurations from the config file

        (Provider) return - returns a Provider object
        '''
        provider_id = config['id']
        provider = None

        # match provided identifier with proper type, create provider object
        if provider_id == ProviderEnum.COLLIBRA.value:
            provider = CollibraProvider(config)

        return provider
