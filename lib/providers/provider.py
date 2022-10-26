from enum import Enum


class ProviderEnum(Enum):
    COLLIBRA = 'collibra'

class Provider():
    '''
    Interface for providers
    '''
    def __init__(self):
        pass

    def authenticate(self):
        '''
        Authenticate with the provider
        '''
        pass

    def search(self, resource_name):
        '''
        Search the provider for the given resource
        '''
        pass

    def process(self, response):
        '''
        Process a search response object
        '''
        pass
