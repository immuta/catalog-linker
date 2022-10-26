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
        pass

    def search(self, resource_names):
        pass
