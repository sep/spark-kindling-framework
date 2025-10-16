from abc import ABC, abstractmethod

from kindling.injection import *

class PlatformServiceProvider(ABC):
    @abstractmethod
    def set_service(self, svc):
        pass

    @abstractmethod
    def get_service(self):
        pass

@GlobalInjector.singleton_autobind()
class SparkPlatformServiceProvider(PlatformServiceProvider):
    svc = None

    def set_service(self, svc):
        self.svc = svc

    def get_service(self):
        return self.svc