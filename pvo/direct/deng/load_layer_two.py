from abc import abstractmethod, ABC 
from typing import Any, Dict, List 

class ILayerTwo(ABC):

    __cc:str = None
    __config:Dict = None

    @property
    def cc(self)->str:
        return self.__cc 

    @cc.setter
    def cc(self, cc:str)->None:
        self.__cc = cc 

    @property
    def config(self)->Dict: 
        return self.__config 

    @config.setter
    def config(self, configParsed:Dict)->None:
        self.__config = configParsed 

    @abstractmethod
    def md_sku(self):pass 

    @abstractmethod
    def trade_channel(self):pass 

    @abstractmethod
    def calendar(self):pass 

    @abstractmethod
    def md_sku_conversion(self):pass 

    @abstractmethod
    def suggestions(self):pass 

    @abstractmethod
    def strike_rate(self):pass 

    @abstractmethod
    def crm_activities(self):pass 

    @abstractmethod
    def customer_visits(self):pass 

    @abstractmethod
    def customer_coverage(self);pass 

    @abstractmethod
    def customer_door_openings(self):pass 

    @abstractmethod
    def door_and_health_summary(self):pass 

    @abstractmethod
    def price_list(self):pass 

    @abstractmethod
    def call_centre(self):pass 

    @abstractmethod
    def future_long(self):pass 

    @abstractmethod
    def history_long(self):pass 

    @abstractmethod
    def copa(self):pass 

    


class RuLayerTwo(ILayerTwo):pass 

class NgLayerTwo(ILayerTwo):pass 

class PlLayersTwo(ILayerTwo):pass 

class GrLayerTwo(ILayerTwo):pass 

class HuLayerTwo(ILayerTwo):pass 

class RoLayerTwo(ILayerTwo):pass 

class IeLayerTwo(ILayerTwo):pass 

class BgLayerTwo(ILayerTwo):pass 

class ItLayerTwo(ILayerTwo):pass