from abc import abstractmethod, ABC
from re import A 
from typing import Any, Dict, List 

class LayerThree(ABC):

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

    def load_customer_md(self)->None:pass 

    @abstractmethod
    def load_openings_and_health_summary(self)->None:pass 

    @abstractmethod
    def load_customer_coverage(self)->None:pass 

    @abstractmethod
    def fac_calendar(self)->None:pass 

    @abstractmethod
    def load_material_md(self)->None:pass 

    @abstractmethod
    def load_red_index(self)->None:pass 

    @abstractmethod
    def red_availability(self)->None:pass 

    @abstractmethod
    def load_cooler(self)->None:pass 
    
    @abstractmethod
    def load_red_benchmark(self)->None:pass 

    @abstractmethod
    def load_red_displays(self)->None:pass 

    @abstractmethod
    def load_suggestions(self)->None:pass 

    @abstractmethod
    def load_strike_rate_crm(self)->None:pass 

    @abstractmethod
    def load_crm_activities(self)->None:pass 

    @abstractmethod
    def load_weather(self)->None:pass 

    @abstractmethod
    def load_sales(self)->None:pass 

    @abstractmethod
    def load_region_demographics(self)->None:pass 

    @abstractmethod
    def load_demographics(self)->None:pass 

    def load_events_calendar(self)->None:pass 

    @abstractmethod
    def sellout(self)->None:pass 

    @abstractmethod
    def load_strata_riscata(self)->None:pass 

    @abstractmethod
    def load_dove_convene(self)->None:pass 

    @abstractmethod
    def load_geouniq(self)->None:pass 

    @abstractmethod
    def load_customers_gnlc(self)->None:pass 

    @abstractmethod
    def load_combine_survey(self)->None:pass 

    @abstractmethod
    def load_out_of_home_universe(self)->None:pass 

