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
    def customer_coverage(self):pass 

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

    @abstractmethod
    def md_customers(self):pass 

    @abstractmethod
    def sellout_items(self):pass 

    @abstractmethod
    def sellouts_shops(self):pass 

    @abstractmethod
    def sellouts_data(self):pass 

    @abstractmethod
    def sellouts_chains(self):pass 

    @abstractmethod
    def sellout_product_attributes(self):pass 

    @abstractmethod
    def sellouts(self):pass 

    @abstractmethod
    def red_index(self):pass 

    @abstractmethod
    def red_availability(self):pass 

    @abstractmethod
    def red_cooler(self):pass 

    @abstractmethod
    def events_calendar(self):pass 

    @abstractmethod
    def sales_di(self):pass 

    @abstractmethod
    def region_demographics(self):pass 

    @abstractmethod
    def demographics(self):pass 

    @abstractmethod
    def red_displays(self):pass 

    @abstractmethod
    def invoices_union_l2(self):pass 

    @abstractmethod
    def order_transformations(self):pass 

    @abstractmethod
    def eds_transformation(self):pass 

    @abstractmethod
    def sellout_dunhumby(self):pass 

    @abstractmethod
    def sellout_eyc(self):pass 

    @abstractmethod
    def ris_irproduct(self):pass 

    @abstractmethod
    def ris_rootlet(self):pass

    @abstractmethod
    def ris_ir_actual_facing(self):pass 

    @abstractmethod
    def uve(self):pass 

    @abstractmethod
    def ooh(self):pass 

    @abstractmethod
    def gnlc(self):pass 

    @abstractmethod
    def dove_beacon(self):pass 

    @abstractmethod
    def geo_hour_share(self):pass 
    
    @abstractmethod
    def geo_passing_traffic(self):pass

    @abstractmethod
    def geo_profiling_income(self):pass 

    @abstractmethod
    def geo_profiling_origin(self):pass 

    @abstractmethod
    def geo_visits(self):pass 

    @abstractmethod
    def old_surveys(self):pass 

    @abstractmethod
    def new_surveys(self):pass 

    @abstractmethod
    def degrees_of_freedom(self):pass 
    
    @abstractmethod
    def geo_pos_analysis(self):pass 

    @abstractmethod
    def demographics_out_of_home(self):pass 

    @abstractmethod
    def demographics_gnlc(self):pass 

    @abstractmethod
    def demographics_wvce(self):pass 

    @abstractmethod
    def demographic_wvce_GNLC(self):pass 

    @abstractmethod
    def demographic_wvce_out_of_home(self):pass 

    @abstractmethod
    def areas_borders(self):pass 

    @abstractmethod
    def sealout_mkt(self):pass 

    @abstractmethod
    def sellout_fct(self):pass

    @abstractmethod
    def sellout_prod(self):pass 

    @abstractmethod
    def sellouts_facts(self):pass 

    @abstractmethod
    def sellout_per(self):pass


class CaLayersTwo(ILayerTwo):pass 

class RuLayerTwo(ILayerTwo):pass 

class NgLayerTwo(ILayerTwo):pass 

class PlLayersTwo(ILayerTwo):pass 

class GrLayerTwo(ILayerTwo):pass 

class HuLayerTwo(ILayerTwo):pass 

class RoLayerTwo(ILayerTwo):pass 

class IeLayerTwo(ILayerTwo):pass 

class BgLayerTwo(ILayerTwo):pass 

class ItLayerTwo(ILayerTwo):pass