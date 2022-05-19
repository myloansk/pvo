from dataclasses import dataclass 
from typing import List, Dict

@dataclass 
class App:
    pp_name: str 
    data_eng_f: str 
    data_sc_f: str 
    data_udm_f: str 
    version: str 
    case:List[str]

@dataclass 
class Country:
    cc:str 
    sales_org:List[str] 
    company_code:List[str] 

@dataclass 
class DB:
    dbname_l1: str 
    dbname_l2: str 
    dbname_l3: str

@dataclass 
class Layers:
    l1_name: str 
    l2_name: str 
    l3_name: str 
    l4_name: str
@dataclass 
class DestFilePath:
    adlpath_l0: str 
    adlpath_l1: str 
    adlpath_l2: str 
    adlpath_udm: str

@dataclass 
class LayerOneConfig:

@dataclass 
class LayerTwoConfig:

@dataclass 
class LayerThreeConfig: 
    


@dataclass 
class PvoConfig:
    app_name: str 
    data_eng_f: str 
    data_sc_f: str 
    data_udm_f: str 
    version: str 
    case:List[str]
    adl_root_path_gen1: str
    adl_root_path_gen2: str 
    l1_name: str 
    l2_name: str 
    l3_name: str 
    l4_name: str
    dbname_l1: str 
    dbname_l2: str 
    dbname_l3: str
    adlpath_l0: str 
    adlpath_l1: str 
    adlpath_l2: str 
    adlpath_udm: str
    data_sources:Dict[Dict[Dict[str]]]
    sales_org:Dict 
    company_code:Dict 

    def print_instance_attributes(self):
        for attribute, value in self.__dict__.items():
            print(attribute, '=', value) 

    def get_layer_two_parameters(self)->Dict:
        return {
                'dbname01':self.dbname_l1, 
                'dbname02':self.dbname_l2,
                'adlpathl0': self.adl_path_l0,
                'aldpathl1': self.adl_path_l1,
                'aldpathl2': self.adl_path_l2,
                'sale_org': self.current_sales_org,
                'company_code': self.company_code
                }

    def get_layer_three_parameters(self)->Dict: 

    def get_layer_one_parameters(self)->Dict:pass 

        
