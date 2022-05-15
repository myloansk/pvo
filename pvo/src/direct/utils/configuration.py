from dataclasses import dataclass
from dataclasses import dataclass 
from typing import List, Dict

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

        
