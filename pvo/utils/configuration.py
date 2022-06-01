from dataclasses import dataclass 
from typing import List, Dict

@dataclass(frozen=True, init=False)
class SalesConfig:
    country_code:str
    company_code:List[str] 
    sales_org:List[str] 
    sales_file_path:str 
    calendar_file_path:str 
    filter_data_filtering_criteria_1:str 
    filter_data_MATERIAL_create: str
    filter_data_Direct_Sales_Volume_in_UC_create:str 
    filter_data_Indirect_Sales_NSR_create: str
    filter_data_Indirect_Sales_Volume_in_UC_create :str 
    filter_data_Direct_Sales_NSR_create:str 
    filter_data_calendar_selected_columns:List[str] 
    filter_data_column_rename_dict: Dict 
    filter_data_filtering_criteria_2 : str 
    feature_engineering_selected_columns:List[str]
    feature_engineering_Sales_Volume_in_UC_create :str 
    feature_engineering_Sales_NSR_create: str 
    impute_nans_filter_1: str 
    impute_nans_filter_2: str 
    impute_nans_drop_columns_list: List[str]
    impute_nans_join_operation : Dict 
    impute_nans_CURRENCY_create : str 
    impute_nans_drop_column_list_v2: List[str]

@dataclass(frozen=True, init=False)
class CustomerConfig:
    country_code:str
    company_code:List[str] 
    sales_org:List[str] 
    customer_md_file_path:str 
    filtering_conditions:str
    CUST_ROUTED:str 
    second_digit_ccaf:str 
    CUST_CCAF_GROUP:str 
    LATITUDE:str 
    LONGITUDE:str 
    LATITUDE:str 
    customer_selected_columns:List[str]
    rename_columns_dict:Dict 

@dataclass(frozen=True, init=False)
class DemographicsConfig:
    country_code:str
    company_code:List[str] 
    sales_org :List[str] 
    demographics_file_path:str
    age_group_column_list:List[str]
    traffic_hours:List[str]
    wvce_01_pc:str
    wvce_02_pc:str
    TF_WINTER:str 
    TF_SUMMER:str 
    Weekend_traffic_perc_diff:str 
    Season_traffic_perc_diff:str
    population_density:str
    competitor_count_density:str 

@dataclass(frozen=True, init=False)
class CoolerConfig:
    country_code:str 
    company_code: List[str]
    sales_org:List[str]
    cooler_file_path:str 
    selected_coolers_column_list:List[str]
    filtering_cooler_conditions:str 

