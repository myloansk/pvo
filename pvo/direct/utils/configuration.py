from dataclasses import dataclass 
from typing import List, Dict

@dataclass(frozen=True, init=False) 
class DengConfig:
    sources:List[str]
    mapBook:Dict
    dbNameOne:str
    dbNameTwo:str
    dbNameThree:str
    adlPathZero:str 
    adlPathOne:str 
    adlPathTwo:str
    adlPathUdm:str

    def __get_version__(self)->str:pass

    @staticmethod
    def __create_adlpath__(adlPath:str, app_name:str=APP_NAME, env:str=ENVIRONMENT, mnt:str=MNT_POINT_GEN2, owner:str=DENG,org:str='cchbc', cc:str=COUNTRY_CODE):
        return "dbfs:/{mnt}/{org}/{env}/{adlPath}/{cc}/".format(env = env, org = org, mnt = mnt, adlPath =adlPath, cc = cc)
    
    @staticmethod
    def __create_dbname__(dbName:str, layerName:str, app_name:str=APP_NAME, env:str=ENVIRONMENT, mnt:str=MNT_POINT_GEN2, owner:str=DENG, cc:str=COUNTRY_CODE):
        return "{cc}_db_prod_{app_name}_{owner}_{layer}".format(app_name = app_name.lower(), owner = owner, cc = cc, layer = layerName)