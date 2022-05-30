from datamesh_data_io import ( DataMeshDataIO, DataLakeLayer, 
                            DataLakeRepository, CuratedDataLakeObject, 
                            ActiveDirectoryApplicationConfiguration, StorageConfiguration,
                            RefreshMode, SynapseConfiguration, DatamartDataLakeObject
                            )
from typing import Any, Dict, List
from pyspark.sql import DataFrame

import pandas as pd
import pyspark.sql.functions as f
import pyspark.sql.types as t
import logging
import os 

class DataMeshLoaderMeta(type):
    """
    Define an Instance operation that lets clients access its unique
    instance.
    """

    def __call__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__call__(*args, **kwargs)
        return cls._instance

class DataMeshLoader(metaclass = DataMeshLoaderMeta):
    """
    _summary_

    _extended_summary_

    :param metaclass: _description_, defaults to DataMeshLoaderMeta
    :type metaclass: _type_, optional
    """
    def __init__(self)->None:
        self.__lib = DataMeshDataIO(spark_session=spark,
                            storage=StorageConfiguration.load(configuration_id=os.getenv("DATA_IO_STORAGE_ID")),
                            layer=DataLakeLayer.CURATED,
                            active_directory_application_configuration=ActiveDirectoryApplicationConfiguration.load(
                            configuration_id=os.getenv("DATA_IO_AD_ID")))  

        
    def set_data_lake(self, nameSpace:str, filePath:str, 
                            schemaVersion:str, source:str)->None:
        self.__data_lake_object = CuratedDataLakeObject(
            namespace = nameSpace,
            object_name = filePath,
            schema_version = schemaVersion,
            source = source,
            )

    
    @property
    def __getattribute__(self, __name: str) -> Any:
        return self.__name

    def bulk_loader(self, data_lake_object_list:List)->List[DataFrame]:
        return map(self.data_loader, data_lake_object_list)

    #def bulk_write(self, destPathDict:Dict)->None:
    #    map(self.write_to_datamesh,)
    
    def data_loader(self, data_lake_object:str )-> DataFrame:
        vaultDf = self.lib.repository(data_lake_object=data_lake_object).read()

        vaultDf = L1_Utils.fix_column_names(vaultDf)

        time = self.lib.repository(data_lake_object=data_lake_object)._delta_table().history()

        max_version = time.groupBy('operation').agg({'timestamp': 'max'}).filter(f.fcol("operation") == "WRITE").select("max(timestamp)").collect()[0][0]

        vaultDf = vaultDf.withColumn("CCH_DATA_UPDATED_DATE", f.lit(max_version.date())) 

        return vaultDf

    def write_to_datamesh(self, adlPath:str, dbNameL1:str, fileName:str)->None:
        try:
            vaultDf = self.data_loader()
            L1_Utils.write_to_location(dataframe=vaultDf,
                adlpath= adlPath,
                dbpath=  dbNameL1,
                filename= fileName,
                tabletype='tb'
                )
        except KeyError:
            logger.error("FileNotWirte : {}.".format(fileName))
