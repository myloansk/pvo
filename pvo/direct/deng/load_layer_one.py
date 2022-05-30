import os
import re
from datamesh_data_io import ( DataMeshDataIO, DataLakeLayer, 
                            DataLakeRepository, CuratedDataLakeObject, 
                            ActiveDirectoryApplicationConfiguration, StorageConfiguration,
                            RefreshMode, SynapseConfiguration, DatamartDataLakeObject
                            )
from pyspark.sql import functions as f 
from skeleton import DataMeshLoader
from utils import DataMeshConfig

class LayerOne:
    """
    LayerOne is responsible f

    _extended_summary_
    """
    def __init__(self, configParsed:DataMeshConfig)->None:
        """
        LayerOne loads the available data sources for each BU to corresponding DataMesh data table

        _extended_summary_

        :param cfg: dictionary with 
        :type cfg: PvoConfig
        """
        self._datalakeAccessObject = DataMeshLoader()
        self._this_config = configParsed

    def load_data_ca(self)->None:pass 

    def load_data_payments_ca(self)->None:pass 

    def load_data_special_treatment(self)->None:pass 

    def load_data_bu(self)->None:pass 




