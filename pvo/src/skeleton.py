from abc import ABC, abstractmethod
import pandas as pd
import pyspark.sql.functions as f
import pyspark.sql.types as t

from datetime import datetime
from dateutil.relativedelta import relativedelta
from pyspark.sql.window import Window
from functools import reduce
from pyspark.sql import DataFrame
from sklearn.neighbors import BallTree, KDTree

from utils import get_latest_modified_file_from_directory

spark.sql("set spark.sql.execution.arrow.pyspark.fallback.enabled=false")



class Source(ABC):
    """
    
    The Source defines a template method that contains a skeleton of
    pvo data processing algorithm, composed of calls to (usually) abstract primitive
    methods.

    Concrete subclasses should implement these operations to adapt to the needs of particular data source used in the project,
    but leave the template method itself intact.

    """
    @abstractmethod 
    def load_data(self)->None:
        """
        Loads the data to a pyspark dataframe
        """
        pass 

    @abstractmethod 
    def filter_data(self)->None:
        """
        Filter pyspark.DataFrame based on some business criteria defined in configuration file
        """
        pass 

    @abstractmethod 
    def feature_engineering(self)->None:
        """
        Creating additional features for input columns loaded to pyspark.DataFrame
        """
        pass 

    def assemble(self)->DataFrame:
        self.load_data()
        self.filter_data()
        self.feature_engineering()
        
        return self


class PvoModelling(ABC):
    """
    The PvoModelling defines a template method that contains a skeleton of
    pvo modelling phase algorithm, composed of calls to (usually) abstract primitive
    methods.

    Concrete subclasses should implement these operations with respect to the BU 
    used in the project, but leave the template method itself intact.
    """
    def __init__(self, configParsedDict)->None:
        self.abt:DataFrame = None 
        self.this_config = configParsedDict
    
    def load_abt_data(self):
        """
        Loads the latest anaylytical base table for particular BU
        to pyspark.DataFrame
        """
        #Get BU latest processed file 
        latestFileName = get_latest_modified_file_from_directory(self.this_config['country_input_data_directory']).rsplit("_",1).pop(0)
        print(f"This is the filepath:{latestFileName}")
        # Load BU data to a pyspark.DataFrame
        self.abtDf =  spark.read.option("header", "true").option("sep", ",").csv(latestFileName)
        self.abtDf = self.abtDf.select([f.col(colName).cast("double").alias(colName) if colName in self.this_config['cast_to_double_list'] else f.col(colName) for colName in self.abtDf.columns ]) 

    @abstractmethod
    def feature_selection(self):pass 

    @abstractmethod
    def modelling(self)->None:pass 

    @abstractmethod
    def calculate_performance_metrics(self)->None:pass 

    @abstractmethod
    def save_results(self)->None:pass 

    @abstractmethod 
    def post_modelling(self)->None:pass 

    def assemble(self)->None:
        self.load_abt_data()
        self.feature_selection()
        self.modelling()
        self.calculate_performance_metrics()
        self.save_results()
        self.post_modelling()






