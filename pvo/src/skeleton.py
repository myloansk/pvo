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

    @abstractmethod 
    def impute_nans(self)->None:
        """
        Impute Nans using using a specific strategy defined in concrete implementations
        """
        pass 

    @abstractmethod 
    def assemble(self)->DataFrame:pass



