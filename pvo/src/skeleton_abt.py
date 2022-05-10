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

class IDataPrep(ABC):
    """
    _summary_

    _extended_summary_

    :param ABC: _description_
    :type ABC: _type_
    """
    @abstractmethod 
    def loadData(self)->DataFrame:pass 

    @abstractmethod 
    def applyFilters(self)->DataFrame:pass 

    @abstractmethod 
    def featureEngineering(self)->DataFrame:pass 

    @abstractmethod
    def imputeNans(self)->DataFrame:pass 

    @abstractmethod
    def joinSources(self)->DataFrame:pass 

    @abstractmethod 
    def imputeSales(self)->DataFrame:pass 

    def removeCustomerWithoutGeolocationData(self)->DataFrame:pass 

