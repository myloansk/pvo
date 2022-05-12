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
    _summary_

    _extended_summary_

    :param ABC: _description_
    :type ABC: _type_
    """
    @abstractmethod 
    def load_data(self)->None:pass 

    @abstractmethod 
    def filter_data(self)->None:pass 

    @abstractmethod 
    def feature_engineering(self)->None:pass 

    @abstractmethod 
    def impute_nans(self)->None:pass 

    @abstractmethod 
    def assemble(self)->DataFrame:pass



