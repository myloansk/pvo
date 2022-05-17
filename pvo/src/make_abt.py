from skeleton import Source 

from abc import ABC, abstractmethod
import pandas as pd
import pyspark.sql.functions as f
import pyspark.sql.types as t
import os
import yaml

from hydra import initialize_config_module,initialize,compose,initialize_config_dir
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pyspark.sql.window import Window
from functools import reduce
from pathlib import Path
from pyspark.sql import DataFrame
from sklearn.neighbors import BallTree, KDTree
from omegaconf import DictConfig

spark.sql("set spark.sql.execution.arrow.pyspark.fallback.enabled=false")

class Cooler(Source):
    """
    Concrete class in which all abstract stages of Source are implemented 
    with respect to coolers data

    :param Source:  parent class used as an interface in which
                    a template method that contains a skeleton of some algorithm 
                    composed of calls, usually to abstract primitive operations. 
    :type Source: class
    """
    def __init__(self, config):
        self.cooleDf:DataFrame = None
        self.this_config = config 

    def load_data(self)->None:
        """
        Loads cooler data using config Dict for getting the filepath
        """
        dfs = []
        for companyCode in self.this_config['company_code']:
            dfs.append(spark.read.option("header", "true").parquet(self.this_config['cooler_file_path'].format(CAPS_CC = self.this_config['country_code'].upper(), companyCode = companyCode )))
        self.coolerDf = reduce(DataFrame.unionAll, dfs)

    def filter_data(self)->None:
        """
        Filter cooler data using knowledge transferred by stakeholder/business units

        """
        self.coolerDf = self.coolerDf.selectExpr([colExpr for colExpr in self.this_config['selected_coolers_column_list']])\
                                    .filter(self.this_config['filtering_cooler_conditions'])
        # --- Remove duplicates (if exist) ---
        self.coolerDf = self.coolerDf.select(*self.this_config['output_selected_column_list']).distinct()
        
    def impute_nans(self)->None:
        """
        Impute Nans on the derived columns using 0 as agreed with business stakeholders

        """
        self.coolerDf = self.coolerDf.fillna(0, ['COOLER_DOORS_SUM', 'COOLER_EQUIP_COUNT'])
    
    def feature_engineering(self)->None:
        """
        Create features associated with coolers data 
        that will take part in estimating potential of each outlet 
        """
        # --- Create features ---
        self.coolerDf = self.coolerDf.groupBy('CUSTOMER').agg(f.sum('ACTUAL_DOORS').alias('COOLER_DOORS_SUM'), f.countDistinct('EQUIPMENT').alias('COOLER_EQUIP_COUNT'))
        
    def assemble(self)->DataFrame:
        self.load_data()
        self.filter_data()
        self.feature_engineering()
        self.impute_nans()

        return self.coolerDf
 