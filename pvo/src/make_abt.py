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
    _summary_

    _extended_summary_

    :param Source: _description_
    :type Source: _type_
    """
    def __init__(self, config):
        self.cooleDf:DataFrame = None
        self.this_config = config 

    def load_data(self)->None:
        dfs = []
        for companyCode in self.this_config['company_code']:
            dfs.append(spark.read.option("header", "true").parquet(self.this_config['cooler_file_path'].format(CAPS_CC = self.this_config['country_code'].upper(), companyCode = companyCode )))
        self.coolerDf = reduce(DataFrame.unionAll, dfs)

    def filter_data(self)->None:
        self.coolerDf = self.coolerDf.selectExpr([colExpr for colExpr in self.this_config['selected_coolers_column_list']])\
                                    .filter(self.this_config['filtering_cooler_conditions'])
        # --- Remove duplicates (if exist) ---
        self.coolerDf = self.coolerDf.select(*self.this_config['output_selected_column_list']).distinct()
        
    def impute_nans(self)->None:
        self.coolerDf = self.coolerDf.fillna(0, ['COOLER_DOORS_SUM', 'COOLER_EQUIP_COUNT'])
    
    def feature_engineering(self)->None:
        # --- Create features ---
        self.coolerDf = self.coolerDf.groupBy('CUSTOMER').agg(f.sum('ACTUAL_DOORS').alias('COOLER_DOORS_SUM'), f.countDistinct('EQUIPMENT').alias('COOLER_EQUIP_COUNT'))
        
    def assemble(self)->DataFrame:
        self.load_data()
        self.filter_data()
        self.feature_engineering()
        self.impute_nans()

        return self.coolerDf
 