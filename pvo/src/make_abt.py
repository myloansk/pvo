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

class Demographics(Source):
    """
    Concrete class in which all abstract stages of Source are implemented 
    with respect to demographic data
    Please bear in mind that is the demographics may vary across country

    :param Source:  parent class used as an interface in which
                    a template method that contains a skeleton of some algorithm 
                    composed of calls, usually to abstract primitive operations. 
    :type Source: class
    """
    def __init__(self,configParsed)->None:
        self.demographicsDf = None 
        self.this_config = configParsed 

    def load_data(self) -> DataFrame:
        """
        Loads cooler data using config Dict for getting the filepath
        """
        self.demographicsDf =  spark.read.option("header", "true").option("sep", ",").csv(self.this_config['demographics_file_path'].format(cc = self.this_config['country_code']))
        
    def filter_data(self) -> DataFrame:
        pass

    def feature_engineering(self) -> DataFrame:
        """
        Create features associated with demographics and corresponds
        to specific BU that will take part in estimating potential of each outlet 
        """
        # Convert age groups to percentages
        #percentCols = ['age_t0014_sum', 'age_t1529_sum', 'age_t3044_sum', 'age_t4559_sum', 'age_t60pl_sum']
        for Col in  self.this_config['age_group_column_list']: self.demographicsDf = self.demographicsDf.withColumn('Percentage_'+Col, f.coalesce(f.round(f.col(Col)/(f.col('pop_sum')), 2), f.lit(0)))  

        # Add per capita in spending
        self.demographicsDf = self.demographicsDf.withColumn('wvce_01_pc',f.expr(self.this_config['wvce_01_pc']))
        self.demographicsDf = self.demographicsDf.withColumn('wvce_02_pc',f.expr(self.this_config['wvce_02_pc']))
        #self.demographicsDf = self.demographicsDf.withColumn('wvce_01_pc',f.col('wvce_01_sum')/f.col('pop_sum'))
        #self.demographicsDf = self.demographicsDf.withColumn('wvce_02_pc',f.col('wvce_02_sum')/f.col('pop_sum'))

        # Add seasonal traffic diff
        self.demographicsDf = self.demographicsDf.withColumn('TF_WINTER', f.expr(self.this_config['TF_WINTER']))
        self.demographicsDf = self.demographicsDf.withColumn('TF_SUMMER', f.expr(self.this_config['TF_SUMMER']))
        #self.demographicsDf = self.demographicsDf.withColumn('TF_WINTER', (f.col('monthly_traffic_2021_12_mean')+f.col('monthly_traffic_2021_1_mean')+f.col('monthly_traffic_2021_2_mean'))/3 )
        #self.demographicsDf = self.demographicsDf.withColumn('TF_SUMMER', (f.col('monthly_traffic_2021_6_mean')+f.col('monthly_traffic_2021_7_mean')+f.col('monthly_traffic_2021_8_mean'))/3 )
        self.demographicsDf = self.demographicsDf.withColumn('Season_traffic_perc_diff', f.expr(self.this_config['Season_traffic_perc_diff']))
        #self.demographicsDf = self.demographicsDf.withColumn('Season_traffic_perc_diff', f.coalesce(f.round((f.col('TF_SUMMER') - f.col('TF_WINTER'))/(f.col('TF_WINTER')),2), f.lit(0)))

        # Add weekend traffic diff
        self.demographicsDf = self.demographicsDf.withColumn('Weekend_traffic_perc_diff',f.expr(self.this_config['Weekend_traffic_perc_diff']))
        #self.demographicsDf = self.demographicsDf.withColumn('Weekend_traffic_perc_diff', 
        #                                           f.coalesce(f.round((f.col('monthly_traffic_weekend_avg_mean') - f.col('monthly_traffic_weekday_avg_mean'))/(f.col('monthly_traffic_weekday_avg_mean')),2), f.lit(0)))

        # Convert to time of day traffic to percentages
        #percentCols = ['monthly_traffic_morning_avg_mean', 'monthly_traffic_afternoon_avg_mean', 'monthly_traffic_evening_avg_mean', 'monthly_traffic_night_avg_mean']
        for Col in  self.this_config['traffic_hours']: self.demographicsDf = demographicsDf.withColumn('Percentage_'+Col, f.coalesce(f.round(f.col(Col)/(f.col('monthly_traffic_avg_mean')), 2), f.lit(0)))

        # Add density features
        self.demographicsDf = self.demographicsDf.withColumn('population_density', f.expr(self.this_config['population_density']))
        self.demographicsDf = self.demographicsDf.withColumn('competitor_count_density', f.expr(self.this_config['competitor_count_density']) )
        #self.demographicsDf = self.demographicsDf.withColumn('population_density', f.col('pop_sum')/(f.lit(3.14) * f.col('ta_size') * f.col('ta_size')))
        #self.demographicsDf = self.demographicsDf.withColumn('competitor_count_density', f.col('competitor_count')/(f.lit(3.14) * f.col('ta_size') * f.col('ta_size'))) 

    def impute_nans(self)->DataFrame:
        """
        Impute Nans on derived features using k-nn imputation strategy

        """
        customerDf = customerDf.fillna("UNKNOWN", subset = [x for x in customerDf.columns if "CUST_" in x])

        to_numeric = [col for col in  demographicsDf.columns if col not in ["TAA_TC","urbanicity","CUSTOMER_DESC","LONGITUDE","LATITUDE","_BIC_CTRADE_CH","_BIC_CDMD_AREA","ta_size","geometry"]]

        demographicsExcNanDf = demographicsDf.dropna(how='any')
        demographicsExcNanDf = demographicsExcNanDf.select([f.col(c).alias(c + "_full") for c in demographicsExcNanDf.columns])

        allNanPdf = demographicsDf.join(demographicsExcNanDf, demographicsDf["CUSTOMER"]==demographicsExcNanDf["CUSTOMER_full"], 'left_anti').toPandas()
        demographicsExcNanPDf= demographicsExcNanDf.toPandas()

        kd = KDTree(demographicsExcNanPDf[["LATITUDE_full", "LONGITUDE_full"]].values, metric='euclidean')

        allNanPdf['distances_euc'], allNanPdf['indices'] = kd.query(allNanPdf[["LATITUDE", "LONGITUDE"]], k = 1)
        allNanPdf['distances_km'] = allNanPdf['distances_euc'] * 104.867407

        demographicsExcNanPDf = demographicsExcNanPDf.reset_index().rename(columns = {"index": "indices"})
        allNanPdf = allNanPdf.merge(demographicsExcNanPDf, on ='indices', how = 'left')

        # Impute only if distance is less than 5km
        notImputePdf = allNanPdf[allNanPdf['distances_km']>=5]
        imputePdf = allNanPdf[allNanPdf['distances_km']<5]

        notImputePdf[to_numeric] = notImputePdf[to_numeric].apply(pd.to_numeric, errors='coerce')
        imputePdf[to_numeric] = imputePdf[to_numeric].apply(pd.to_numeric, errors='coerce')
        # Transform pandas DF to pyspark DF
        notImputeDf = spark.createDataFrame(notImputePdf).drop("distances_euc", "distances_km", "indices")
        imputeDf = spark.createDataFrame(imputePdf.replace(float('nan'), None)).drop("distances_euc", "distances_km", "indices")    

        # Impute columns
        wArea = Window().partitionBy("_BIC_CDMD_AREA")
        cols_to_impute = [x for x in imputeDf.columns if "full" not in x]
        for col in cols_to_impute:
            imputeDf = imputeDf.withColumn(col, f.coalesce(f.col(col), f.col(col + "_full"),f.avg(f.col(col)).over(wArea))
            )

        # Drop/rename helper columns
        imputeDf = imputeDf.drop(*[x for x in imputeDf.columns if "full" in x])

        notImputeDf = notImputeDf.drop(*[x for x in notImputeDf.columns if "full" in x])
        demographicsExcNanDf = demographicsExcNanDf.select([f.col(c).alias(c.replace('_full', '')) for c in demographicsExcNanDf.columns])

        # Join Imputed and Full data
        demographicsImputedDf = imputeDf.unionByName(demographicsExcNanDf).unionByName(notImputeDf)
        demographicsImputedDf = demographicsImputedDf.drop("LATITUDE", "LONGITUDE")

    def assemble(self):
        self.load_data()
        self.filter_data()
        self.feature_engineering()
        self.impute_nans()
        return self.demographicsDf 

class Customer(Source):

    def __init__(self, config):
        """
        Concrete class in which all abstract stages of Source are implemented 
        with respect to customer master data

        :param Source:  parent class used as an interface in which
                        a template method that contains a skeleton of some algorithm 
                        composed of calls, usually to abstract primitive operations. 
        :type Source: class
        """
        self.customerDf:DataFrame = None
        self.this_config = config
    
    def load_data(self)->None:
        """
        Load data to pyspark.DataFrame from datalake
        """
        self.customerDf = spark.read.option("header", "true").option("sep", "|").csv(self.this_config['customer_md_file_path'])

    def filter_data(self)->None:
        """
        Apply standard filters as it deemed appropriate by business stakeholders
        """
        self.customerDf = self.customerDf.filter(self.this_config['filtering_conditions'])

    def feature_engineering(self)->None:
        """
        Create features associated with customer data including labelcol 
        """
        # Rename columns
        self.customerDf = self.customerDf.select( *[ f.col(colName) for colName in self.customerDf.columns if colName not in self.this_config['rename_columns_dict']] + [f.col(key).alias(value) for key, value in self.this_config['rename_columns_dict'].items() ])
    
    
        # Create additional columns
        self.customerDf = (self.customerDf.withColumn("CUST_ROUTED", f.expr(self.this_config['CUST_ROUTED']))
                                        .withColumn('second_digit_ccaf', f.expr(self.this_config['second_digit_ccaf']))
                                        .withColumn('CUST_CCAF_GROUP', f.expr(self.this_config['CUST_CCAF_GROUP']))
                    )
    
        # Convert long/lat to double
        self.customerDf = self.customerDf.withColumn('LONGITUDE', f.expr(self.this_config['LONGITUDE'])).withColumn('LATITUDE', f.expr(self.this_config['LATITUDE'] ))
    
        #Keel only informative columns
        self.customerDf = self.customerDf.select(*self.this_config['customer_selected_columns'])

    def impute_nans(self)->None:pass 

    def assemble(self)->DataFrame:
        self.load_data()
        self.filter_data()
        self.feature_engineering()
        self.impute_nans()
        return self.customerDf
