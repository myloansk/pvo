# Import Python Pkgs
import imp
import pandas as pd
import pyspark.sql.functions as f
import pyspark.sql.types as t
import os
import yaml

from abc import ABC, abstractmethod
from hydra import (initialize_config_module,
                    initialize,compose,initialize_config_dir)
from datetime import datetime
from dateutil.relativedelta import relativedelta
from functools import reduce
from sklearn.neighbors import BallTree, KDTree
from pathlib import Path
from pyspark.sql.window import Window
from pyspark.sql import DataFrame
from typing import List 
from omegaconf import DictConfig 

# Import other project dependencies
from skeleton import Source 
from utils.datalake_utils import get_latest_modified_file_from_directory
from utils.pvo_impute_missing_data import imputeWithKnn, imputeWithAveragesPerCtradeAndDemandArea,imputeWithZero
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
    def __init__(self) -> None:
        super().__init__()

    def load_data(self)->None:
        """
        Loads cooler data using config Dict for getting the filepath
        """
        dfs = []
        for companyCode in self._this_config['company_code']:
            dfs.append(spark.read.option("header", "true").parquet(self._this_config['cooler_file_path'].format(CAPS_CC = self._this_config['country_code'].upper(), companyCode = companyCode )))
        self._sparkDf = reduce(DataFrame.unionAll, dfs)

    def filter_data(self)->None:
        periodLatest = self._sparkDf.agg({"FISCPER": "max"}).collect()[0][0]
        print(periodLatest)

        self._sparkDf = self._sparkDf.selectExpr([colExpr for colExpr in self._this_config['selected_coolers_column_list']])\
                                    .filter(self._this_config['filtering_cooler_conditions'].format( periodLatest = periodLatest))
        # --- Remove duplicates (if exist) ---
        self._sparkDf = self._sparkDf.select(*self._this_config['output_selected_column_list']).distinct()
        
    
    def feature_engineering(self)->None:
        """
        Create features associated with coolers data 
        that will take part in estimating potential of each outlet 
        """
        # --- Create features ---
        self._sparkDf = self._sparkDf.groupBy('CUSTOMER').agg(f.sum('ACTUAL_DOORS').alias('COOLER_DOORS_SUM'), f.countDistinct('EQUIPMENT').alias('COOLER_EQUIP_COUNT'))
        

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
    def __init__(self) -> None:
        super().__init__()

    def load_data(self) -> DataFrame:
        """
        Loads cooler data using config Dict for getting the filepath
        """
        self._sparkDf =  spark.read.option("header", "true").option("sep", ",").csv(self._this_config['demographics_file_path'].format(cc = self._this_config['country_code']))
        
    def filter_data(self) -> DataFrame:
        pass

    def feature_engineering(self) -> DataFrame:
        """
        Create features associated with demographics and corresponds
        to specific BU that will take part in estimating potential of each outlet 
        """
        # Convert age groups to percentages

        for Col in  self._this_config['age_group_column_list']: self._sparkDf = self._sparkDf.withColumn('Percentage_'+Col, f.coalesce(f.round(f.col(Col)/(f.col('pop_sum')), 2), f.lit(0)))  

        # Add per capita in spending
        self._sparkDf = self._sparkDf.withColumn('wvce_01_pc',f.expr(self._this_config['wvce_01_pc']))
        self._sparkDf = self._sparkDf.withColumn('wvce_02_pc',f.expr(self._this_config['wvce_02_pc']))
        

        # Add seasonal traffic diff
        self._sparkDf = self._sparkDf.withColumn('TF_WINTER', f.expr(self._this_config['TF_WINTER']))
        self._sparkDf = self._sparkDf.withColumn('TF_SUMMER', f.expr(self._this_config['TF_SUMMER']))
        
        self._sparkDf = self._sparkDf.withColumn('Season_traffic_perc_diff', f.expr(self._this_config['Season_traffic_perc_diff']))

        # Add weekend traffic diff
        self._sparkDf = self._sparkDf.withColumn('Weekend_traffic_perc_diff',f.expr(self._this_config['Weekend_traffic_perc_diff']))
        
        # Convert to time of day traffic to percentages
        for Col in  self._this_config['traffic_hours']: self._sparkDf = _sparkDf.withColumn('Percentage_'+Col, f.coalesce(f.round(f.col(Col)/(f.col('monthly_traffic_avg_mean')), 2), f.lit(0)))

        # Add density features
        self._sparkDf = self._sparkDf.withColumn('population_density', f.expr(self._this_config['population_density']))
        self._sparkDf = self._sparkDf.withColumn('competitor_count_density', f.expr(self._this_config['competitor_count_density']) ) 

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
        def __init__(self) -> None:
            super().__init__()
    
    def load_data(self)->None:
        """
        Load data to pyspark.DataFrame from datalake
        """
        self._sparkDf = spark.read.option("header", "true").option("sep", "|").csv(self._this_config['customer_md_file_path'])

    def filter_data(self)->None:
        """
        Apply standard filters as it deemed appropriate by business stakeholders
        """
        self._sparkDf = self._sparkDf.filter(self._this_config['filtering_conditions'])

    def feature_engineering(self)->None:
        """
        Create features associated with customer data including labelcol 
        """
        # Rename columns
        self._sparkDf = self._sparkDf.select( *[ f.col(colName) for colName in self._sparkDf.columns if colName not in self._this_config['rename_columns_dict']] + [f.col(key).alias(value) for key, value in self._this_config['rename_columns_dict'].items() ])
    
    
        # Create additional columns
        self._sparkDf = (self._sparkDf.withColumn("CUST_ROUTED", f.expr(self._this_config['CUST_ROUTED']))
                                        .withColumn('second_digit_ccaf', f.expr(self._this_config['second_digit_ccaf']))
                                        .withColumn('CUST_CCAF_GROUP', f.expr(self._this_config['CUST_CCAF_GROUP']))
                    )
    
        # Convert long/lat to double
        self._sparkDf = self._sparkDf.withColumn('LONGITUDE', f.expr(self._this_config['LONGITUDE'])).withColumn('LATITUDE', f.expr(self._this_config['LATITUDE'] ))
    
        #Keel only informative columns
        self._sparkDf = self._sparkDf.select(*self._this_config['customer_selected_columns'])


class Sales(Source):
    """
    Concrete class in which all abstract stages of Source are implemented 
    with respect to sales data

    :param Source:  parent class used as an interface in which
                    a template method that contains a skeleton of some algorithm 
                    composed of calls, usually to abstract primitive operations. 
    :type Source: class
    """

    def __init__(self) -> None:
        super().__init__()
        
    def load_data(self)->None:
        """
        Load data to pyspark.DataFrame by iteratively adding a pyspark.DataFrame with
        sales data corresponding to specific sales org to a list. 
        In the end list is reduced to a single pyspark.DataFrame
        """
        dfs = []
        for salesOrg in self._this_config['sales_org']:
            dfs.append(spark.read.parquet(self._this_config['sales_file_path'].format(CAPS_CC = self._this_config['country_code'].upper(), salesOrg = salesOrg) ,header='true'))

        self._sparkDf = reduce(DataFrame.unionAll, dfs)
    
    def filter_data(self)->None:
        """
        Applying standard filters and minor trannsformation including column renaming and enrichment
        """
        
        self._sparkDf = self._sparkDf.filter(''.join(self._this_config['filter_data_filtering_criteria_1']))

        # Correct data types & rename
        self._sparkDf = (self._sparkDf.withColumn('MATERIAL', f.expr(self._this_config['filter_data_MATERIAL_create']))
                                    .withColumn('Direct_Sales_Volume_in_UC', f.expr(self._this_config['filter_data_Direct_Sales_Volume_in_UC_create']))
                                    .withColumn('Indirect_Sales_NSR', f.expr(self._this_config['filter_data_Indirect_Sales_NSR_create']))
                                    .withColumn('Indirect_Sales_Volume_in_UC',  f.expr(self._this_config['filter_data_Indirect_Sales_Volume_in_UC_create']))
                                    .withColumn('Direct_Sales_NSR', f.expr(self._this_config['filter_data_Direct_Sales_NSR_create']))
                                    .withColumnRenamed('FIELDNM005', 'CURRENCY')
                                    .drop("FISCPER")
            )
    
        # Calendar
        calendarDf = spark.read.option("header", "true").csv(self._this_config['calendar_file_path'])
        # Add calendar
        self._sparkDf = self._sparkDf.join(calendarDf.select(*self._this_config['filter_data_calendar_selected_columns']).distinct(), on='CALDAY', how='left') 

        periodLatest, numOfDays = self._sparkDf.select(f.col("FISCPER"),
                                                f.datediff(
                                                    f.to_date(f.max(f.col("CALDAY")).over(Window().partitionBy(f.col("FISCPER"))), 'yyyyMMdd'),
                                                    f.trunc( f.to_date(f.max(f.col("CALDAY")).over(Window().partitionBy(f.col("FISCPER"))), 'yyyyMMdd'), "month")).alias('num_of_days_passed_from_start_of_month'))\
                                        .filter(
                                                (f.col("num_of_days_passed_from_start_of_month").cast("integer") >20) | 
                                                (f.col("num_of_days_passed_from_start_of_month")==0)).distinct().sort(f.col("FISCPER").desc()).limit(1).toPandas().values.flatten().tolist()


        periodStart = (datetime.strptime(periodLatest[0:4] + '-' + periodLatest[6:7] + '-01', '%Y-%m-%d').date() - relativedelta(months=12)).strftime("%Y0%m")
        # Filter for relevant periods
        self._sparkDf = self._sparkDf.filter(self._this_config['filter_data_filtering_criteria_2'].format( periodStart = periodStart, periodLatest = periodLatest ))


    def feature_engineering(self)->None:
        # Finalize sales table
        """
        Create features corresponding to sales data that would participate in estimating the CCAF lable
        """
        #TODO Fix error coming from creating Sales_NSR nad Sales_Volumw_in_UC using the implementation below
        #     error is Can't extract value from CheckOverflow((promote_precision(cast(coalesce(Direct_Sales_Volume_in_UC#4342, cast(0 as decimal(38,3))) as decimal(38,3))) 
        #              + promote_precision(cast(coalesce(Indirect_Sales_Volume_in_UC#4383, cast(0 as decimal(38,3))) as decimal(38,3)))), DecimalType(38,3), true): 
        #               need struct type but got decimal(38,3)
        #self._sparkDf = self._sparkDf.withColumn('Sales_Volume_in_UC',f.expr(self._this_config['feature_engineering_Sales_Volume_in_UC_create']))\
        #                        .withColumn('Sales_NSR',f.expr(self._this_config['feature_engineering_Sales_NSR_create']))\
        #                        .select(self._this_config['feature_engineering_selected_columns'], ['Sales_Volume_in_UC','Sales_NSR'])
        self._sparkDf = (self._sparkDf.select('CUSTOMER', 'CALDAY', 'FISCPER', 'MATERIAL',
                                        f.coalesce(f.col('Direct_Sales_Volume_in_UC'), f.lit(0)) + f.coalesce(f.col('Indirect_Sales_Volume_in_UC'), f.lit(0))).alias('Sales_Volume_in_UC'),
                                        (f.coalesce(f.col('Direct_Sales_NSR'), f.lit(0)) + f.coalesce(f.col('Indirect_Sales_NSR'), f.lit(0))).alias('Sales_NSR'),
                                        'CURRENCY'
                                        )

        # Impute currency
        unique_currency_df = (self._sparkDf.filter(self._this_config['impute_nans_filter_1'])
                                        .groupBy('CUSTOMER').agg(f.countDistinct('CURRENCY').alias('currency_options'), f.first('CURRENCY').alias('UNIQUE_CURRENCY'))
                                        .filter(self._this_config['impute_nans_filter_2'])
                                        .drop(self._this_config['impute_nans_drop_columns_list'])
                            )

        self._sparkDf = (self._sparkDf.join(unique_currency_df, on=self._this_config['impute_nans_join_operation']['join_on'], how=self._this_config['impute_nans_join_operation']['how'])
                                    .withColumn('CURRENCY',f.expr(self._this_config['impute_nans_CURRENCY_create']))
                                    .fillna('NA', subset=['CURRENCY'])
                                    .drop(self._this_config['impute_nans_drop_column_list_v2'])
                    )

        self._sparkDf = self._sparkDf.withColumn("Sales_Volume_in_UC_monthly_sum", f.sum(f.col("Sales_Volume_in_UC")).over(Window().partitionBy("CUSTOMER","FISCPER")))\
                                .withColumn("Sales_NSR_monthly_sum",f.sum(f.col("Sales_NSR")).over(Window().partitionBy("CUSTOMER","FISCPER")))

        self._sparkDf = self._sparkDf.withColumn("Sales_Volume_in_UC_rolling_xmonths_back_avg", f.avg(f.col("Sales_Volume_in_UC_monthly_sum")).over(Window().partitionBy("CUSTOMER")))\
                                .withColumn("Sales_NSR_rolling_xmonths_back_avg", f.avg(f.col("Sales_NSR_monthly_sum")).over(Window().partitionBy("CUSTOMER")))


        self._sparkDf = self._sparkDf.select(f.col("CUSTOMER"), f.col("Sales_Volume_in_UC_rolling_xmonths_back_avg") ,f.col("Sales_NSR_rolling_xmonths_back_avg")).distinct()


def make_analytical_base_table(customerDf:DataFrame, 
                            coolersDf: DataFrame, salesDf:DataFrame,
                            demographicsDf:DataFrame,INCLUDE_COLS_LIST:List[str], output_file_path:str, cc = COUNTRY_CODE):
    demographicsDf = demographicsDf.drop("LONGITUDE","LATITUDE")
    
    abtDf = customerDf.join(demographicsDf, on='CUSTOMER', how='left')\
                    .join(coolersDf,  on='CUSTOMER', how='left')\
                    .join(salesDf, on='CUSTOMER', how='left')

    customerDf = customerDf.fillna("UNKNOWN", subset = [x for x in customerDf.columns if "CUST_" in x])

    to_numeric = [col for col in  demographicsDf.columns if col not in ["TAA_TC","urbanicity","CUSTOMER_DESC","LONGITUDE","LATITUDE","_BIC_CTRADE_CH","_BIC_CDMD_AREA","ta_size","geometry"]]
    demographicsOfAllCustDf = abtDf.select([colName for colName in abtDf.columns if colName in to_numeric + ["LONGITUDE","LATITUDE",'_BIC_CDMD_AREA'] ])


    demographicsExcNanDf = demographicsOfAllCustDf.dropna(how='any')
    demographicsExcNanDf = demographicsExcNanDf.select([f.col(c).alias(c + "_full") for c in demographicsExcNanDf.columns])

    allNanPdf = demographicsOfAllCustDf.join(demographicsExcNanDf, demographicsOfAllCustDf["CUSTOMER"]==demographicsExcNanDf["CUSTOMER_full"], 'left_anti').toPandas()
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
        imputeDf = imputeDf.withColumn(col, f.coalesce(f.col(col), f.col(col + "_full"),f.avg(f.col(col)).over(wArea)))

    # Drop/rename helper columns
    imputeDf = imputeDf.drop(*[x for x in imputeDf.columns if "full" in x])

    notImputeDf = notImputeDf.drop(*[x for x in notImputeDf.columns if "full" in x])
    demographicsExcNanDf = demographicsExcNanDf.select([f.col(c).alias(c.replace('_full', '')) for c in demographicsExcNanDf.columns])

    # Join Imputed and Full data
    demographicsImputedDf = imputeDf.unionByName(demographicsExcNanDf).unionByName(notImputeDf)
    demographicsImputedDf = demographicsImputedDf.drop("LATITUDE", "LONGITUDE")

    #Rename columns(Adding prefix imputed)
    demographicsImputedDf = demographicsImputedDf.select(*[f.col(colName).alias("imputed_" + colName)  if colName != 'CUSTOMER' else f.col(colName) for colName in demographicsImputedDf.columns ])

    #Keep only imputed columns from demographics dataset
    leaveColLst = ['CUSTOMER', 'TAA_TC', 'urbanicity', 'CUSTOMER_DESC', 'LONGITUDE', 'LATITUDE', '_BIC_CTRADE_CH', '_BIC_CDMD_AREA', 'ta_size','geometry']

    abtDf = abtDf.join(demographicsImputedDf, on='CUSTOMER', how='inner')
    abtDf = abtDf.drop(*[colName for colName in demographicsDf.columns if colName not in leaveColLst])

    # Remove prefix `imputed`
    abtDf = abtDf.drop(*['imputed_TAA_TC','imputed_urbanicity','imputed_CUSTOMER_DESC','imputed__BIC_CTRADE_CH','imputed__BIC_CDMD_AREA','imputed_ta_size','imputed_geometry'])
    abtDf = abtDf.select(*[f.col(colName).alias(colName.replace('imputed_', '')) if 'imputed' in colName else f.col(colName) for colName in abtDf.columns])


    abtDf = abtDf.withColumn("Sales_Volume_in_UC_imputed",f.lit(None))
    abtDf = abtDf.withColumn("Sales_Volume_in_UC_imputed",
                        f.coalesce(
                                    f.col("Sales_Volume_in_UC_rolling_xmonths_back_avg").cast("Double"),
                                    f.avg(f.col("Sales_Volume_in_UC_rolling_xmonths_back_avg").cast("Double")).over(Window.partitionBy("CUST_CTRADE_CH_DESC","CUST_CDMD_AREA")),
                                    f.avg(f.col("Sales_Volume_in_UC_rolling_xmonths_back_avg").cast("Double")).over( Window.partitionBy("CUST_CTRADE_CH_DESC")),
                                    f.avg(f.col("Sales_Volume_in_UC_rolling_xmonths_back_avg").cast("Double")).over(Window.partitionBy())
                        ))
    spark.sql("DROP TABLE harry.tempqq_abt")
    abtDf.write.mode("overwrite").saveAsTable("harry.temp_abt")
    abtDf = spark.sql("select * from harry.temp_abt")

    abtDf = abtDf.withColumn("Sales_NSR_imputed",f.lit(None))
    abtDf = abtDf.withColumn("Sales_NSR_imputed",
                        f.coalesce(
                                    f.col("Sales_NSR_rolling_xmonths_back_avg").cast("Double"),
                                    f.avg(f.col("Sales_NSR_rolling_xmonths_back_avg").cast("Double")).over( Window.partitionBy("CUST_CTRADE_CH_DESC","CUST_CDMD_AREA")),
                                    f.avg(f.col("Sales_NSR_rolling_xmonths_back_avg").cast("Double")).over( Window.partitionBy("CUST_CTRADE_CH_DESC")),
                                    f.avg(f.col("Sales_NSR_rolling_xmonths_back_avg").cast("Double")).over(Window.partitionBy())
                        ))
    abtDf = abtDf.fillna(0, subset=['Sales_NSR_imputed', 'Sales_Volume_in_UC_imputed']) 
    abtDf = abtDf.select(*INCLUDE_COLS_LIST) 

    #Get BU latest processed file 
    latestFileName = get_latest_modified_file_from_directory(COUNTRY_INPUT_DATA_DIRECTORY).rsplit("_",1).pop(0)
    print(f"This is the filepath:{latestFileName}")
    # Load BU data to a pyspark.DataFrame
    previousAbtDf =  spark.read.option("header", "true").option("sep", ",").csv(latestFileName)

    previousAbtDf = previousAbtDf.withColumn('pardt', f.lit('20220512'))
    newAbtDf = abtDf.join(previousAbtDf, on='CUSTOMER', how = 'left_anti')


    pardt=datetime.now().strftime("%Y%m%d")
    newAbtDf = newAbtDf.withColumn('pardt', f.lit(pardt))
    allAbtDf = previousAbtDf.union(newAbtDf)

    partition_date=datetime.now().strftime("%Y%m%d_%H%M%S")
    
    allAbtDf.write.csv(output_file_path.format(cc = cc, partition_date = partition_date), header=True)