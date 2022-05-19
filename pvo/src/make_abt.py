# Import other project dependencies
from skeleton import Source 

# Import Python Pkgs
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

        for Col in  self.this_config['age_group_column_list']: self.demographicsDf = self.demographicsDf.withColumn('Percentage_'+Col, f.coalesce(f.round(f.col(Col)/(f.col('pop_sum')), 2), f.lit(0)))  

        # Add per capita in spending
        self.demographicsDf = self.demographicsDf.withColumn('wvce_01_pc',f.expr(self.this_config['wvce_01_pc']))
        self.demographicsDf = self.demographicsDf.withColumn('wvce_02_pc',f.expr(self.this_config['wvce_02_pc']))
        

        # Add seasonal traffic diff
        self.demographicsDf = self.demographicsDf.withColumn('TF_WINTER', f.expr(self.this_config['TF_WINTER']))
        self.demographicsDf = self.demographicsDf.withColumn('TF_SUMMER', f.expr(self.this_config['TF_SUMMER']))
        
        self.demographicsDf = self.demographicsDf.withColumn('Season_traffic_perc_diff', f.expr(self.this_config['Season_traffic_perc_diff']))

        # Add weekend traffic diff
        self.demographicsDf = self.demographicsDf.withColumn('Weekend_traffic_perc_diff',f.expr(self.this_config['Weekend_traffic_perc_diff']))
        
        # Convert to time of day traffic to percentages
        for Col in  self.this_config['traffic_hours']: self.demographicsDf = demographicsDf.withColumn('Percentage_'+Col, f.coalesce(f.round(f.col(Col)/(f.col('monthly_traffic_avg_mean')), 2), f.lit(0)))

        # Add density features
        self.demographicsDf = self.demographicsDf.withColumn('population_density', f.expr(self.this_config['population_density']))
        self.demographicsDf = self.demographicsDf.withColumn('competitor_count_density', f.expr(self.this_config['competitor_count_density']) )
        
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

class Sales(Source):
    """
    Concrete class in which all abstract stages of Source are implemented 
    with respect to sales data

    :param Source:  parent class used as an interface in which
                    a template method that contains a skeleton of some algorithm 
                    composed of calls, usually to abstract primitive operations. 
    :type Source: class
    """

    def __init__(self, parsedConfig) -> None:
        self.salesDf:DataFrame = None 
        self.this_config = parsedConfig 

    def load_data(self)->None:
        """
        Load data to pyspark.DataFrame by iteratively adding a pyspark.DataFrame with
        sales data corresponding to specific sales org to a list. 
        In the end list is reduced to a single pyspark.DataFrame
        """
        dfs = []
        for salesOrg in self.this_config['sales_org']:
            dfs.append(spark.read.parquet(self.this_config['sales_file_path'].format(CAPS_CC = self.this_config['country_code'].upper(), salesOrg = salesOrg) ,header='true'))

        self.salesDf = reduce(DataFrame.unionAll, dfs)
    
    def filter_data(self)->None:
        """
        Applying standard filters and minor trannsformation including column renaming and enrichment
        """
        
        self.salesDf = self.salesDf.filter(''.join(self.this_config['filter_data_filtering_criteria_1']))

        # Correct data types & rename
        self.salesDf = (self.salesDf.withColumn('MATERIAL', f.expr(self.this_config['filter_data_MATERIAL_create']))
                                    .withColumn('Direct_Sales_Volume_in_UC', f.expr(self.this_config['filter_data_Direct_Sales_Volume_in_UC_create']))
                                    .withColumn('Indirect_Sales_NSR', f.expr(self.this_config['filter_data_Indirect_Sales_NSR_create']))
                                    .withColumn('Indirect_Sales_Volume_in_UC',  f.expr(self.this_config['filter_data_Indirect_Sales_Volume_in_UC_create']))
                                    .withColumn('Direct_Sales_NSR', f.expr(self.this_config['filter_data_Direct_Sales_NSR_create']))
                                    .withColumnRenamed('FIELDNM005', 'CURRENCY')
                                    .drop("FISCPER")
            )
    
        # Calendar
        calendarDf = spark.read.option("header", "true").csv(self.this_config['calendar_file_path'])
        # Add calendar
        self.salesDf = self.salesDf.join(calendarDf.select(*self.this_config['filter_data_calendar_selected_columns']).distinct(), on='CALDAY', how='left') 

        periodLatest, numOfDays = self.salesDf.select(f.col("FISCPER"),
                                                f.datediff(
                                                    f.to_date(f.max(f.col("CALDAY")).over(Window().partitionBy(f.col("FISCPER"))), 'yyyyMMdd'),
                                                    f.trunc( f.to_date(f.max(f.col("CALDAY")).over(Window().partitionBy(f.col("FISCPER"))), 'yyyyMMdd'), "month")).alias('num_of_days_passed_from_start_of_month'))\
                                        .filter(
                                                (f.col("num_of_days_passed_from_start_of_month").cast("integer") >20) | 
                                                (f.col("num_of_days_passed_from_start_of_month")==0)).distinct().sort(f.col("FISCPER").desc()).limit(1).toPandas().values.flatten().tolist()


        periodStart = (datetime.strptime(periodLatest[0:4] + '-' + periodLatest[6:7] + '-01', '%Y-%m-%d').date() - relativedelta(months=12)).strftime("%Y0%m")
        # Filter for relevant periods
        self.salesDf = self.salesDf.filter(self.this_config['filter_data_filtering_criteria_2'].format( periodStart = periodStart, periodLatest = periodLatest ))


    def feature_engineering(self)->None:
        # Finalize sales table
        """
        Create features corresponding to sales data that would participate in estimating the CCAF lable
        """
        #TODO Fix error coming from creating Sales_NSR nad Sales_Volumw_in_UC using the implementation below
        #     error is Can't extract value from CheckOverflow((promote_precision(cast(coalesce(Direct_Sales_Volume_in_UC#4342, cast(0 as decimal(38,3))) as decimal(38,3))) 
        #              + promote_precision(cast(coalesce(Indirect_Sales_Volume_in_UC#4383, cast(0 as decimal(38,3))) as decimal(38,3)))), DecimalType(38,3), true): 
        #               need struct type but got decimal(38,3)
        #self.salesDf = self.salesDf.withColumn('Sales_Volume_in_UC',f.expr(self.this_config['feature_engineering_Sales_Volume_in_UC_create']))\
        #                        .withColumn('Sales_NSR',f.expr(self.this_config['feature_engineering_Sales_NSR_create']))\
        #                        .select(self.this_config['feature_engineering_selected_columns'], ['Sales_Volume_in_UC','Sales_NSR'])
        self.salesDf = (self.salesDf.select('CUSTOMER', 'CALDAY', 'FISCPER', 'MATERIAL',
                                        f.coalesce(f.col('Direct_Sales_Volume_in_UC'), f.lit(0)) + f.coalesce(f.col('Indirect_Sales_Volume_in_UC'), f.lit(0))).alias('Sales_Volume_in_UC'),
                                        (f.coalesce(f.col('Direct_Sales_NSR'), f.lit(0)) + f.coalesce(f.col('Indirect_Sales_NSR'), f.lit(0))).alias('Sales_NSR'),
                                        'CURRENCY'
                                        )

        # Impute currency
        unique_currency_df = (self.salesDf.filter(self.this_config['impute_nans_filter_1'])
                                        .groupBy('CUSTOMER').agg(f.countDistinct('CURRENCY').alias('currency_options'), f.first('CURRENCY').alias('UNIQUE_CURRENCY'))
                                        .filter(self.this_config['impute_nans_filter_2'])
                                        .drop(self.this_config['impute_nans_drop_columns_list'])
                            )

        self.salesDf = (self.salesDf.join(unique_currency_df, on=self.this_config['impute_nans_join_operation']['join_on'], how=self.this_config['impute_nans_join_operation']['how'])
                                    .withColumn('CURRENCY',f.expr(self.this_config['impute_nans_CURRENCY_create']))
                                    .fillna('NA', subset=['CURRENCY'])
                                    .drop(self.this_config['impute_nans_drop_column_list_v2'])
                    )

        self.salesDf = self.salesDf.withColumn("Sales_Volume_in_UC_monthly_sum", f.sum(f.col("Sales_Volume_in_UC")).over(Window().partitionBy("CUSTOMER","FISCPER")))\
                                .withColumn("Sales_NSR_monthly_sum",f.sum(f.col("Sales_NSR")).over(Window().partitionBy("CUSTOMER","FISCPER")))

        self.salesDf = self.salesDf.withColumn("Sales_Volume_in_UC_rolling_xmonths_back_avg", f.avg(f.col("Sales_Volume_in_UC_monthly_sum")).over(Window().partitionBy("CUSTOMER")))\
                                .withColumn("Sales_NSR_rolling_xmonths_back_avg", f.avg(f.col("Sales_NSR_monthly_sum")).over(Window().partitionBy("CUSTOMER")))


        self.salesDf = self.salesDf.select(f.col("CUSTOMER"), f.col("Sales_Volume_in_UC_rolling_xmonths_back_avg") ,f.col("Sales_NSR_rolling_xmonths_back_avg")).distinct()

    def impute_nans(self)->None:pass 

    def assemble(self)->DataFrame:
        self.load_data()
        self.filter_data()
        self.feature_engineering()
        self.impute_nans()
        return self.salesDf

def make_analytical_base_table(customerDF:DataFrame, 
                            coolersDf: DataFrame, salesDf:DataFrame,
                            demographicsImputedDf:DataFrame,INCLUDE_COLS_LIST:List[str] ):
    
    abtDf = customerDF.join(demographicsImputedDf, on='CUSTOMER', how='inner')\
                    .join(coolersDf,  on='CUSTOMER', how='inner')\
                    .join(salesDf, on='CUSTOMER', how='left')

    abtDf = abtDf.withColumn("Sales_Volume_in_UC_imputed",f.lit(None))
    abtDf = abtDf.withColumn("Sales_Volume_in_UC_imputed",
                        f.coalesce(
                                    f.col("Sales_Volume_in_UC_rolling_xmonths_back_avg").cast("Double"),
                                    f.avg(f.col("Sales_Volume_in_UC_rolling_xmonths_back_avg").cast("Double")).over(Window.partitionBy("CUST_CTRADE_CH_DESC","CUST_CDMD_AREA")),
                                    f.avg(f.col("Sales_Volume_in_UC_rolling_xmonths_back_avg").cast("Double")).over( Window.partitionBy("CUST_CTRADE_CH_DESC")),
                                    f.avg(f.col("Sales_Volume_in_UC_rolling_xmonths_back_avg").cast("Double")).over(Window.partitionBy())
                        ))

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
    
    #Filter customer MD if no coordinates or postal code
    condition1 = (
                    ((f.col('LONGITUDE').cast("double")!=0) | (f.col('LONGITUDE').isNotNull())) & 
                    ((f.col('LATITUDE').cast("double")!=0)  | (f.col('LATITUDE').isNotNull()))
                )
    condition2 = (f.col('POSTAL_CODE').isNotNull())


    abtDf = abtDf.filter(condition1 | condition2)

    abtDf = abtDf.select(INCLUDE_COLS_LIST)

    partition_date=datetime.now().strftime("%Y%m%d_%H%M%S")
    #TODO Fix output save file path
    abtDf.write.csv(f"/mnt/datalake/development/mylonas/pvo/data/abt/{cc}/output/{cc}_abt_{partition_date}.csv", header=True)


