#Import Python Pkgs
from functools import reduce
from sklearn.neighbors import BallTree, KDTree
from pathlib import Path
from pyspark.sql.window import Window
from typing import Dict, List 

import pandas as pd
import pyspark.sql.functions as f
import pyspark.sql.types as t


# Import other dependencies from diffent projects
from os import path
import sys
sys.path.append(path.abspath('C:\\Users\\hq001381\\bitbucket\\master\\cchbc\\deng'))

from impute_missing import imputeStrategy

class imputeWithKnn(imputeStrategy):
    def impute(self,sdf,col_lst:List):
        customerDf = customerDf.fillna("UNKNOWN", subset = [x for x in customerDf.columns if "CUST_" in x])

        to_numeric = [col for col in  col_lst if col not in ["TAA_TC","urbanicity","CUSTOMER_DESC","LONGITUDE","LATITUDE","_BIC_CTRADE_CH","_BIC_CDMD_AREA","ta_size","geometry"]]
        demographicsOfAllCustDf = sdf.select([colName for colName in abtDf.columns if colName in col_lst ])


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
        abtDf = abtDf.drop(*[colName for colName in col_lst if colName not in leaveColLst])

        # Remove prefix `imputed`
        abtDf = abtDf.select(*[colName if 'imputed' in colName else colName for colName in sdf.columns])

class imputeWithAveragesPerCtradeAndDemandArea(imputeStrategy):
    def impute(self, sdf, col_list:List):
        for col_name in col_list:

            sdf = sdf.withColumn('{colName}'.format(colName = col_name + '_' + 'imputed'),f.lit(None))
            sdf = sdf.withColumn('{colName}'.format(colName = col_name + '_' + 'imputed'),
                        f.coalesce(
                                    f.col(col_name).cast("Double"),
                                    f.avg(f.col(col_name).cast("Double")).over(Window.partitionBy("CUST_CTRADE_CH_DESC","CUST_CDMD_AREA")),
                                    f.avg(f.col(col_name).cast("Double")).over( Window.partitionBy("CUST_CTRADE_CH_DESC")),
                                    f.avg(f.col(col_name).cast("Double")).over(Window.partitionBy())
                        )) 
        return sdf 

class imputeWithZero(imputeStrategy):
    def impute(self, sdf, col_list:List):
        sdf = sdf.fillna(0, subset=col_list)
        return sdf