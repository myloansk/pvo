# Import Python Pkgs
from ast import Pass
import pandas as pd
import pyspark.sql.functions as f
import pyspark.sql.types as t

from pyspark.ml import Pipeline, Transformer
from pyspark.ml.functions import vector_to_array
from pyspark.ml.feature import (VectorAssembler, StringIndexer,IndexToString,
                                VectorIndexer, OneHotEncoder, QuantileDiscretizer,
                                Bucketizer,ChiSqSelector, UnivariateFeatureSelector)

from pyspark.ml.classification import RandomForestClassifier, DecisionTreeClassifier, GBTClassifier
from pyspark.ml.evaluation import ClusteringEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

from pyspark.sql import DataFrame
from functools import reduce
from pyspark.sql.window import Window
from typing import List, Dict

import numpy as np

import matplotlib.pyplot as plt
import seaborn as sns

from datetime import date

from datetime import datetime
from dateutil.relativedelta import relativedelta
from pyspark.sql.window import Window
from functools import reduce
from pyspark.sql import DataFrame
from sklearn.neighbors import BallTree, KDTree

#Import other dependencies
from utils.chi_square import ChiSquare
from utils.model_benchmark import Benchmark
from skeleton import PvoModelling ,PvoMachineLearningDAG




class PvoAtModelling(PvoModelling):
    """
    Concrete class in which all abstract stages of Source are implemented 
    with respect to AT BU

    :param PvoModelling:    parent class used as an interface in which
                            a template method that contains a skeleton of some algorithm 
                            composed of calls, usually to abstract primitive operations. 
    :type Source: class
    """
    def __init__(self, configParsedDict)->None: 
        self.abtDf:DataFrame = None 
        self.mbtDf:DataFrame = None
        self.categoricalCols:List[str] = None
        self.continuousCols:List[str] = None
        self.this_config = configParsedDict
        
    def load_abt_data(self):
        return super().load_abt_data()

    def feature_selection(self)->None:
        """
        Conduct feature selection using Chi-Square and Correlations 
        to reduce the number of columns entering the modelling
        """
        includeColLst = [c for c in self.abtDf.columns if c not in self.this_config['exclude_column_list']]

        #Initialize ChiSquare Class
        cT = ChiSquare(self.abtDf.toPandas())

        for var in includeColLst:
            cT.TestIndependence(colX=var,colY="CUST_CCAF_GROUP" ) 
    
        corrColLst = cT.importantColsLst
        corrPdf = compute_associations(self.abtDf.select(corrColLst).toPandas())

        # Select upper triangle of correlation matrix
        upper = corrPdf.where(np.triu(np.ones(corrPdf.shape), k=1).astype(np.bool))

        # Find features with correlation greater than 0.95
        to_drop = [column for column in upper.columns if any(upper[column] > 0.90)]

        excludeCols.extend(to_drop)

        dataDf = self.abtDf.filter(f.col("CUST_CCAF_GROUP") != 'Other')\
          .select('CUSTOMER', *[c for c in corrColLst if c not in excludeCols])\

        continuousCols = [c[0] for c in dataDf.dtypes if c[1] in ['int', 'double', 'bigint'] if c[0] not in ['CUST_CCAF_GROUP', 'CUSTOMER']]
        categoricalCols = [c[0] for c in dataDf.dtypes if c[1] in ['string'] if c[0] not in ['CUST_CCAF_GROUP','CUSTOMER']]

        print(dataDf.select(*[c for c in dataDf.columns if c not in excludeCols]).columns)
        includeColLst = [c for c in dataDf.columns if c not in excludeCols]

        dataDf = dataDf.withColumnRenamed('CUST_CCAF_GROUP','targetVar')


    def modelling(self):
        """
        Instantiates a cchbc core class for transforming to ready to consume data by ML algorithm 
        and subsquently fitting a ML model
        Finally, wraps up results to modelling base table
        """

        # Split train-test
        (trainDf, testDf) = stratified_split_train_test(self.abtDf, frac=self.this_config['modelling']['sampling'], label="targetVar", join_on="CUSTOMER")

        # Pipeline  
        graph = PvoMachineLearningDAG(self.this_config['modelling'])
        pipeline = graph.assemble(self.categoricalCols, self.continuousCols)
        #pipeline = Pipeline(stages= [labelIndexer] + indexers + encoders + [assembler] +  [crossval])

        # Train model.  This also runs the indexers.
        pipeline_model = pipeline.fit(trainDf)
        #pipeline_model = pipeline.fit(trainDf)

        # Make predictions.
        self.predictions = pipeline_model.transform(testDf)
        
        # Make predictions on Train.
        self.predictionsTrain = pipeline_model.transform(trainDf)
        
        idx_to_string = IndexToString(inputCol="prediction", outputCol="predictionLabel",labels=pipeline_model.stages[0].labels)
        labelMapIndexDict = dict(zip(pipeline_model.stages[0].labels, self.this_config['modelling']['target_variable_mapping']))
        
        self.predictions = self.predictions.filter(f.col("targetVar") != 'Platinum').withColumn('Holdout',f.lit('Test'))
        self.predictionsTrain = self.predictionsTrain.filter(f.col("targetVar") != 'Platinum').withColumn('Holdout',f.lit('Test'))
    
        appendedTempDf = self.predictionAndLabels.union(self.predictionAndLabelsTrain)
        appendedTempDf = idx_to_string.transform(appendedTempDf)\
                                    .withColumn("just_probs",vector_to_array("probability").alias("probability"))\
                                    .select(f.col(colName).alias(aliasName) if colName != 'just_probs' 
                                            else f.col(colName).getItem(labelMapIndexDict[colName]).alias(aliasName) for aliasName, colName in self.this_config['modelling']['output_column_map'].items())
                        
        
        self.mbtDf = self.abtDf.join(appendedTempDf, 'CUSTOMER', how='left') 
        
        idx_to_string = IndexToString(inputCol="prediction", outputCol="predictionLabel",labels=pipeline_model.stages[0].labels)
        labelMapIndexDict = dict(zip(pipeline_model.stages[0].labels, self.this_config['modelling']['target_variable_mapping']))
        
        return self

        

    def calculate_performance_metrics(self, holdout:str)->None:
        predictionAndLabels = self.mbtDf.filter(self.this_config['holdout_filter'].format(holdout = holdout)) 

        metric = Benchmark(use='mllib')
        metrics.set_data(predictionAndLabels)
        metric.set_label_column("labelIndCol")


    def save_results(self) -> None:
        return super().save_results() 

    def post_modelling(self) -> None:
        return super().post_modelling() 

