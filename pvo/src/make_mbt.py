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
        self.abtDf:DataFrame=None 
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


    def modelling(self) -> None:
        """
        _summary_

        _extended_summary_
        """
        # Label Indexer
        labelIndexer = StringIndexer(inputCol="targetVar", outputCol="targetVarInd")

        # Indexers
        indexers = [ StringIndexer(inputCol=c, outputCol=f"{c}_indexed", handleInvalid="keep") for c in categoricalCols ]

        # Encoders
        encoders = [ OneHotEncoder(inputCol=indexer.getOutputCol(), outputCol=f"{indexer.getOutputCol()}_encoded", dropLast=False) for indexer in indexers ]

        # Assembler
        assembler = VectorAssembler(inputCols=[encoder.getOutputCol() for encoder in encoders] + continuousCols, outputCol="features",handleInvalid="skip")

        # Random Forest-Model
        rf = RandomForestClassifier(labelCol="targetVarInd", featuresCol="features",seed=123)

        paramGrid = (
                    ParamGridBuilder()
                    .addGrid(rf.numTrees, [10,15,20,25])
                    .addGrid(rf.maxDepth,[12,14,16,18,20])
                    .addGrid(rf.maxBins, [12,14,16,18,20])
                    .build()
                )



        evaluator_accuracy = MulticlassClassificationEvaluator(
                labelCol="targetVarInd", metricName="accuracy"
            )

        crossval = CrossValidator(
            estimator=rf,
            estimatorParamMaps=paramGrid,
            evaluator=evaluator_accuracy,
            numFolds=5,
            seed=21,
        )
        # Split train-test
        (trainDf, testDf) = stratified_split_train_test(dataDf, frac=0.8, label="targetVar", join_on="CUSTOMER")

        # Pipeline  
        pipeline = Pipeline(stages= [labelIndexer] + indexers + encoders + [assembler] +  [crossval])

        # Train model.  This also runs the indexers.
        pipeline_model = pipeline.fit(trainDf)

        # Make predictions.
        predictions = pipeline_model.transform(testDf)

        

    def calculate_performance_metrics(self, holdout:str)->None:
        predictionAndLabels = self.mbtDf.filter(self.this_config['holdout_filter'].format(holdout = holdout)) 

        metric = Benchmark(use='mllib')
        metrics.set_data(predictionAndLabels)
        metric.set_label_column("labelIndCol")


    def save_results(self) -> None:
        return super().save_results() 

    def post_modelling(self) -> None:
        return super().post_modelling() 

