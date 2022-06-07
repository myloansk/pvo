from abc import ABC, abstractmethod
import pandas as pd
import pyspark.sql.functions as f
import pyspark.sql.types as t

from datetime import datetime
from dateutil.relativedelta import relativedelta
from pyspark.sql.window import Window
from functools import reduce
from pyspark.sql import DataFrame 
from pyspark.ml.feature import (VectorAssembler, StringIndexer,IndexToString,
                                VectorIndexer, OneHotEncoder, QuantileDiscretizer,
                                Bucketizer,ChiSqSelector, UnivariateFeatureSelector)
from pyspark.ml.classification import RandomForestClassifier, DecisionTreeClassifier, GBTClassifier
from pyspark.ml.evaluation import ClusteringEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml import Pipeline, Transformer
from sklearn.neighbors import BallTree, KDTree
from typing import Dict, List, Any
from utils import get_latest_modified_file_from_directory

spark.sql("set spark.sql.execution.arrow.pyspark.fallback.enabled=false")



class Source(ABC):
    """
    
    The Source defines a template method that contains a skeleton of
    pvo data processing algorithm, composed of calls to (usually) abstract primitive
    methods.

    Concrete subclasses should implement these operations to adapt to the needs of particular data source used in the project,
    but leave the template method itself intact.

    """
    def __init__(self)->None:
        self._sparkDf:DataFrame = None
        self._this_config:Dict = None

    def set_params(self, paparsedConfigDict:Dict): 
        self._this_config = paparsedConfigDict 

        return self 

    @property 
    def get_params(self)->Dict:
        return self._this_config 

    @property 
    def get_data(self)->DataFrame:
        return self._sparkDf
    
    @abstractmethod 
    def load_data(self)->None:
        """
        Loads the data to a pyspark dataframe
        """
        pass 

    @abstractmethod 
    def filter_data(self)->None:
        """
        Filter pyspark.DataFrame based on some business criteria defined in configuration file
        """
        pass 

    @abstractmethod 
    def feature_engineering(self)->None:
        """
        Creating additional features for input columns loaded to pyspark.DataFrame
        """
        pass 

    def assemble(self)->DataFrame:
        self.load_data()
        self.filter_data()
        self.feature_engineering()
        
        return self 

class PvoMachineLearningDAG(ABC):
    def __init__(self, parsedConfig:Dict) -> None:
        self.config = parsedConfig
        self.stages = {} 

    def to_indexer(self, categicalCols:List[str] = None)->None:
        if categicalCols is None:
            self.stages['label_indexer'] = [StringIndexer(inputCol=self.this_config['label_indexer']["targetVar"], outputCol=self.this_config['label_indexer']["targetVarInd"])]
        self.stages['string_indexer'] = [ StringIndexer(inputCol=c, outputCol=f"{c}_indexed", handleInvalid="keep") for c in categicalCols ]

    def to_encode(self)->None:
        self.stages['one_hot_encode'] = [ OneHotEncoder(inputCol=indexer.getOutputCol(), outputCol=f"{indexer.getOutputCol()}_encoded", dropLast=False) for indexer in self.stages['string_indexer'] ]

    def to_vector(self, continuousCols:List[str])->None:
        self.stages['assembler'] = [VectorAssembler(inputCols=[encoder.getOutputCol() for encoder in  self.stages['one_hot_encode'] ] + continuousCols, outputCol="features",handleInvalid="skip")]


    @abstractmethod 
    def model(self):
        # Random Forest-Model
        rf = RandomForestClassifier(labelCol="targetVarInd", featuresCol="features",seed=123)

        paramGrid = (
                    ParamGridBuilder()
                    .addGrid(rf.numTrees, self.this_config['num_of_trees'])
                    .addGrid(rf.maxDepth, self.this_config['max_depth'])
                    .addGrid(rf.maxBins,  self.this_config['max_bins'])
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
        self.stages['crossval'] = [crossval]


    def assemble(self, categicalCols:List[str], continuousCols:List[str]):
        self.to_indexer()
        self.to_indexer(categicalCols)
        self.to_encode()
        self.to_vector(continuousCols)
        
        return Pipeline(stages= self.stages['label_indexer'] + self.stages['string_indexer']  +  self.stages['one_hot_encode'] + self.stages['assembler'] +  self.stages['crossval'])



class PvoModelling(ABC):
    """
    The PvoModelling defines a template method that contains a skeleton of
    pvo modelling phase algorithm, composed of calls to (usually) abstract primitive
    methods.

    Concrete subclasses should implement these operations with respect to the BU 
    used in the project, but leave the template method itself intact.
    """
    def __init__(self, configParsedDict)->None:
        self.abt:DataFrame = None 
        self.this_config = configParsedDict
    
    def load_abt_data(self):
        """
        Loads the latest anaylytical base table for particular BU
        to pyspark.DataFrame
        """
        #Get BU latest processed file 
        latestFileName = get_latest_modified_file_from_directory(self.this_config['country_input_data_directory']).rsplit("_",1).pop(0)
        print(f"This is the filepath:{latestFileName}")
        # Load BU data to a pyspark.DataFrame
        self.abtDf =  spark.read.option("header", "true").option("sep", ",").csv(latestFileName)
        self.abtDf = self.abtDf.select([f.col(colName).cast("double").alias(colName) 
                                        if colName in self.this_config['cast_to_double_list'] 
                                        else f.col(colName) 
                                        for colName in self.abtDf.columns ]) 

    @abstractmethod
    def feature_selection(self):pass 

    @abstractmethod
    def modelling(self)->None:pass 

    @abstractmethod
    def calculate_performance_metrics(self)->None:pass 

    @abstractmethod
    def save_results(self)->None:pass 

    @abstractmethod 
    def post_modelling(self)->None:pass 

    def assemble(self)->None:
        self.load_abt_data()
        self.feature_selection()
        self.modelling()
        self.calculate_performance_metrics()
        self.save_results()
        self.post_modelling()






