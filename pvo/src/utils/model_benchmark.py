from __future__ import annotations
from abc import ABC, abstractmethod
from pyspark.mllib.util import MLUtils
from pyspark.mllib.evaluation import (MulticlassMetrics,
                                    BinaryClassificationMetrics)
from pyspark.sql import DataFrame
from sklearn.metrics import (accuracy_score, precision_score,
                            recall_score, roc_auc_score, f1_score)
from typing import List
import numpy as np

class Benchmark:
    def __init__(self, use:str=None)->None:
        self.use = use
        self.kpis = None
        self.classification_matrix = None

    def get_method(self)->str:
        return self.use

    def set_user(self, use:str)->None:
        self.use = use

    def get_data(self)->DataFrame:
        return self.predictionAndLabels

    def set_data(self, df:DataFrame)->None:
        self.predictionAndLabels = df
    
    def set_label_column(self, labelCol:str)->None:
        self.labelCol = labelCol

    def get_label_column(self)->str:
        return self.labelCol

    def set_use(self, use:str)->None:
        self.use = use

    @staticmethod  
    def get_feature_importances(featureImp, dataset:DataFrame, featuresCol:str = "features")->pd.DataFrame:
        list_extract = []
        for i in dataset.schema[featuresCol].metadata["ml_attr"]["attrs"]:
            list_extract = list_extract + dataset.schema[featuresCol].metadata["ml_attr"]["attrs"][i]
        varLst = pd.DataFrame(list_extract)
        varLst['score'] = varLst['idx'].apply(lambda x: featureImp[x])
        featureImp = (varLst.sort_values('score', ascending = False))
        return featureImp[featureImp['score']>0]  
        
    def get_classification_report(self, labels:str):
        if self.use == 'mllib':
            multiMetric = MulticlassMetrics(self.predictionAndLabels.select('labelIndCol3',self.labelCol).rdd.map(tuple))
            binMetric = BinaryClassificationMetrics(self.predictionAndLabels.select("labelIndCol3",self.labelCol).rdd.map(tuple))
            obj = ClassifierBenchmarkUsingMllib(multiMetric, binMetric)
            return obj.get_metric(), obj.get_confusion_matrix(labels)
        else:
            predictionAndLabelsPdf = self.predictionAndLabels.select(f.col("predictionLabel"),f.col("labelCol")).toPandas()
            predictionAndTargetNumpy = np.array((self.predictionAndLabels.select(f.col("prediction"),f.col("labelIndCol")).collect())).astype(np.float)
            
            obj = ClassifierBenchmarkUsingSklearn(predictionAndLabelsPdf,predictionAndTargetNumpy)
            self.kpis, self.classification_matrix  = obj.get_metric(), obj.get_confusion_matrix()

class ClassifierBenchmarkUsingMllib:
    def __init__(self, multiMetric:MulticlassMetrics,binMetric:BinaryClassificationMetrics)->None: 
        self.multiMetric = multiMetric
        self.binMetric =   binMetric
    
    def get_accuracy(self)->float:
        return self.multiMetric.accuracy

    def get_f1score(self)->float:
        return self.multiMetric.fMeasure(1.0)
    
    def get_recall(self)->float:
        return self.multiMetric.recall(1.0)

    def get_precision(self)->float:
        return self.multiMetric.precision(1.0)

    def get_auc(self)->float:
        return self.binMetric.areaUnderROC
    
    def get_metric(self,indexVal:str='test')->List[float]:
        return pd.DataFrame({
                            'accuracy':[self.get_accuracy()],
                            'recall':[self.get_recall()],
                            'precision':[self.get_precision()],
                            'auc':[self.get_auc()]
                            }, 
                            index = [indexVal])
        
    def get_confusion_matrix(self, labels:str):
        return pd.DataFrame(self.multiMetric.confusionMatrix().toArray().tolist(),
                        columns = labels, index = labels)
    
    
class ClassifierBenchmarkUsingSklearn:
    def __init__(self, predictionAndLabelsPdf:pd.DataFrame, predictionAndTargetNumpy:np.array)->None:
        self.predictionAndLabelsPdf = predictionAndLabels.select(f.col("predictionLabel"),f.col("labelCol")).toPandas()
        self.predictionAndTargetNumpy = np.array((predictionAndLabels.select(f.col("prediction"),f.col("labelIndCol")).collect())).astype(np.float)

    def get_accuracy(self)->float:
        return accuracy_score(self.predictionAndTargetNumpy[:,0], self.predictionAndTargetNumpy[:,1])

    def get_f1score(self)->float:
        return f1_score(self.predictionAndTargetNumpy[:,0], self.predictionAndTargetNumpy[:,1])

    def get_weighted_precision(self)->float:
        return precision_score(self.predictionAndTargetNumpy[:,0], self.predictionAndTargetNumpy[:,1],average='weighted')

    def get_weighted_recall(self)->float:
        return recall_score(self.predictionAndTargetNumpy[:,0], self.predictionAndTargetNumpy[:,1],average='weighted')

    def get_auc(self)->float:
        return roc_auc_score(self.predictionAndTargetNumpy[:,0], self.predictionAndTargetNumpy[:,1],multi_class = 'ovo')

    def get_confusion_matrix(self)->pd.DataFrame:
        return pd.crosstab(self.predictionAndLabelsPDf["labelCol"],
                        self.predictionAndLabelsPDf["predictionLabel"],
                        rownames=["Actual"],
                        colnames=["Predicted"],
        )

    def get_metric(self,indexVal:str='test')->List[float]:
        return pd.DataFrame({
                            'accuracy':[self.get_accuracy()],
                            'recall':[self.get_weighted_recall()],
                            'precision':[self.get_weighted_precision()],
                            'auc':[self.get_auc()]
                        }, 
                        index = [indexVal])  