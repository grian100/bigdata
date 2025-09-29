#import librerie

import pandas as pd
import io
import requests
import urllib.request
import matplotlib.pyplot as plt
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import SGDClassifier
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier, DecisionTreeClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, RegressionEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.feature import VectorAssembler,StandardScaler,PCA,Imputer,CountVectorizer, Tokenizer, StopWordsRemover, HashingTF, IDF, StringIndexer
from pyspark.ml import Pipeline
from pyspark.sql import SparkSession, Window
from pyspark.context import SparkContext
from pyspark.ml.clustering import LDA
from pyspark.sql.functions import avg, count, max, col, sum as spark_sum, cast, size, split, array_max, array_agg, array_max,array_min,array_position,create_map, explode, split, lower, regexp_replace, trim, concat_ws, lower, regexp_replace


#caricamento del dataset
url = 'https://proai-datasets.s3.eu-west-3.amazonaws.com/wikipedia.csv'
rs = requests.get(url).content
dataset  = pd.read_csv(io.StringIO(rs.decode('utf-8')))
dataset

#convertiamo il dataset Pandas in uno Spark Dataframe
spark_df = spark.createDataFrame(dataset)
spark_df = spark_df.drop("Unnamed: 0") #esclude la colonna Unnamed: 0
spark_df.show()