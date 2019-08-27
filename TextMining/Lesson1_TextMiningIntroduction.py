from pyspark import SparkContext
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from functools import reduce
import nltk
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
import matplotlib.pyplot as plt
from wordcloud import WordCloud 
import pandas as pd
import re
import string


spark = SparkSession.builder.master('local[2]').appName('Text_Mining').getOrCreate()

schema = StructType([
    StructField("rating", IntegerType(), True),
    StructField("date", StringType(), True),
    StructField("variation", StringType(), True),
    StructField("verified_reviews", StringType(), True),
    StructField("feedback", IntegerType(), True)])

data_DF = spark.read.format("org.apache.spark.csv").option("delimiter","\t").schema(schema).option("mode", "PERMISSIVE").option("inferSchema", "True").csv('/DATA/data_set/spark/text_mining/Lesson01_TextMining/amazon_alexa.tsv')


data_DF.createOrReplaceTempView("data")

data_DF.show()