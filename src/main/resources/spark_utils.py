from pyspark import SparkContext
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
sc=spark.sparkContext

def listFiles(filepath):
    return sc._jvm.com.github.whaitukay.utils.UtilWrapper.listFiles(filepath)