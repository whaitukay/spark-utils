from pyspark import SparkContext
from pyspark.sql import SparkSession, DataFrame

spark = SparkSession._instantiatedSession
sc = spark.sparkContext

def listFiles(filepath):
    return sc._jvm.com.github.whaitukay.utils.UtilWrapper.listFiles(filepath)


def writeMergedCsv(df, outputFilename, delimiter=',', overwrite=True, ignoreQuotes=True, ignoreEscapes=True, charset='utf8'):
    jdf = df._jdf
    return sc._jvm.com.github.whaitukay.utils.UtilWrapper.writeMergedCsv(jdf, outputFilename, delimiter, overwrite, ignoreQuotes, ignoreEscapes, charset)


def zipFile(input, output, hdfsDir='/workdir'):
    return sc._jvm.com.github.whaitukay.utils.UtilWrapper.zipFile(input, output, hdfsDir)


def binaryJoin(arr, key='aggrkey', joinType='left'):
    assert len(arr) > 0, "DataFrame list cannot be empty!"
    jdf_list = list(map(lambda x: x._jdf, arr))
    jdf = sc._jvm.com.github.whaitukay.utils.UtilWrapper.binaryJoin(jdf_list, key, joinType)
    return DataFrame(jdf, arr[0].sql_ctx)
