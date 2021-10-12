from pyspark import SparkContext
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
sc=spark.sparkContext

def listFiles(filepath):
    return sc._jvm.com.github.whaitukay.utils.UtilWrapper.listFiles(filepath)


def writeMergedCsv(df, outputFilename, delimiter=',', overwrite=True, ignoreQuotes=True, ignoreEscapes=True, charset='utf8'):
    jdf = df._jdf
    return sc._jvm.com.github.whaitukay.utils.UtilWrapper.writeMergedCsv(jdf, outputFilename, delimiter, overwrite, ignoreQuotes, ignoreEscapes, charset)


def zipFile(input, output, hdfsDir='/workdir'):
    return sc._jvm.com.github.whaitukay.utils.UtilWrapper.zipFile(input, output, hdfsDir)