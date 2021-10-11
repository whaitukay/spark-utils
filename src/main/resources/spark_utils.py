from pyspark import SparkContext

# Create the SparkSession
spark = SparkSession.builder.getOrCreate()

# SparkContext from the SparkSession
sc = spark._sc

def listFiles(filepath):
    return sc._jvm.com.github.whaitukay.utils.UtilWrapper.listFiles(filepath)


def writeMergedCsv(df, outputFilename, delimiter, overwrite, ignoreQuotes, ignoreEscapes, charset):
    jdf = df._jdf
    return sc._jvm.com.github.whaitukay.utils.UtilWrapper.writeMergedCsv(jdf, outputFilename, ",", True, True, True, "utf8")


def zipFile(input, output, hdfsDir):
    return sc._jvm.com.github.whaitukay.utils.UtilWrapper.zipFile(input, output, "/tmp")