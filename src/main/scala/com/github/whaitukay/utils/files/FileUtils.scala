package com.github.whaitukay.utils.files

import java.net.URI

import com.github.whaitukay.utils.spark.SparkSessionWrapper
import com.github.whaitukay.utils.zipper.ZipUtil
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.sql._

import scala.collection.JavaConverters._

object FileUtils extends SparkSessionWrapper {

  def listPaths(path:String): Seq[Path] = {
    val hadoopConf: Configuration = _internalSparkSession.sparkContext.hadoopConfiguration
    val fileSystem: FileSystem = FileSystem.get( new URI(path), hadoopConf)
    val listStatus = fileSystem.listStatus(new Path(path))
    val filePaths = listStatus.map(_.getPath).toSeq

    filePaths
  }

  def listFiles(path:String): Seq[String] = {
    val paths = listPaths(path)
    val fileNames = paths.map(_.toString)

    fileNames
  }

  def writeMergedCsv(df: DataFrame, outputFilename:String, delimiter: String = ",", overwrite: Boolean = true): Unit = {
    // setup
    val oldConf = _internalSparkSession.sparkContext.hadoopConfiguration.get("mapreduce.fileoutputcommitter.marksuccessfuljobs","false")
    _internalSparkSession.sparkContext.hadoopConfiguration.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

    // create tmp dir for staging
    val tmpDir = outputFilename+"_tmp"

    // get filesytems
    val sourceFS = FileSystem.get(new URI(tmpDir), _internalSparkSession.sparkContext.hadoopConfiguration)
    val destFS = FileSystem.get(new URI(outputFilename), _internalSparkSession.sparkContext.hadoopConfiguration)

    // clean target path if overwrite = true
    if (overwrite && destFS.exists(new Path(outputFilename))) {
        destFS.delete(new Path(outputFilename), true)
    }
    else if (!overwrite && destFS.exists(new Path(outputFilename))){
      throw new Exception(s"Unable to save to $outputFilename. File already exists!")
    }

    // cast types of all columns to String
    val dataDF = df.select(df.columns.map(c => df.col(c).cast("string")): _*)

    // create a new data frame containing only header names
    val headerDF = _internalSparkSession.createDataFrame(List(Row.fromSeq(dataDF.columns.toSeq)).asJava, dataDF.schema)

    // merge header names with data
    headerDF.union(dataDF).write.mode("overwrite").option("delimiter",delimiter).option("header", "false").csv(tmpDir)

    // use hadoop FileUtil to merge all partition csv files into a single file
    val success = FileUtil.copyMerge(sourceFS,
                                     new Path(tmpDir),
                                     destFS,
                                     new Path(outputFilename),
                         true,
                                     _internalSparkSession.sparkContext.hadoopConfiguration,
                                     null )

    // set conf back to original value
    _internalSparkSession.sparkContext.hadoopConfiguration.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", oldConf)

  }
}
