package com.github.whaitukay.utils.files

import java.net.URI
import com.github.whaitukay.utils.spark.SparkSessionWrapper
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql._
import org.apache.hadoop.io.IOUtils
import scala.util.Try
import scala.collection.JavaConverters._

object FileUtils extends SparkSessionWrapper {

  def listPaths(path: String): Seq[Path] = {
    val hadoopConf: Configuration = _internalSparkSession.sparkContext.hadoopConfiguration
    val fileSystem: FileSystem = FileSystem.get(new URI(path), hadoopConf)
    val listStatus = fileSystem.listStatus(new Path(path))
    val filePaths = listStatus.map(_.getPath).toSeq

    filePaths
  }

  def listFiles(path: String): Seq[String] = {
    val paths = listPaths(path)
    val fileNames = paths.map(_.toString)

    fileNames
  }

  def copyMerge(srcFS: FileSystem, srcDir: Path, dstFS: FileSystem, dstFile: Path, deleteSource: Boolean, conf: Configuration): Boolean = {
    // Source path is expected to be a directory:
    if (srcFS.getFileStatus(srcDir).isDirectory()) {
      val outputFile = dstFS.create(dstFile)
      Try {
        srcFS.listStatus(srcDir).sortBy(_.getPath.getName).collect {
            case status if status.isFile() =>
              val inputFile = srcFS.open(status.getPath())
              Try(IOUtils.copyBytes(inputFile, outputFile, conf, false))
              inputFile.close()
          }
      }
      outputFile.close()
      if (deleteSource) srcFS.delete(srcDir, true) else true
    }
    else false
  }

  def writeMergedCsv(df: DataFrame, outputFilename: String, delimiter: String = ",", overwrite: Boolean = true, ignoreQuotes: Boolean = true, ignoreEscapes: Boolean = true, charset: String = "utf8", options: Map[String, String] = Map()): Unit = {
    // setup
    val oldConf = _internalSparkSession.sparkContext.hadoopConfiguration.get("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    _internalSparkSession.sparkContext.hadoopConfiguration.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

    // create tmp dir for staging
    val tmpDir = outputFilename + "_tmp"

    // get filesytems
    val sourceFS = FileSystem.get(new URI(tmpDir), _internalSparkSession.sparkContext.hadoopConfiguration)
    val destFS = FileSystem.get(new URI(outputFilename), _internalSparkSession.sparkContext.hadoopConfiguration)

    var _options = Map("delimiter" -> delimiter, "header" -> "false", "charset" -> charset)
    if (ignoreEscapes) _options = _options ++ Map("escape" -> "")
    if (ignoreQuotes) _options = _options ++ Map("quote" -> "")
    _options = _options ++ options

    //check if file exists
    if (!overwrite && destFS.exists(new Path(outputFilename))) {
      throw new Exception(s"Unable to save to $outputFilename. File already exists!")
    }

    // cast types of all columns to String
    val dataDF = df.select(df.columns.map(c => df.col(c).cast("string")): _*)

    // create a new data frame containing only header names
    val headerDF = _internalSparkSession.createDataFrame(List(Row.fromSeq(dataDF.columns.toSeq)).asJava, dataDF.schema)

    // merge header names with data
    headerDF.union(dataDF).write.format("csv").mode("overwrite").options(_options).save(tmpDir)

    // clean target path if overwrite = true
    if (overwrite && destFS.exists(new Path(outputFilename))) {
      destFS.delete(new Path(outputFilename), true)
    }

    // use hadoop FileUtil to merge all partition csv files into a single file
    val success = copyMerge(sourceFS,
      new Path(tmpDir),
      destFS,
      new Path(outputFilename),
      true,
      _internalSparkSession.sparkContext.hadoopConfiguration)

    // set conf back to original value
    _internalSparkSession.sparkContext.hadoopConfiguration.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", oldConf)
  }
}
