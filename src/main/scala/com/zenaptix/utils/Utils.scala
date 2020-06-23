package com.zenaptix.utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
import java.net.URI

object Utils extends SparkSessionWrapper {

  //TODO: list files in directory
  def listPaths(path:String): Seq[Path] = {
    val hadoopConf: Configuration = spark.sparkContext.hadoopConfiguration
    val fileSystem: FileSystem = FileSystem.get( new URI(path), hadoopConf)
    val listStatus = fileSystem.listStatus(new Path(path))
    val filePaths = listStatus.map(_.getPath).toSeq

    filePaths
  }

  def ListFiles(path:String): Seq[String] = {
    val paths = listPaths(path)
    val fileNames = paths.map(_.toString)

    fileNames
  }

  //TODO: Write merged CSV

  //TODO: Zip a file (not really spark)

}
