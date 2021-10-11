package com.github.whaitukay.utils

import com.github.whaitukay.utils.files.FileUtils
import com.github.whaitukay.utils.zipper.ZipUtil
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql._
import scala.collection.JavaConverters._

object UtilWrapper {

  def listFiles(filepath: String) = {
    FileUtils.listFiles(filepath).asJava
  }

  def copyMerge(srcFS: FileSystem, srcDir: Path, dstFS: FileSystem, dstFile: Path, deleteSource: Boolean, conf: Configuration) = {
    FileUtils.copyMerge(srcFS, srcDir, dstFS, dstFile, deleteSource, conf)
  }

  def writeMergedCsv(jdf: DataFrame, outputFilename: String, delimiter: String = ",", overwrite: Boolean = true, ignoreQuotes: Boolean = true, ignoreEscapes: Boolean = true, charset: String = "utf8")= {
    FileUtils.writeMergedCsv(jdf, outputFilename, delimiter, overwrite, ignoreQuotes, ignoreEscapes, charset)
  }

  def zipFile(input:String, output:String, hdfsDir:String = "/workdir") = {
    ZipUtil.zipFile(input, output, hdfsDir)
  }

}
