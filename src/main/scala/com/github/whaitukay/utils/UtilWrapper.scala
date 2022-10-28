package com.github.whaitukay.utils

import com.github.whaitukay.utils.files.FileUtils
import com.github.whaitukay.utils.spark.Transforms
import com.github.whaitukay.utils.zipper.ZipUtil
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql._

import java.util
import scala.collection.JavaConverters._

object UtilWrapper {

  def listFiles(filepath: String): util.List[String] = {
    FileUtils.listFiles(filepath).asJava
  }

  def getFolderInfo(path: String): org.apache.spark.sql.Dataset[Row] = {
    FileUtils.getFolderInfo(path)
  }

  def delete(filepath: String): AnyVal = {
    FileUtils.delete(filepath)
  }

  def rename(srcPath: String, dstPath: String): Boolean = {
    FileUtils.rename(srcPath, dstPath)
  }

  def copyMove(srcPath: String, dstPath: String, deleteSrc: Boolean): Boolean = {
    FileUtils.copyMove(srcPath, dstPath, deleteSrc)
  }

  def copyMerge(srcFS: FileSystem, srcDir: Path, dstFS: FileSystem, dstFile: Path, deleteSource: Boolean, conf: Configuration): Boolean = {
    FileUtils.copyMerge(srcFS, srcDir, dstFS, dstFile, deleteSource, conf)
  }

  def writeMergedCsv(jdf: DataFrame, outputFilename: String, delimiter: String = ",", overwrite: Boolean = true, ignoreQuotes: Boolean = true, ignoreEscapes: Boolean = true, charset: String = "utf8"): Unit = {
    FileUtils.writeMergedCsv(jdf, outputFilename, delimiter, overwrite, ignoreQuotes, ignoreEscapes, charset)
  }

  def zipFile(input: String, output: String, hdfsDir: String = "/workdir"): Unit = {
    ZipUtil.zipFile(input, output, hdfsDir)
  }

  def gzipFile(input: String, output: String, hdfsDir: String = "/workdir"): Unit = {
    ZipUtil.gzipFile(input, output, hdfsDir)
  }

  def binaryJoin(arr: util.List[org.apache.spark.sql.Dataset[Row]], key: String = "aggrkey", joinType: String = "left"): org.apache.spark.sql.Dataset[Row] =
    Transforms.binaryJoin(arr.asScala, key, joinType)

}
