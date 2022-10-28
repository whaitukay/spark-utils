package com.github.whaitukay.utils

import com.github.whaitukay.utils.files.FileUtils
import com.github.whaitukay.utils.spark.Transforms
import com.github.whaitukay.utils.zipper.ZipUtil
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, Row}

object Util {

  def listPaths(path:String): Seq[Path] = FileUtils.listPaths(path)

  def listFiles(path:String): Seq[String] = FileUtils.listFiles(path)

  def getFolderInfo(path:String): DataFrame = FileUtils.getFolderInfo(path)

  def delete(path: String): AnyVal = FileUtils.delete(path)

  def rename(srcPath:String, dstPath: String): Boolean = FileUtils.rename(srcPath, dstPath)

  def copyMove(srcPath:String, dstPath: String, deleteSrc: Boolean = false): Boolean = FileUtils.copyMove(srcPath, dstPath, deleteSrc)

  def writeMergedCsv(df:DataFrame,filename:String, delimiter:String = ",", overwrite: Boolean = true, ignoreQuotes: Boolean = true, ignoreEscapes: Boolean = true, charset: String = "utf8", options: Map[String, String] = Map()): Unit =
    FileUtils.writeMergedCsv(df, filename, delimiter, overwrite, ignoreQuotes, ignoreEscapes,charset,options)

  def zipFile(inputFile:String, outputFile:String): Unit = ZipUtil.zipFile(inputFile,outputFile)
  def gzipFile(inputFile:String, outputFile:String): Unit = ZipUtil.gzipFile(inputFile,outputFile)
  //TODO: zipDirectory / zipFiles

  def binaryJoin(arr: Seq[org.apache.spark.sql.Dataset[Row]], key: String = "aggrkey", joinType: String = "left"): org.apache.spark.sql.Dataset[Row] =
    Transforms.binaryJoin(arr, key, joinType)
}
