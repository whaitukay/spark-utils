package com.github.whaitukay.utils

import com.github.whaitukay.utils.files.FileUtils
import com.github.whaitukay.utils.spark.Transforms
import com.github.whaitukay.utils.zipper.ZipUtil
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, Row}

object Util {

  def listPaths(path:String): Seq[Path] = FileUtils.listPaths(path)

  def listFiles(path:String): Seq[String] = FileUtils.listFiles(path)

  def writeMergedCsv(df:DataFrame,filename:String, delimiter:String = ",", overwrite: Boolean = true, ignoreQuotes: Boolean = true, ignoreEscapes: Boolean = true, charset: String = "utf8", options: Map[String, String] = Map()): Unit =
    FileUtils.writeMergedCsv(df, filename, delimiter, overwrite, ignoreQuotes, ignoreEscapes,charset,options)

  def zipFile(inputFile:String, outputFile:String): Unit = ZipUtil.zipFile(inputFile,outputFile)
  //TODO: zipDirectory / zipFiles

  def binaryJoin(arr: Seq[org.apache.spark.sql.Dataset[Row]], key: String = "aggrkey", joinType: String = "left"): org.apache.spark.sql.Dataset[Row] =
    Transforms.binaryJoin(arr, key, joinType)
}
