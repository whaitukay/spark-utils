package whaitukay.utils

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame
import whaitukay.utils.files.FileUtils
import whaitukay.utils.zipper.ZipUtil

object Util {

  def listPaths(path:String): Seq[Path] = FileUtils.listPaths(path)

  def listFiles(path:String): Seq[String] = FileUtils.listFiles(path)

  def writeMergedCsv(df:DataFrame,filename:String, delimiter:String = ",", overwrite: Boolean = true): Unit = FileUtils.writeMergedCsv(df, filename, delimiter, overwrite)

  def zipFile(inputFile:String, outputFile:String): Unit = ZipUtil.zipFile(inputFile,outputFile)

  //TODO: zipDirectory / zipFiles
}
