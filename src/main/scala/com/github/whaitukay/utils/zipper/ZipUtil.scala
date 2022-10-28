package com.github.whaitukay.utils.zipper

import java.io.{File, FileInputStream, FileOutputStream, IOException, PrintWriter}
import java.net.URI
import java.util.zip._
import scala.io.Source
import com.github.whaitukay.utils.spark.SparkSessionWrapper
import net.lingala.zip4j.io.outputstream.ZipOutputStream
import net.lingala.zip4j.model.ZipParameters
import net.lingala.zip4j.model.enums.CompressionMethod
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.reflect.io.Directory


object ZipUtil extends SparkSessionWrapper{

  @throws[IOException]
  private def initializeZipOutputStream(outputZipFile: File): ZipOutputStream = {
    val fos = new FileOutputStream(outputZipFile)
    new ZipOutputStream(fos)
  }

  private def zipOutputStream(outputZipFile: File,
                              filesToAdd: List[File]): Unit = {
    val zipParameters = new ZipParameters
    zipParameters.setCompressionMethod(CompressionMethod.DEFLATE)

    val buff = new Array[Byte](4096)
    var readLen = 0

    val zos = initializeZipOutputStream(outputZipFile)
    for (fileToAdd <- filesToAdd) {
      zipParameters.setFileNameInZip(fileToAdd.getName)
      zos.putNextEntry(zipParameters)
      val inputStream = new FileInputStream(fileToAdd)
      try while ( {
        (readLen = inputStream.read(buff));
        readLen != -1
      }) zos.write(buff, 0, readLen)
      finally if (inputStream != null) inputStream.close()
      zos.closeEntry
    }
    if (zos != null) zos.close()
    filesToAdd.map(f => f.delete())
  }

  def zipFile(input:String, output:String, hdfsDir:String = "/workdir"):Unit = {

    val conf: Configuration = _internalSparkSession.sparkContext.hadoopConfiguration
    val fs: FileSystem = FileSystem.get( new URI(input), conf)

    //check if hdfsDir exists

    val ts: String = (System.currentTimeMillis()/1000).toString
    val tmpDir: String = s"$hdfsDir/zip_tmp/$ts/"

    new Directory(new File(tmpDir)).createDirectory()

    val tmpFilePath: String = s"${tmpDir}${new Path(input).getName}"
    val tmpZipFilePath: String = s"${tmpDir}$ts.zip"
    val tmpFile: File = new File(tmpFilePath)
    val tmpZipFile: File = new File(tmpZipFilePath)
    val outputFileNameWithExtension: String = if (output.endsWith(".zip")) output else output+".zip"

    try {
      println(s"Copying $input to temp directory")
      fs.copyToLocalFile(new Path(input), new Path(tmpFilePath))
      println(s"Done\n")

      println(s"Zipping $input")
      zipOutputStream(
        tmpZipFile,
        List(tmpFile)
      )
      println(s"Done\n")

      println(s"Uploading file to $outputFileNameWithExtension")
      fs.copyFromLocalFile(true, new Path(tmpZipFilePath), new Path(outputFileNameWithExtension))
      println(s"Done\n")

    }
    catch {
      case ex: Exception => println(s"Error: $ex")
    }
    finally {
      // cleanup
      fs.delete(new Path(tmpDir),true)
    }
  }


  def gzipFile(input:String, output:String, hdfsDir:String = "/workdir") {
    /* Boilerplate */
    val conf: Configuration = _internalSparkSession.sparkContext.hadoopConfiguration
    val fs: FileSystem = FileSystem.get( new URI(input), conf)

    //check if hdfsDir exists
    val ts: String = (System.currentTimeMillis()/1000).toString
    val tmpDir: String = s"$hdfsDir/zip_tmp/$ts/"

    new Directory(new File(tmpDir)).createDirectory()

    val tmpFilePath: String = s"${tmpDir}${new Path(input).getName}"
    val tmpZipFilePath: String = s"${tmpDir}$ts.gz"
    val tmpFile: File = new File(tmpFilePath)
    val tmpZipFile: File = new File(tmpZipFilePath)
    val outputFileNameWithExtension: String = if (output.endsWith(".gz")) output else output+".gz"

    try{
      println(s"Copying $input to temp directory")
      fs.copyToLocalFile(new Path(input), new Path(tmpFilePath))
      println(s"Done\n")

      println(s"Zipping $input")

      val in = new FileInputStream(tmpFile)
      // write setup in different objects to close later properly (important for big files )
      val fos = new FileOutputStream(tmpZipFile)
      val gzos = new GZIPOutputStream(fos)
      val w = new PrintWriter(gzos)
      for (line <- Source.fromInputStream(in).getLines()) {
        w.write(line + "\n")
      }
      w.close()
      gzos.close()
      fos.close()

      println(s"Done\n")

      println(s"Uploading file to $outputFileNameWithExtension")
      fs.copyFromLocalFile(true, new Path(tmpZipFilePath), new Path(outputFileNameWithExtension))
      println(s"Done\n")

    }
    catch{
      case ex: Exception => println(s"Error: $ex")
    }
    finally {
      fs.delete(new Path(tmpDir),true)
    }
  }
}