name := "spark-utils"

version := "0.1.1"
organization := "com.github.whaitukay"

scalaVersion := "2.11.12"

val sparkVersion = "2.4.5"
val hadoopVersion = "2.10.0"
val zip4jVersion = "2.6.0"


val spark = Seq( "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
                 "org.apache.spark" %% "spark-sql" % sparkVersion % "provided")


val hadoop = Seq( "org.apache.hadoop" % "hadoop-aws" % hadoopVersion % "provided",
                  "org.apache.hadoop" % "hadoop-common" % hadoopVersion % "provided",
                  "org.apache.hadoop" % "hadoop-client" % hadoopVersion % "provided",
                  "org.apache.hadoop" % "hadoop-hdfs" % hadoopVersion % "provided",
                  "org.apache.hadoop" % "hadoop-mapreduce-client-core" % hadoopVersion % "provided")

val misc = Seq( "net.lingala.zip4j" % "zip4j" % zip4jVersion)

libraryDependencies ++= spark ++ hadoop ++ misc

publishTo := Some(MavenCache("local-maven", file("local-repo/releases")))
publishConfiguration := publishConfiguration.value.withOverwrite(true)
publishLocalConfiguration := publishLocalConfiguration.value.withOverwrite(true)

