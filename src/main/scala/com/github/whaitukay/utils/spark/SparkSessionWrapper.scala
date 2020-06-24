package com.github.whaitukay.utils.spark

import org.apache.spark.sql.SparkSession

trait SparkSessionWrapper {

  val _internalSparkSession : SparkSession = SparkSession
    .builder()
    .getOrCreate()

}
