package com.zenaptix.utils

import org.apache.spark.sql.SparkSession

trait SparkSessionWrapper {

  val spark : SparkSession = SparkSession
    .builder()
    .getOrCreate()


}
