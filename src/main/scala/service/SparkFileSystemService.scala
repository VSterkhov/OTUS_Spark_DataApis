package ru.broom
package service

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Dataset, Row, SparkSession}

class SparkFileSystemService(spark: SparkSession) {

  def readCSVWithSchema(path: String, schema: StructType): Dataset[Row] = {
    spark.read.format("csv").option("header",true).load(path)
  }

  def readParquet(path: String, schema : StructType): Dataset[Row] = {
    spark.read.load(path)
  }
}
